import {
    CheckpointUtils,
    sanitizeDoc,
    waitForCount
} from "src/mongo/db/modules/enterprise/jstests/streams/utils.js";
import {
    LocalKafkaCluster
} from "src/mongo/db/modules/enterprise/jstests/streams_kafka/kafka_utils.js";

const kafkaName = "kafka1";
const dbConnName = "db1";
const uri = 'mongodb://' + db.getMongo().host;
const kafkaUri = 'localhost:9092';
const topicName1 = 'outputTopic1';
const topicName2 = 'outputTopic2';
// Dynamic topic name expression for the $emit operator.
const topicNameExpr = {
    $cond: {if: {$eq: ["$gid", 0]}, then: topicName1, else: topicName2}
};
const dbName = 'test';
const sourceCollName1 = 'sourceColl1';
const sourceCollName2 = 'sourceColl2';
const sinkCollName1 = 'sinkColl1';
const sinkCollName2 = 'sinkColl2';
const tenantId = 'tenantId';
const checkpointCollName = 'checkpointColl';
const checkpointColl = db.getSiblingDB(dbName)[checkpointCollName];
const sourceColl1 = db.getSiblingDB(dbName)[sourceCollName1];
const sourceColl2 = db.getSiblingDB(dbName)[sourceCollName2];
const sinkColl1 = db.getSiblingDB(dbName)[sinkCollName1];
const sinkColl2 = db.getSiblingDB(dbName)[sinkCollName2];
const startOptions = {
    checkpointOptions: {
        storage: {
            uri: uri,
            db: dbName,
            coll: checkpointCollName,
        }
    },
};
const connectionRegistry = [
    {name: dbConnName, type: 'atlas', options: {uri: uri}},
    {
        name: kafkaName,
        type: 'kafka',
        options: {bootstrapServers: kafkaUri},
    }
];
const mongoToKafkaName = "mongoToKafka";
const kafkaToMongoNamePrefix = "kafkaToMongo";
const consumerGroupId = "consumer-group-1";

// Makes mongoToKafkaStartCmd for a specific collection name & topic name, being static or dynamic.
function makeMongoToKafkaStartCmd(collName, topicName) {
    return {
        streams_startStreamProcessor: '',
        name: mongoToKafkaName,
        pipeline: [
            {$source: {connectionName: dbConnName, db: dbName, coll: collName}},
            {$match: {operationType: "insert"}},
            {$replaceRoot: {newRoot: "$fullDocument"}},
            {$emit: {connectionName: kafkaName, topic: topicName}}
        ],
        connections: connectionRegistry,
        options: startOptions,
        processorId: `processor-coll_${collName}-to-topic`,
        tenantId: tenantId
    };
}

// Makes kafkaToMongoStartCmd for a specific topic name & collection name pair.
function makeKafkaToMongoStartCmd(topicName, collName) {
    return {
        streams_startStreamProcessor: '',
        name: `${kafkaToMongoNamePrefix}-${topicName}`,
        pipeline: [
            {
                $source: {
                    connectionName: kafkaName,
                    topic: topicName,
                    consumerGroupId: consumerGroupId,
                }
            },
            {$merge: {into: {connectionName: dbConnName, db: dbName, coll: collName}}}
        ],
        connections: connectionRegistry,
        options: startOptions,
        processorId: `processor-topic_${topicName}-to-coll_${collName}`,
        tenantId: tenantId
    };
}

// Makes sure that the Kafka topic is created by waiting for sample to return a result. While
// waiting for it, the $emit output makes it to the Kafka topic.
function makeSureKafkaTopicCreated(coll, topicName) {
    coll.drop();

    // Start mongoToKafka, which will read from 'coll' and write to the Kafka topic.
    assert.commandWorked(db.runCommand(makeMongoToKafkaStartCmd(coll.getName(), topicName)));
    coll.insert({a: -1});
    // Start a sample on the stream processor.
    let result = db.runCommand({streams_startStreamSample: '', name: mongoToKafkaName});
    assert.commandWorked(result);
    const cursorId = result["id"];
    // Insert events and wait for an event to be output, using sample.
    const getMoreCmd = {streams_getMoreStreamSample: cursorId, name: mongoToKafkaName};
    let sampledDocs = [];
    while (sampledDocs.length < 1) {
        result = db.runCommand(getMoreCmd);
        assert.commandWorked(result);
        assert.eq(result["cursor"]["id"], cursorId);
        sampledDocs = sampledDocs.concat(result["cursor"]["nextBatch"]);
    }

    // Stop mongoToKafka to flush the Kafka $emit output.
    assert.commandWorked(db.runCommand({streams_stopStreamProcessor: '', name: mongoToKafkaName}));
}

function dropCollections() {
    checkpointColl.drop();
    sourceColl1.drop();
    sourceColl2.drop();
    sinkColl1.drop();
    sinkColl2.drop();
}

function insertData(coll) {
    let input = [];
    for (let i = 0; i < 10000; i += 1) {
        input.push({a: i, gid: i % 2});
    }
    sourceColl1.insertMany(input);

    return input;
}

// Use a streamProcessor to write data from the source collection changestream to a Kafka topic.
// Then use another streamProcessor to write data from the Kafka topic to a sink collection.
// Verify the data in the sink collection equals to data originally inserted into the source
// collection.
function mongoToKafkaToMongo() {
    // Prepare a topic 'topicName1'.
    makeSureKafkaTopicCreated(sourceColl1, topicName1);

    // Now the Kafka topic exists, and it has at least 1 event in it.
    // Start kafkaToMongo, which will write from the topic to the sink collection.
    // kafkaToMongo uses the default kafka startAt behavior, which starts reading
    // from the current end of topic. The event we wrote above
    // won't be included in the output in the sink collection.
    const kafkaToMongoStartCmd = makeKafkaToMongoStartCmd(topicName1, sinkColl1.getName());
    const kafkaToMongoProcessorId = kafkaToMongoStartCmd.processorId;
    const kafkaToMongoName = kafkaToMongoStartCmd.name;

    let checkpointUtils = new CheckpointUtils(checkpointColl);
    assert.eq(0, checkpointUtils.getCheckpointIds(tenantId, kafkaToMongoProcessorId));
    assert.commandWorked(db.runCommand(kafkaToMongoStartCmd));
    // Wait for one kafkaToMongo checkpoint to be written, indicating the
    // streamProcessor has started up and picked a starting point.
    assert.soon(() => {
        return checkpointUtils.getCheckpointIds(tenantId, kafkaToMongoProcessorId).length > 0;
    });

    // Start the mongoToKafka streamProcessor.
    assert.commandWorked(
        db.runCommand(makeMongoToKafkaStartCmd(sourceColl1.getName(), topicName1)));

    // Write input to the 'sourceColl'.
    // mongoToKafka reads the source collection and writes to Kafka.
    // kafkaToMongo reads Kafka and writes to the sink collection.
    let input = insertData(sourceColl1);

    // Verify output shows up in the sink collection as expected.
    waitForCount(sinkColl1, input.length, 60 /* timeout */);
    let results = sinkColl1.find({}).sort({a: 1}).toArray();
    let output = [];
    for (let doc of results) {
        doc = sanitizeDoc(doc);
        delete doc._id;
        output.push(doc);
    }
    assert.eq(input, output);

    // Stop the streamProcessors.
    db.runCommand({streams_stopStreamProcessor: '', name: kafkaToMongoName});
    db.runCommand({streams_stopStreamProcessor: '', name: mongoToKafkaName});

    // Clear the checkpointing collection.
    checkpointColl.deleteMany({});
}

// Use a streamProcessor to write data from the source collection changestream to Kafka topics using
// dynamic name expression. Then use another streamProcessors to write data from those Kafka topics
// to the sink collections. Verify the data in sink collections equals to data that are suppoed to
// be inserted to each sink collection.
function mongoToDynamicKafkaTopicToMongo() {
    // Prepare topics.
    makeSureKafkaTopicCreated(sourceColl1, topicName1);
    jsTestLog(`Created topic ${topicName1}`);
    makeSureKafkaTopicCreated(sourceColl2, topicName2);
    jsTestLog(`Created topic ${topicName2}`);

    // Now the Kafka topic exists, and it has at least 1 event in it.
    // Start kafkaToMongo, which will write from the topics to the sink collections. The
    // kafkaToMongo uses the default kafka startAt behavior, which starts reading from the current
    // end of topic. The event we wrote above won't be included in the output in the sink
    // collections.
    const kafkaToMongoStartCmd1 = makeKafkaToMongoStartCmd(topicName1, sinkColl1.getName());
    const kafkaToMongoProcessorId1 = kafkaToMongoStartCmd1.processorId;
    const kafkaToMongoName1 = kafkaToMongoStartCmd1.name;
    const kafkaToMongoStartCmd2 = makeKafkaToMongoStartCmd(topicName2, sinkColl2.getName());
    const kafkaToMongoProcessorId2 = kafkaToMongoStartCmd2.processorId;
    const kafkaToMongoName2 = kafkaToMongoStartCmd2.name;

    let checkpointUtils = new CheckpointUtils(checkpointColl);
    assert.eq(0, checkpointUtils.getCheckpointIds(tenantId, kafkaToMongoProcessorId1));
    assert.eq(0, checkpointUtils.getCheckpointIds(tenantId, kafkaToMongoProcessorId2));
    assert.commandWorked(db.runCommand(kafkaToMongoStartCmd1));
    jsTestLog(`Started sp ${kafkaToMongoName1}`);
    assert.commandWorked(db.runCommand(kafkaToMongoStartCmd2));
    jsTestLog(`Started sp ${kafkaToMongoName2}`);
    // Wait for one kafkaToMongo checkpoint to be written, indicating each streamProcessor has
    // started up and picked a starting point.
    assert.soon(() => {
        return checkpointUtils.getCheckpointIds(tenantId, kafkaToMongoProcessorId1).length > 0 &&
            checkpointUtils.getCheckpointIds(tenantId, kafkaToMongoProcessorId2).length > 0;
    });
    jsTestLog("Checkpointing started");

    // Start the mongoToKafka streamProcessor with a dynamic topic name expression. The dynamic
    // topic expression will route the events to the two topics we created above.
    assert.commandWorked(
        db.runCommand(makeMongoToKafkaStartCmd(sourceColl1.getName(), topicNameExpr)));
    jsTestLog(`Started sp ${mongoToKafkaName}`);

    // Write input to the 'sourceColl'.
    // mongoToKafka reads the source collection and writes to Kafka topics.
    // kafkaToMongo reads Kafka topics and writes to the sink collections.
    let input = insertData(sourceColl1);
    jsTestLog(`Inserted data into ${sourceColl1.getName()}`);

    // Verify output shows up in the sink collections as expected.
    waitForCount(sinkColl1, input.length / 2, 60 /* timeout */);
    waitForCount(sinkColl2, input.length / 2, 60 /* timeout */);

    // The 'sinkColl1' is supposed to receive all gid == 0 events.
    let results1 = sinkColl1.find({}).sort({a: 1}).toArray();
    assert.eq(input.length / 2, results1.length);
    assert.eq(0, sinkColl1.find({gid: 1}).itcount());
    let output1 = [];
    for (let doc of results1) {
        doc = sanitizeDoc(doc);
        delete doc._id;
        output1.push(doc);
    }

    // The 'sinkColl2' is supposed to receive all gid == 1 events.
    let results2 = sinkColl2.find({}).sort({a: 1}).toArray();
    assert.eq(input.length / 2, results2.length);
    assert.eq(0, sinkColl2.find({gid: 0}).itcount());
    let output2 = [];
    for (let doc of results2) {
        doc = sanitizeDoc(doc);
        delete doc._id;
        output2.push(doc);
    }

    // Verify the merged output of the sink collections equals to the input.
    const output = output1.concat(output2).sort((lhs, rhs) => lhs.a - rhs.a);
    assert.eq(input, output);

    // Stop the streamProcessors.
    db.runCommand({streams_stopStreamProcessor: '', name: kafkaToMongoName1});
    db.runCommand({streams_stopStreamProcessor: '', name: kafkaToMongoName2});
    db.runCommand({streams_stopStreamProcessor: '', name: mongoToKafkaName});

    // Clear the checkpointing collection.
    checkpointColl.deleteMany({});
}

function kafkaConsumerGroupIdTest(kafka, partitionCount) {
    return function() {
        // Prepare a topic 'topicName1', which will also write atleast one event to
        // it.
        makeSureKafkaTopicCreated(sourceColl1, topicName1);

        const startCmd = makeKafkaToMongoStartCmd(topicName1, sinkColl1.getName());
        const {processorId, name} = startCmd;

        let checkpointUtils = new CheckpointUtils(checkpointColl);
        assert.eq(0, checkpointUtils.getCheckpointIds(tenantId, processorId).length);
        assert.commandWorked(db.runCommand(startCmd));

        // Wait for a checkpoint to be committed.
        assert.soon(
            () => { return checkpointUtils.getCheckpointIds(tenantId, processorId).length > 0; });

        const checkpoint = checkpointUtils.getCheckpointIds(tenantId, processorId)[0];
        jsTestLog(checkpoint);

        const res = kafka.getConsumerGroupId(consumerGroupId);
        assert.eq(partitionCount, Object.keys(res).length);

        // Only one message was sent to the kafka broker, so one partition should
        // have committed offset=1 and the other partition should not have committed
        // anything yet.
        assert.neq(undefined, Object.values(res).find((p) => p["current_offset"] == 1));
        assert.neq(undefined, Object.values(res).find((p) => p["current_offset"] == 0));

        // Stop the stream processor.
        db.runCommand({streams_stopStreamProcessor: '', name});
    };
}

// Runs a test function with a fresh state including a fresh Kafka cluster.
function runKafkaTest(kafka, testFn, partitionCount = 1) {
    kafka.start(partitionCount);
    try {
        // Clear any previous persistent state so that the test starts with a clean slate.
        dropCollections();
        testFn();
    } finally {
        kafka.stop();
    }
}

// Verify that a streamProcessor can be stopped when the Kafka $emit
// target is in an unhealthy state.
function testBadKafkaEmitAsyncError() {
    // Bring up a Kafka. We will crash this Kafka after the streamProcessor starts.
    let kafkaThatWillFail = new LocalKafkaCluster();
    kafkaThatWillFail.start(1);

    // Start the streamProcessor.
    const spName = "emitToKafkaThatWillFail";
    assert.commandWorked(db.runCommand({
        streams_startStreamProcessor: '',
        name: spName,
        pipeline: [
            {
                $source: {
                    connectionName: dbConnName,
                    db: dbName,
                    coll: sourceCollName1,
                }
            },
            {$emit: {connectionName: kafkaName, topic: "outputTopic"}}
        ],
        connections: connectionRegistry,
        options: startOptions,
        processorId: spName,
        tenantId: tenantId
    }));

    // Insert some data to buffer it in the local producers queue.
    for (let i = 0; i < 100; i += 1) {
        sourceColl1.insert({a: 1});
    }

    // Now, crash the Kafka target.
    kafkaThatWillFail.stop();

    // Insert some more data to buffer even more in the local producers queue.
    for (let i = 0; i < 1000; i += 1) {
        sourceColl1.insert({a: 1});
    }

    // Now try to stop the streamProcessor.
    assert.commandWorked(db.runCommand({streams_stopStreamProcessor: '', name: spName}));
}

let kafka = new LocalKafkaCluster();
runKafkaTest(kafka, mongoToKafkaToMongo);
runKafkaTest(kafka, mongoToKafkaToMongo, 12);
runKafkaTest(kafka, mongoToDynamicKafkaTopicToMongo);
runKafkaTest(
    kafka, kafkaConsumerGroupIdTest(kafka, /* partitionCount */ 2), /* partitionCount */ 2);

testBadKafkaEmitAsyncError();
