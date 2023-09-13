import {
    CheckpointUtils,
    sanitizeDoc,
    waitForCount
} from "src/mongo/db/modules/enterprise/jstests/streams/utils.js";
import {
    LocalKafkaCluster
} from "src/mongo/db/modules/enterprise/jstests/streams_kafka/kafka_utils.js";

// Use a streamProcessor to write data from the coll1 collection changestream to a Kafka topic.
// Then use another streamProcessor to write data from the Kafka topic to the coll2 collection.
// Verify the data in coll2 equals to data originally inserted into coll1.
function mongoToKafkaToMongo() {
    const kafkaName = "kafka1";
    const dbConnName = "db1";
    const uri = 'mongodb://' + db.getMongo().host;
    const kafkaUri = 'localhost:9092';
    const topicName = 'outputTopic';
    const dbName = 'test';
    const coll1Name = 'coll1';
    const coll2Name = 'coll2';
    const tenantId = 'tenantId';
    const checkpointCollName = 'checkpointColl';
    const checkpointColl = db.getSiblingDB(dbName)[checkpointCollName];
    const coll1 = db.getSiblingDB(dbName)[coll1Name];
    const coll2 = db.getSiblingDB(dbName)[coll2Name];
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

    // Start the mongoToKafka streamProcessor.
    const mongoToKafkaName = "mongoToKafka";
    const mongoToKafkaStartCmd = {
        streams_startStreamProcessor: '',
        name: mongoToKafkaName,
        pipeline: [
            {$source: {connectionName: dbConnName, db: dbName, coll: coll1Name}},
            {$match: {operationType: "insert"}},
            {$replaceRoot: {newRoot: "$fullDocument"}},
            {$emit: {connectionName: kafkaName, topic: topicName}}
        ],
        connections: connectionRegistry,
        options: startOptions,
        processorId: "processorId2",
        tenantId: tenantId
    };

    // Start mongoToKafka, which will read from coll1 and write to the Kafka topic.
    assert.commandWorked(db.runCommand(mongoToKafkaStartCmd));
    coll1.insert({a: -1});
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
    assert.commandWorked(db.runCommand({streams_stopStreamProcessor: '', name: "mongoToKafka"}));
    // Now the Kafka topic exists, and it has at least 1 event in it.
    // Start kafkaToMongo, which will write from the topic to coll2.
    // kafkaToMongo uses the default kafka startAt behavior, which starts reading
    // from the current end of topic. The event we wrote above
    // won't be included in the output in coll2.
    let checkpointUtils = new CheckpointUtils(checkpointColl);
    let kafkaToMongoProcessorId = "kafkaToMongoId";
    assert.eq(0, checkpointUtils.getCheckpointIds(tenantId, kafkaToMongoProcessorId));
    assert.commandWorked(db.runCommand({
        streams_startStreamProcessor: '',
        name: "kafkaToMongo",
        pipeline: [
            {$source: {connectionName: kafkaName, topic: topicName}},
            {$merge: {into: {connectionName: dbConnName, db: dbName, coll: coll2Name}}}
        ],
        connections: connectionRegistry,
        options: startOptions,
        processorId: kafkaToMongoProcessorId,
        tenantId: tenantId
    }));
    // Wait for one kafkaToMongo checkpoint to be written, indicating the
    // streamProcessor has started up and picked a starting point.
    assert.soon(() => {
        return checkpointUtils.getCheckpointIds(tenantId, kafkaToMongoProcessorId).length > 0;
    });
    // Start mongoToKafka again.
    assert.commandWorked(db.runCommand(mongoToKafkaStartCmd));

    // Write input to coll1.
    // mongoToKafka reads coll1 and writes to Kafka.
    // kafkaToMongo reads Kafka and writes to coll2.
    let input = [];
    for (let i = 0; i < 10000; i += 1) {
        input.push({a: 1});
    }
    coll1.insertMany(input);

    // Verify output shows up in coll2 as expected.
    waitForCount(coll2, input.length, 60 /* timeout */);
    let results = coll2.find({}).sort({a: 1}).toArray();
    let output = [];
    for (let doc of results) {
        doc = sanitizeDoc(doc);
        delete doc._id;
        output.push(doc);
    }
    assert.eq(input, output);

    // Stop the streamProcessors.
    db.runCommand({streams_stopStreamProcessor: '', name: "kafkaToMongo"});
    db.runCommand({streams_stopStreamProcessor: '', name: "mongoToKafka"});

    // Clear the checkpointing collection.
    checkpointColl.deleteMany({});
}

let kafka = new LocalKafkaCluster();
kafka.start();
try {
    mongoToKafkaToMongo();
} finally {
    kafka.stop();
}
