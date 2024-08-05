import {Thread} from "jstests/libs/parallelTester.js";
import {
    flushUntilStopped,
    LocalDiskCheckpointUtil,
} from "src/mongo/db/modules/enterprise/jstests/streams/checkpoint_helper.js";
import {
    getStats,
    listStreamProcessors,
    sampleUntil,
    sanitizeDoc,
    TEST_TENANT_ID,
    waitForCount,
} from "src/mongo/db/modules/enterprise/jstests/streams/utils.js";
import {
    LocalKafkaCluster
} from "src/mongo/db/modules/enterprise/jstests/streams_kafka/kafka_utils.js";

const kafkaPlaintextName = "kafka1";
const kafkaSASLSSLName = "kafkaSSL1";
const dbConnName = "db1";
const uri = 'mongodb://' + db.getMongo().host;
const kafkaUri = 'localhost:9092';
const kafkaUriSASLSSL = 'localhost:9093';
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
const dlqCollName = 'dlq';
const sourceColl1 = db.getSiblingDB(dbName)[sourceCollName1];
const sourceColl2 = db.getSiblingDB(dbName)[sourceCollName2];
const sinkColl1 = db.getSiblingDB(dbName)[sinkCollName1];
const sinkColl2 = db.getSiblingDB(dbName)[sinkCollName2];
const dlqColl = db.getSiblingDB(dbName)[dlqCollName];
const checkpointBaseDir = "/tmp/checkpointskafka";
const startOptions = {
    checkpointOptions: {
        localDisk: {
            writeDirectory: checkpointBaseDir,
        },
        // Checkpoint every five seconds.
        debugOnlyIntervalMs: 5000,
    },
    featureFlags: {useExecutionPlanFromCheckpoint: true},
};

const connectionRegistry = [
    {name: dbConnName, type: 'atlas', options: {uri: uri}},
    {
        name: kafkaPlaintextName,
        type: 'kafka',
        options: {bootstrapServers: kafkaUri},
    },
    {
        name: kafkaSASLSSLName,
        type: 'kafka',
        options: {
            bootstrapServers: kafkaUriSASLSSL,
            auth: {
                saslMechanism: "PLAIN",
                saslUsername: "kafka",
                saslPassword: "kafka",
                securityProtocol: "SASL_SSL",
                caCertificatePath:
                    "src/mongo/db/modules/enterprise/jstests/streams_kafka/lib/certs/ca.pem"
            }
        }
    }
];

const mongoToKafkaName = "mongoToKafka";
const kafkaToMongoNamePrefix = "kafkaToMongo";

function getRestoreDirectory(processorId) {
    // Return the directory of the latest committed checkpoint ID.
    let util = new LocalDiskCheckpointUtil(checkpointBaseDir, TEST_TENANT_ID, processorId);
    if (!util.hasCheckpoint) {
        return null;
    }
    return util.getRestoreDirectory(util.latestCheckpointId);
}

function stopStreamProcessor(name, alreadyFlushedIds = []) {
    jsTestLog(`Stopping ${name}`);
    let result = db.runCommand({streams_listStreamProcessors: '', tenantId: TEST_TENANT_ID});
    assert.commandWorked(result);
    const processor = result.streamProcessors.filter(s => s.name == name)[0];
    assert(processor != null);
    const processorId = processor.processorId;
    let flushThread = new Thread(
        flushUntilStopped, name, TEST_TENANT_ID, processorId, checkpointBaseDir, alreadyFlushedIds);
    flushThread.start();

    let stopCmd = {
        streams_stopStreamProcessor: '',
        tenantId: TEST_TENANT_ID,
        name: name,
    };
    assert.commandWorked(db.runCommand(stopCmd));
    if (flushThread != null) {
        flushThread.join();
    }
}

// Makes mongoToKafkaStartCmd for a specific collection name & topic name, being static or dynamic.
function makeMongoToKafkaStartCmd({
    collName,
    topicName,
    connName,
    sinkKey = undefined,
    sinkKeyFormat = undefined,
    sinkHeaders = undefined,
    jsonType = undefined,
    parseOnly = false,
}) {
    let processorId = `processor-coll_${collName}-to-topic${Math.floor(Math.random() * 10000)}`;
    const emitOptions = {
        connectionName: connName,
        topic: topicName,
        config: {outputFormat: 'canonicalJson'}
    };
    if (sinkKey !== undefined) {
        emitOptions.config.key = sinkKey;
    }
    if (sinkKey !== undefined) {
        emitOptions.config.keyFormat = sinkKeyFormat;
    }
    if (sinkHeaders !== undefined) {
        emitOptions.config.headers = sinkHeaders;
    }
    if (jsonType != undefined) {
        emitOptions.config.outputFormat = jsonType;
    }
    let options = {
        checkpointOptions: {
            localDisk: {
                writeDirectory: checkpointBaseDir,
                restoreDirectory: getRestoreDirectory(processorId)
            },
            // Checkpoint every five seconds.
            debugOnlyIntervalMs: 5000,
        },
        dlq: {connectionName: dbConnName, db: dbName, coll: dlqColl.getName()},
        featureFlags: {useExecutionPlanFromCheckpoint: true},
        shouldStartSample: true
    };
    if (parseOnly) {
        options.parseOnly = true;
    }
    return {
        streams_startStreamProcessor: '',
        tenantId: TEST_TENANT_ID,
        name: mongoToKafkaName,
        pipeline: [
            {$source: {connectionName: dbConnName, db: dbName, coll: collName}},
            {$match: {operationType: "insert"}},
            {$replaceRoot: {newRoot: "$fullDocument"}},
            {$project: {_stream_meta: 0}},
            {$emit: emitOptions}
        ],
        connections: connectionRegistry,
        options: options,
        processorId: processorId,
    };
}

// Makes kafkaToMongoStartCmd for a specific topic name & collection name pair.
function makeKafkaToMongoStartCmd({
    topicName,
    collName,
    pipeline = [],
    sourceKeyFormat = 'binData',
    sourceKeyFormatError = 'dlq',
    parseOnly = false
}) {
    const processorId = `processor-topic_${topicName}-to-coll_${collName}`;
    let options = {
        checkpointOptions: {
            localDisk: {
                writeDirectory: checkpointBaseDir,
                restoreDirectory: getRestoreDirectory(processorId)
            },
            // Checkpoint every five seconds.
            debugOnlyIntervalMs: 5000,
        },
        dlq: {connectionName: dbConnName, db: dbName, coll: dlqColl.getName()},
        featureFlags: {useExecutionPlanFromCheckpoint: true},
    };
    if (parseOnly) {
        options.parseOnly = true;
    }
    return {
        streams_startStreamProcessor: '',
        tenantId: TEST_TENANT_ID,
        name: `${kafkaToMongoNamePrefix}-${topicName}`,
        pipeline: pipeline.length ? pipeline : [
            {
                $source: {
                    connectionName: kafkaPlaintextName,
                    topic: topicName,
                    config: {
                        keyFormat: sourceKeyFormat,
                        keyFormatError: sourceKeyFormatError,
                    }
                }
            },
            {$merge: {into: {connectionName: dbConnName, db: dbName, coll: collName}}}
        ],
        connections: connectionRegistry,
        options: options,
        processorId: processorId,
    };
}

// Makes sure that the Kafka topic is created by waiting for sample to return a result. While
// waiting for it, the $emit output makes it to the Kafka topic.
function makeSureKafkaTopicCreated(coll, topicName, connName, count = 1) {
    coll.drop();

    // Start mongoToKafka, which will read from 'coll' and write to the Kafka topic.
    const startCmd = makeMongoToKafkaStartCmd({collName: coll.getName(), topicName, connName});
    jsTestLog(startCmd);
    let startResult = db.runCommand(startCmd);
    assert.commandWorked(startResult);
    const cursorId = startResult["sampleCursorId"];

    for (let i = 0; i < count; i++) {
        coll.insert({a: i - 1});
    }

    sampleUntil(cursorId, count, mongoToKafkaName);

    // Stop mongoToKafka to flush the Kafka $emit output.
    stopStreamProcessor(mongoToKafkaName);
}

function dropCollections() {
    removeFile(checkpointBaseDir);
    sourceColl1.drop();
    sourceColl2.drop();
    sinkColl1.drop();
    sinkColl2.drop();
    dlqColl.drop();
}

let numDocumentsToInsert = 10000;
function generateInput(count = null) {
    let num = numDocumentsToInsert;
    if (count != null) {
        num = count;
    }
    let input = [];
    for (let i = 0; i < num; i += 1) {
        const binData = new BinData(0, (i % 1000).toString().padStart(4, "0"));
        input.push({
            a: i,
            gid: i % 2,
            headers: [
                {k: "h1", v: binData},
                {k: "h2", v: binData},
            ],
            headersArrBadType: [{k: "h1", v: NumberDecimal(1)}],
            headersObj: {h1: binData, h2: binData},
            headersVariedArr: [
                {k: "h1", v: NumberInt(42)},
                {k: "h2", v: NumberLong(200)},
                {k: "h3", v: {a: 1}},
                {k: "h4", v: "hello"},
                {k: "h5", v: null},
                {k: "h6", v: -2.5}
            ],
            headersVariedObj: {
                h1: NumberInt(42),
                h2: NumberLong(200),
                h3: {a: 1},
                h4: "hello",
                h5: null,
                h6: -2.5
            },
            keyBinData: binData,
            keyDouble: 100.5 - i,
            keyInt: NumberInt(i),
            keyJson: {a: 1},
            keyLong: NumberLong(i),
            keyString: "s" + i.toString(),
        });
    }
    return input;
}
function insertData(coll, count = null) {
    const input = generateInput(count);
    sourceColl1.insertMany(input);
    return input;
}

// Use a streamProcessor to write data from the source collection changestream to a Kafka topic.
// Then use another streamProcessor to write data from the Kafka topic to a sink collection.
// Verify the data in the sink collection equals to data originally inserted into the source
// collection.
function mongoToKafkaToMongo({
    expectDlq,
    sinkKey,
    sinkKeyFormat,
    expectedKeyFunc,
    sinkHeaders,
    expectedSerializedHeaders,
    sourceKeyFormat,
    sourceKeyFormatError,
    jsonType
} = {}) {
    // Prepare a topic 'topicName1'.
    makeSureKafkaTopicCreated(sourceColl1, topicName1, kafkaPlaintextName);
    // Cleanup the source collection.
    sourceColl1.drop();

    // Now the Kafka topic exists, and it has at least 1 event in it.
    // Start kafkaToMongo, which will write from the topic to the sink collection.
    // kafkaToMongo uses the default kafka startAt behavior, which starts reading
    // from the current end of topic. The event we wrote above
    // won't be included in the output in the sink collection.
    const kafkaToMongoStartCmd = makeKafkaToMongoStartCmd({
        topicName: topicName1,
        collName: sinkColl1.getName(),
        sourceKeyFormat,
        sourceKeyFormatError
    });
    const kafkaToMongoProcessorId = kafkaToMongoStartCmd.processorId;
    const kafkaToMongoName = kafkaToMongoStartCmd.name;

    let checkpointUtils =
        new LocalDiskCheckpointUtil(checkpointBaseDir, TEST_TENANT_ID, kafkaToMongoProcessorId);
    assert.eq(0, checkpointUtils.checkpointIds);
    assert.commandWorked(db.runCommand(kafkaToMongoStartCmd));
    // Wait for one kafkaToMongo checkpoint to be written, indicating the
    // streamProcessor has started up and picked a starting point.
    assert.soon(() => { return checkpointUtils.checkpointIds.length > 0; });

    // Start the mongoToKafka streamProcessor.
    // This is used to write more data to the Kafka topic used as input in the kafkaToMongo stream
    // processor.
    assert.commandWorked(db.runCommand(makeMongoToKafkaStartCmd({
        collName: sourceColl1.getName(),
        topicName: topicName1,
        connName: kafkaPlaintextName,
        sinkKey,
        sinkKeyFormat,
        sinkHeaders,
        jsonType
    })));

    // Write input to the 'sourceColl'.
    // mongoToKafka reads the source collection and writes to Kafka.
    // kafkaToMongo reads Kafka and writes to the sink collection.
    let input = insertData(sourceColl1);
    if (expectDlq) {
        // Verify output shows up in the dlq collection as expected.
        waitForCount(dlqColl, input.length, 5 * 60 /* timeout */);
    } else {
        // Verify output shows up in the sink collection as expected.
        waitForCount(sinkColl1, input.length, 5 * 60 /* timeout */);
        let results = sinkColl1.find({}).sort({a: 1}).toArray();
        let output = [];
        for (let i = 0; i < results.length; i++) {
            let outputDoc = results[i];
            const expectedKey = sinkKey === undefined ? undefined : expectedKeyFunc(input[i]);

            let expectedHeaders = expectedSerializedHeaders;
            if (expectedHeaders === undefined) {
                expectedHeaders = sinkHeaders === undefined ? undefined : input[i].headers;
            }

            assert.eq(outputDoc._stream_meta.source.topic, topicName1, outputDoc);
            assert.eq(outputDoc._stream_meta.source.key, expectedKey, outputDoc);
            assert.eq(outputDoc._stream_meta.source.headers, expectedHeaders, outputDoc);

            outputDoc = sanitizeDoc(outputDoc);
            delete outputDoc._id;
            assert.docEq(input[i], outputDoc, outputDoc);
        }
    }

    // Stop the streamProcessors.
    stopStreamProcessor(kafkaToMongoName);
    stopStreamProcessor(mongoToKafkaName);
}

// Test that the stream metadata are preserved when non-group window operator exists even if the
// pipeline doesn't have explicit dependency on the metadata.
function mongoToKafkaToMongoMaintainStreamMeta(nonGroupWindowStage) {
    // Prepare a topic 'topicName1'.
    makeSureKafkaTopicCreated(sourceColl1, topicName1, kafkaPlaintextName);
    // Cleanup the source collection.
    sourceColl1.drop();

    // Now the Kafka topic exists, and it has at least 1 event in it.
    // Start kafkaToMongo, which will write from the topic to the sink collection.
    // kafkaToMongo uses the default kafka startAt behavior, which starts reading
    // from the current end of topic. The event we wrote above
    // won't be included in the output in the sink collection.
    const nonGroupWindowPipeline = [
        {
            $source: {
                connectionName: kafkaPlaintextName,
                topic: topicName1,
                timeField: {$toDate: {$toLong: "$a"}},
            }
        },
        {
            $tumblingWindow: {
                interval: {size: NumberInt(1), unit: "second"},
                allowedLateness: {size: NumberInt(0), unit: "second"},
                pipeline: [nonGroupWindowStage]
            }
        },
        {$merge: {into: {connectionName: dbConnName, db: dbName, coll: sinkColl1.getName()}}},
    ];
    const kafkaToMongoStartCmd = makeKafkaToMongoStartCmd(
        {topicName: topicName1, collName: sinkColl1.getName(), pipeline: nonGroupWindowPipeline});
    const kafkaToMongoProcessorId = kafkaToMongoStartCmd.processorId;
    const kafkaToMongoName = kafkaToMongoStartCmd.name;

    let checkpointUtils =
        new LocalDiskCheckpointUtil(checkpointBaseDir, TEST_TENANT_ID, kafkaToMongoProcessorId);
    assert.eq(0, checkpointUtils.checkpointIds);
    assert.commandWorked(db.runCommand(kafkaToMongoStartCmd));
    // Wait for one kafkaToMongo checkpoint to be written, indicating the
    // streamProcessor has started up and picked a starting point.
    assert.soon(() => { return checkpointUtils.checkpointIds.length > 0; });

    // Start the mongoToKafka streamProcessor.
    // This is used to write more data to the Kafka topic used as input in the kafkaToMongo stream
    // processor.
    assert.commandWorked(db.runCommand(makeMongoToKafkaStartCmd(
        {collName: sourceColl1.getName(), topicName: topicName1, connName: kafkaPlaintextName})));

    // Write input to the 'sourceColl'.
    // mongoToKafka reads the source collection and writes to Kafka.
    // kafkaToMongo reads Kafka and writes to the sink collection.
    insertData(sourceColl1);
    // Verify at least one document shows up in the sink collection as expected.
    assert.soon(() => { return sinkColl1.count() > 0; });
    const results = sinkColl1.find({}).toArray();
    for (let doc of results) {
        assert(doc._stream_meta.source.hasOwnProperty("type"), doc);
        assert(doc._stream_meta.source.hasOwnProperty("topic"), doc);
        assert(doc._stream_meta.source.hasOwnProperty("partition"), doc);
        assert(doc._stream_meta.source.hasOwnProperty("offset"), doc);
        assert(doc._stream_meta.window.hasOwnProperty("start"), doc);
        assert(doc._stream_meta.window.hasOwnProperty("end"), doc);
    }

    // Stop the streamProcessors.
    stopStreamProcessor(kafkaToMongoName);
    stopStreamProcessor(mongoToKafkaName);
}

// Test that the connection names can be retrieved.
function mongoToKafkaToMongoGetConnectionNames() {
    makeSureKafkaTopicCreated(sourceColl1, topicName1, kafkaPlaintextName);

    const kafkaToMongoStartResult = assert.commandWorked(db.runCommand(makeKafkaToMongoStartCmd({
        topicName: topicName1,
        collName: sinkColl1.getName(),
        parseOnly: true,
    })));
    assert.eq(kafkaToMongoStartResult.connectionNames.sort(),
              [dbConnName, kafkaPlaintextName],
              kafkaToMongoStartResult);
    const mongoToKafkaStartResult = assert.commandWorked(db.runCommand(makeMongoToKafkaStartCmd({
        collName: sourceColl1.getName(),
        topicName: topicName1,
        connName: kafkaPlaintextName,
        parseOnly: true
    })));
    assert.eq(mongoToKafkaStartResult.connectionNames.sort(),
              [dbConnName, kafkaPlaintextName],
              kafkaToMongoStartResult);
}

// This test uses the same logic as the mongoToKafka test, but uses the connection
// registry entry for the SASL_SSL authenticated listener + SSL validation.
function mongoToKafkaSASLSSL() {
    // Prepare a topic 'topicName1'.
    makeSureKafkaTopicCreated(sourceColl1, topicName1, kafkaSASLSSLName);
    // Cleanup the source collection.
    sourceColl1.drop();

    // Now the Kafka topic exists, and it has at least 1 event in it.
    // Start kafkaToMongo, which will write from the topic to the sink collection.
    // kafkaToMongo uses the default kafka startAt behavior, which starts reading
    // from the current end of topic. The event we wrote above
    // won't be included in the output in the sink collection.
    const kafkaToMongoStartCmd =
        makeKafkaToMongoStartCmd({topicName: topicName1, collName: sinkColl1.getName()});
    const kafkaToMongoProcessorId = kafkaToMongoStartCmd.processorId;
    const kafkaToMongoName = kafkaToMongoStartCmd.name;

    let checkpointUtils =
        new LocalDiskCheckpointUtil(checkpointBaseDir, TEST_TENANT_ID, kafkaToMongoProcessorId);
    assert.eq(0, checkpointUtils.getCheckpointIds());
    assert.commandWorked(db.runCommand(kafkaToMongoStartCmd));
    // Wait for one kafkaToMongo checkpoint to be written, indicating the
    // streamProcessor has started up and picked a starting point.
    assert.soon(() => {
        return checkpointUtils.getCheckpointIds(TEST_TENANT_ID, kafkaToMongoProcessorId).length > 0;
    });

    // Start the mongoToKafka streamProcessor.
    assert.commandWorked(db.runCommand(makeMongoToKafkaStartCmd(
        {collName: sourceColl1.getName(), topicName: topicName1, connName: kafkaSASLSSLName})));

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
    stopStreamProcessor(kafkaToMongoName);
    stopStreamProcessor(mongoToKafkaName);
}

// Use a streamProcessor to write data from the source collection changestream to Kafka topics using
// dynamic name expression. Then use another streamProcessors to write data from those Kafka topics
// to the sink collections. Verify the data in sink collections equals to data that are suppoed to
// be inserted to each sink collection.
function mongoToDynamicKafkaTopicToMongo() {
    // Prepare topics.
    makeSureKafkaTopicCreated(sourceColl1, topicName1, kafkaPlaintextName);
    jsTestLog(`Created topic ${topicName1}`);
    makeSureKafkaTopicCreated(sourceColl2, topicName2, kafkaPlaintextName);
    jsTestLog(`Created topic ${topicName2}`);

    // Now the Kafka topic exists, and it has at least 1 event in it.
    // Start kafkaToMongo, which will write from the topics to the sink collections. The
    // kafkaToMongo uses the default kafka startAt behavior, which starts reading from the current
    // end of topic. The event we wrote above won't be included in the output in the sink
    // collections.
    const kafkaToMongoStartCmd1 =
        makeKafkaToMongoStartCmd({topicName: topicName1, collName: sinkColl1.getName()});
    const kafkaToMongoProcessorId1 = kafkaToMongoStartCmd1.processorId;
    const kafkaToMongoName1 = kafkaToMongoStartCmd1.name;
    const kafkaToMongoStartCmd2 =
        makeKafkaToMongoStartCmd({topicName: topicName2, collName: sinkColl2.getName()});
    const kafkaToMongoProcessorId2 = kafkaToMongoStartCmd2.processorId;
    const kafkaToMongoName2 = kafkaToMongoStartCmd2.name;

    let checkpointUtils =
        new LocalDiskCheckpointUtil(checkpointBaseDir, TEST_TENANT_ID, kafkaToMongoProcessorId1);
    assert.eq(0, checkpointUtils.getCheckpointIds());
    let checkpointUtils2 =
        new LocalDiskCheckpointUtil(checkpointBaseDir, TEST_TENANT_ID, kafkaToMongoProcessorId2);
    assert.eq(0, checkpointUtils2.getCheckpointIds());
    assert.commandWorked(db.runCommand(kafkaToMongoStartCmd1));
    jsTestLog(`Started sp ${kafkaToMongoName1}`);
    assert.commandWorked(db.runCommand(kafkaToMongoStartCmd2));
    jsTestLog(`Started sp ${kafkaToMongoName2}`);
    // Wait for one kafkaToMongo checkpoint to be written, indicating each streamProcessor has
    // started up and picked a starting point.
    assert.soon(() => {
        return checkpointUtils.getCheckpointIds(TEST_TENANT_ID, kafkaToMongoProcessorId1).length >
            0 &&
            checkpointUtils.getCheckpointIds(TEST_TENANT_ID, kafkaToMongoProcessorId2).length > 0;
    });
    jsTestLog("Checkpointing started");

    // Cleanup the source collection.
    sourceColl1.drop();

    // Start the mongoToKafka streamProcessor with a dynamic topic name expression. The dynamic
    // topic expression will route the events to the two topics we created above.
    assert.commandWorked(db.runCommand(makeMongoToKafkaStartCmd({
        collName: sourceColl1.getName(),
        topicName: topicNameExpr,
        connName: kafkaPlaintextName
    })));
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
    stopStreamProcessor(kafkaToMongoName1);
    stopStreamProcessor(kafkaToMongoName2);
    stopStreamProcessor(mongoToKafkaName);
}

function writeToTopic(topicName, input) {
    const collName = UUID().toString().split('"')[1];
    const startCmd = makeMongoToKafkaStartCmd({
        collName: collName,
        topicName: topicName,
        connName: kafkaPlaintextName,
    });
    const sourceColl = db.getSiblingDB(dbName)[collName];
    assert.commandWorked(db.runCommand(startCmd));
    sourceColl.insertMany(input);
    assert.soon(() => { return getStats(startCmd.name).outputMessageCount == input.length; });
    stopStreamProcessor(startCmd.name);
}

function kafkaConsumerGroupIdWithNewCheckpointTest(kafka) {
    return function() {
        // Prepare a topic 'topicName1', which will also write atleast one event to
        // it.
        makeSureKafkaTopicCreated(sourceColl1, topicName1, kafkaPlaintextName);
        const consumerGroupId = "consumer-group-1";
        const startCmd = {
            streams_startStreamProcessor: '',
            tenantId: TEST_TENANT_ID,
            name: `${kafkaToMongoNamePrefix}-${topicName1}`,
            pipeline: [
                {
                    $source: {
                        connectionName: kafkaPlaintextName,
                        topic: topicName1,
                        config: {
                            auto_offset_reset: "earliest",
                            group_id: consumerGroupId,
                        },
                    }
                },
                {
                    $merge:
                        {into: {connectionName: dbConnName, db: dbName, coll: sinkColl1.getName()}}
                }
            ],
            connections: connectionRegistry,
            options: {
                checkpointOptions: {
                    localDisk: {
                        writeDirectory: checkpointBaseDir,
                    },
                },
                enableUnnestedWindow: true,
                featureFlags: {useExecutionPlanFromCheckpoint: true},
            },
            processorId: `processor-topic_${topicName1}-to-coll_${sinkColl1.getName()}`,
        };
        const {processorId, name} = startCmd;
        const checkpointUtil =
            new LocalDiskCheckpointUtil(checkpointBaseDir, TEST_TENANT_ID, processorId, name);
        checkpointUtil.clear();

        // Start the kafka to mongo stream processor.
        assert.commandWorked(db.runCommand(startCmd));
        // Wait for message to be read from Kafka and show up in the sink.
        waitForCount(sinkColl1, 1, /* timeout */ 60);
        // Wait for a current offset of 1 in the consumer group.
        assert.soon(() => {
            // Mark all checkpoints as flushed. The streams Agent does this after the upload to S3
            // completes.
            checkpointUtil.flushAll();

            // TODO(SERVER-87997): Sometimes the consumerGroup.commitAsync fails if the stream
            // processor was just started... but the next attempt works. So we write more
            // checkpoints to work around that.
            db.runCommand(
                {streams_writeCheckpoint: '', tenantId: TEST_TENANT_ID, name: name, force: true});

            const res = kafka.getConsumerGroupId(consumerGroupId);
            jsTestLog(res);
            if (res == null || Object.keys(res).length === 0) {
                return false;
            }

            let groupMembers = kafka.getConsumerGroupMembers(consumerGroupId);
            jsTestLog(`groupMembers: ${groupMembers}`);

            // Only one message was sent to the kafka broker, so the first partition
            // should have committed offset=1. There also should be one active
            // group member.
            return res[0]["current_offset"] == 1 && groupMembers.length == 1;
        }, "waiting for current_offset == 1");
        jsTestLog(`Last committed checkpoint: ${checkpointUtil.latestCheckpointId}`);

        // Write 3 more documents.
        writeToTopic(topicName1, [{a: 1}, {b: 1}, {c: 1}]);
        const numDocsBeforeStop = 4;
        // Wait for the processor to read the documents.
        assert.soon(() => getStats(startCmd.name).inputMessageCount == numDocsBeforeStop);

        let alreadyFlushedIds = checkpointUtil.flushAll();
        // With the default checkpoint interval of (5 minutes), changes are we have not
        // written another checkpoint by now. Stop the processor which should write a final
        // checkpoint.
        stopStreamProcessor(name, alreadyFlushedIds);
        // Get the last committed checkpoint.
        const checkpointId = checkpointUtil.latestCheckpointId;

        // Delete all documents in the sink.
        sinkColl1.deleteMany({});

        // Insert more data into the kafka topic.
        // This data will span offsets 4 through (4+input.length).
        const input = generateInput(5 /* count */);
        writeToTopic(topicName1, input);

        // Start a new stream processor. This SP will restore from the checkpoint and begin
        // at offset 4.
        const checkpointsFromLastRun = checkpointUtil.getCheckpointIds();
        checkpointUtil.flushedIds = checkpointUtil.flushedIds.concat(checkpointsFromLastRun);
        startCmd.options.checkpointOptions.localDisk.restoreDirectory =
            checkpointUtil.getRestoreDirectory(checkpointId);
        assert.commandWorked(db.runCommand(startCmd));
        for (const id of checkpointsFromLastRun) {
            // Delete these checkpoints so we don't call flush on them below.
            checkpointUtil.deleteCheckpointDirectory(id);
        }
        // Verify output 4 through 4+input.length shows up in the sink collection as expected.
        assert.soon(() => { return sinkColl1.count() == input.length; });

        // Wait for the Kafka consumer group offsets to be updated.
        assert.soon(() => {
            // Mark all checkpoints as flushed. The streams Agent does this after the upload to S3
            // completes.
            checkpointUtil.flushAll();

            // TODO(SERVER-87997): Sometimes consumerGroup.commitAsync fails, but the next attempt
            // works. So we write more checkpoints to work around that.
            db.runCommand(
                {streams_writeCheckpoint: '', tenantId: TEST_TENANT_ID, name: name, force: true});

            const res = kafka.getConsumerGroupId(consumerGroupId);
            if (Object.keys(res).length === 0) {
                return false;
            }

            // +4 since we emitted a 4 records to the kafka topic at the beginning
            // of this test.
            return res[0]["current_offset"] == input.length + numDocsBeforeStop;
        }, `wait for current_offset == ${input.length + numDocsBeforeStop}`);

        // Ensure that we only processed `input` documents and not `input + 1`
        // because the stream processor should have resumed from when the last
        // committed offset (1) rather than the "earliest" or "latest".
        assert.eq(input.length, sinkColl1.find({}).count());

        assert.soon(() => {
            const stats = getStats(name);
            return stats["kafkaPartitions"][0]["checkpointOffset"] ==
                input.length + numDocsBeforeStop;
        });

        // Validate the stats in the kafka partitions.
        const stats = getStats(name);
        assert.commandWorked(stats);
        jsTestLog(stats);
        assert.neq(undefined, stats["kafkaPartitions"]);
        assert.eq(1, stats["kafkaPartitions"].length);

        assert.eq(0, stats["kafkaPartitions"][0]["partition"]);
        assert.eq(input.length + numDocsBeforeStop, stats["kafkaPartitions"][0]["currentOffset"]);
        assert.eq(input.length + numDocsBeforeStop,
                  stats["kafkaPartitions"][0]["checkpointOffset"]);

        alreadyFlushedIds = checkpointUtil.flushAll();
        // Stop the stream processor.
        stopStreamProcessor(name, alreadyFlushedIds);
    };
}

function kafkaStartAtEarliestTest() {
    // Create a new topic and write two documents to it.
    const numDocuments = 2;
    makeSureKafkaTopicCreated(
        sourceColl1, topicName1, kafkaPlaintextName, /* count */ numDocuments);

    const processorId = `processor-topic_${topicName1}-to-coll_${sinkColl1.getName()}`;
    const startCmd = {
        streams_startStreamProcessor: '',
        tenantId: TEST_TENANT_ID,
        name: `${kafkaToMongoNamePrefix}-${topicName1}`,
        pipeline: [
            {
                $source: {
                    connectionName: kafkaPlaintextName,
                    topic: topicName1,
                    config: {auto_offset_reset: "earliest"},
                }
            },
            {$emit: {connectionName: "__noopSink"}}
        ],
        connections: connectionRegistry,
        options: startOptions,
        processorId: processorId,
    };
    const {name} = startCmd;
    assert.commandWorked(db.runCommand(startCmd));

    // Ensure that `auto_offset_reset=earliest` was respected and all the
    // messages from the start of the topic were consumed by the stream
    // processor.
    assert.soon(() => {
        const stats = getStats(name);
        assert.commandWorked(stats);
        jsTestLog(stats);
        return stats["outputMessageCount"] == numDocuments;
    });

    stopStreamProcessor(name);
}

// Runs a test function with a fresh state including a fresh Kafka cluster.
function runKafkaTest(kafka, testFn, partitionCount = 1) {
    // Clear any previous persistent state so that the test starts with a clean slate.
    dropCollections();
    kafka.start(partitionCount);
    const funcStr = testFn.toString();
    try {
        jsTestLog(`Running: ${funcStr}`);
        testFn();
        jsTestLog(`Passed: ${funcStr}`);
    } finally {
        kafka.stop();
    }
}

// Verify that a streamProcessor goes into an error status when the Kafka broker goes down.
function testKafkaAsyncError() {
    dropCollections();

    // Bring up a Kafka. We will crash this Kafka after the streamProcessor starts.
    const partitionCount = 1;
    let kafkaThatWillFail = new LocalKafkaCluster();
    kafkaThatWillFail.start(partitionCount);

    // Start a mongo->kafka processor and insert some data.
    const startCmd = makeMongoToKafkaStartCmd(
        {collName: sourceColl1.getName(), topicName: topicName1, connName: kafkaPlaintextName});
    assert.commandWorked(db.runCommand(startCmd));
    let inputCount = 10;
    for (let i = 0; i < inputCount; i++) {
        sourceColl1.insert({a: i - 1});
    }
    // Wait for output message stats.
    assert.soon(() => {
        let stats = getStats(startCmd.name);
        assert.commandWorked(stats);
        return stats["outputMessageCount"] == inputCount;
    });
    // Stop to flush the Kafka $emit output.
    stopStreamProcessor(mongoToKafkaName);
    // Start the mongo->Kafka SP again.
    assert.commandWorked(db.runCommand(startCmd));

    // Start Kafka->mongo SP.
    const kafkaToMongoName = "kafkaToMongo";
    assert.commandWorked(db.runCommand({
        streams_startStreamProcessor: '',
        tenantId: TEST_TENANT_ID,
        name: kafkaToMongoName,
        pipeline: [
            {
                $source: {
                    connectionName: kafkaPlaintextName,
                    topic: topicName1,
                }
            },
            {$merge: {into: {connectionName: dbConnName, db: dbName, coll: sinkColl1.getName()}}}
        ],
        connections: connectionRegistry,
        options: startOptions,
        processorId: kafkaToMongoName,
    }));

    // Insert some data that the mongo->Kafka SP will pickup.
    inputCount = 100;
    for (let i = 0; i < inputCount; i += 1) {
        sourceColl1.insert({a: 1});
    }
    // Wait for the Kafka->mongo SP to have read some messages.
    assert.soon(() => {
        let stats = getStats(kafkaToMongoName);
        return stats["outputMessageCount"] > 0;
    });

    // Crash the Kafka broker.
    kafkaThatWillFail.stop();

    assert.soon(() => {
        // Kafka $emit errors are detected during flush. Write some data so checkpoints and flushes
        // happen and we detect the error.
        sourceColl1.insert({a: 1});
        let result = listStreamProcessors();
        let mongoToKafka = result.streamProcessors.filter(s => s.name == "mongoToKafka")[0];
        jsTestLog(mongoToKafka);
        return mongoToKafka.status == "error" &&
            mongoToKafka.error.reason.includes("Kafka $emit encountered error") &&
            mongoToKafka.error.reason.includes(
                "Connect to ipv4#127.0.0.1:9092 failed: Connection refused");

        // TODO(SERVER-89760): Figure out a reliable way to detect source connection failure.
        // let kafkaToMongo = result.streamProcessors.filter(s => s.name == kafkaToMongoName)[0];
        // return kafkaToMongo.status == "error" &&
        //     kafkaToMongo.error.reason.includes("Kafka $source partition 0 encountered error") &&
        //     kafkaToMongo.error.reason.includes(
        //         "Connect to ipv4#127.0.0.1:9092 failed: Connection refused");
    }, "expected mongoToKafka processor to go into failed state");

    // Now stop both stream processors.
    stopStreamProcessor(kafkaToMongoName);
    stopStreamProcessor(startCmd.name);
}

// Starts a mongo->kafka->mongo setup and tests that in the SP that is
// processing incoming kafka events, we see an offset lag in the verbose stats
function testKafkaOffsetLag(
    {sinkKey, sinkKeyFormat, sinkHeaders, sourceKeyFormat, sourceKeyFormatError, jsonType} = {}) {
    // Prepare a topic 'topicName1'.
    makeSureKafkaTopicCreated(sourceColl1, topicName1, kafkaPlaintextName);
    // Cleanup the source collection.
    sourceColl1.drop();

    // Now the Kafka topic exists, and it has at least 1 event in it.
    // Start kafkaToMongo, which will write from the topic to the sink collection.
    // kafkaToMongo uses the default kafka startAt behavior, which starts reading
    // from the current end of topic. The event we wrote above
    // won't be included in the output in the sink collection.
    const kafkaToMongoStartCmd = makeKafkaToMongoStartCmd({
        topicName: topicName1,
        collName: sinkColl1.getName(),
        sourceKeyFormat,
        sourceKeyFormatError
    });
    const kafkaToMongoProcessorId = kafkaToMongoStartCmd.processorId;
    const kafkaToMongoName = kafkaToMongoStartCmd.name;

    let checkpointUtils =
        new LocalDiskCheckpointUtil(checkpointBaseDir, TEST_TENANT_ID, kafkaToMongoProcessorId);
    assert.eq(0, checkpointUtils.checkpointIds);
    assert.commandWorked(db.runCommand(kafkaToMongoStartCmd));
    // Wait for one kafkaToMongo checkpoint to be written, indicating the
    // streamProcessor has started up and picked a starting point.
    assert.soon(() => { return checkpointUtils.checkpointIds.length > 0; });

    // Turn on failpoint to slow down the kafka source operator in the stream processor
    assert.commandWorked(
        db.adminCommand({'configureFailPoint': 'slowKafkaSource', 'mode': 'alwaysOn'}));

    // Start the mongoToKafka streamProcessor.
    // This is used to write more data to the Kafka topic used as input in the kafkaToMongo stream
    // processor.
    assert.commandWorked(db.runCommand(makeMongoToKafkaStartCmd({
        collName: sourceColl1.getName(),
        topicName: topicName1,
        connName: kafkaPlaintextName,
        sinkKey,
        sinkKeyFormat,
        sinkHeaders,
        jsonType
    })));

    assert.soon(() => {
        // Write input to the 'sourceColl'.
        // mongoToKafka reads the source collection and writes to Kafka.
        // kafkaToMongo reads Kafka and writes to the sink collection.
        sleep(1000);
        insertData(sourceColl1, 1000);
        const verboseStats = getStats(kafkaToMongoName);
        assert.eq(verboseStats["ok"], 1);
        jsTestLog(verboseStats);
        const kafkaPartitions = verboseStats['kafkaPartitions'];
        if (!kafkaPartitions[0].hasOwnProperty('partitionOffsetLag')) {
            return false;
        }
        assert.eq(kafkaPartitions[0]['partitionOffsetLag'], verboseStats['kafkaTotalOffsetLag']);
        // We have inserted 10000 docs and are running with the slowKafkaSource failpoint which
        // will cause the SP to process one message a second.
        return kafkaPartitions[0]['partitionOffsetLag'] > 0;
    });

    // Now disable the failpoint and ensure that the lag eventually falls to 0
    assert.commandWorked(db.adminCommand({'configureFailPoint': 'slowKafkaSource', 'mode': 'off'}));

    assert.soon(() => {
        const verboseStats = getStats(kafkaToMongoName);
        assert.eq(verboseStats["ok"], 1);
        jsTestLog(verboseStats);
        const kafkaPartitions = verboseStats['kafkaPartitions'];
        if (!kafkaPartitions[0].hasOwnProperty('partitionOffsetLag')) {
            return false;
        }
        assert.eq(kafkaPartitions[0]['partitionOffsetLag'], verboseStats['kafkaTotalOffsetLag']);
        return kafkaPartitions[0]['partitionOffsetLag'] == 0;
    });

    // Stop the streamProcessors.
    stopStreamProcessor(kafkaToMongoName);
    stopStreamProcessor(mongoToKafkaName);
}

let kafka = new LocalKafkaCluster();
runKafkaTest(kafka, mongoToKafkaToMongo);
runKafkaTest(kafka, mongoToKafkaToMongo, 12);
runKafkaTest(kafka,
             () => mongoToKafkaToMongoMaintainStreamMeta({$limit: 1} /* nonGroupWindowStage */));
runKafkaTest(kafka, () => mongoToKafkaToMongoMaintainStreamMeta({
                        $sort: {a: 1}
                    } /* nonGroupWindowStage
                       */));
runKafkaTest(kafka, mongoToDynamicKafkaTopicToMongo);
runKafkaTest(kafka, mongoToKafkaSASLSSL);
runKafkaTest(kafka, kafkaConsumerGroupIdWithNewCheckpointTest(kafka));
runKafkaTest(kafka, kafkaStartAtEarliestTest);

// offset lag in verbose stats
runKafkaTest(kafka, testKafkaOffsetLag);

numDocumentsToInsert = 100000;
runKafkaTest(kafka, mongoToKafkaToMongo, 12);

numDocumentsToInsert = 1000;
runKafkaTest(kafka, () => mongoToKafkaToMongo({expectDlq: false, jsonType: "relaxedJson"}));
runKafkaTest(kafka, () => mongoToKafkaToMongo({expectDlq: false, jsonType: "canonicalJson"}));

testKafkaAsyncError();

// binData key field
runKafkaTest(kafka, () => mongoToKafkaToMongo({
                        expectDlq: false,
                        sinkKey: "$keyBinData",
                        sinkKeyFormat: "binData",
                        expectedKeyFunc: (doc) => doc.keyBinData,
                    }));
// binData key field
runKafkaTest(kafka, () => mongoToKafkaToMongo({
                        expectDlq: false,
                        sinkKey: "$keyBinData",
                        sinkKeyFormat: "binData",
                        expectedKeyFunc: (doc) => doc.keyBinData,
                    }));
// binData key expression
runKafkaTest(kafka, () => mongoToKafkaToMongo({
                        expectDlq: false,
                        sinkKey: {$getField: "keyBinData"},
                        sinkKeyFormat: "binData",
                        expectedKeyFunc: (doc) => doc.keyBinData,
                    }));
// int key field
runKafkaTest(kafka, () => mongoToKafkaToMongo({
                        expectDlq: false,
                        sinkKey: "$keyInt",
                        sinkKeyFormat: "int",
                        sourceKeyFormat: "int",
                        expectedKeyFunc: (doc) => doc.keyInt,
                    }));
// invalid int key field
runKafkaTest(kafka, () => mongoToKafkaToMongo({
                        expectDlq: true,
                        sinkKey: "$keyBinData",
                        sinkKeyFormat: "binData",
                        sourceKeyFormat: "int",
                    }));
// invalid int key field passthrough
runKafkaTest(kafka, () => mongoToKafkaToMongo({
                        expectDlq: false,
                        sinkKey: "$keyBinData",
                        sinkKeyFormat: "binData",
                        sourceKeyFormat: "int",
                        sourceKeyFormatError: "passThrough",
                        expectedKeyFunc: (doc) => doc.keyBinData,
                    }));
// long key field
runKafkaTest(kafka, () => mongoToKafkaToMongo({
                        expectDlq: false,
                        sinkKey: "$keyLong",
                        sinkKeyFormat: "long",
                        sourceKeyFormat: "long",
                        expectedKeyFunc: (doc) => doc.keyLong,
                    }));
// invalid long key field
runKafkaTest(kafka, () => mongoToKafkaToMongo({
                        expectDlq: true,
                        sinkKey: "$keyBinData",
                        sinkKeyFormat: "binData",
                        sourceKeyFormat: "long",
                    }));
// double key field
runKafkaTest(kafka, () => mongoToKafkaToMongo({
                        expectDlq: false,
                        sinkKey: "$keyDouble",
                        sinkKeyFormat: "double",
                        sourceKeyFormat: "double",
                        expectedKeyFunc: (doc) => doc.keyDouble,
                    }));
// invalid double key field
runKafkaTest(kafka, () => mongoToKafkaToMongo({
                        expectDlq: true,
                        sinkKey: "$keyBinData",
                        sinkKeyFormat: "binData",
                        sourceKeyFormat: "double",
                    }));
// string key field
runKafkaTest(kafka, () => mongoToKafkaToMongo({
                        expectDlq: false,
                        sinkKey: "$keyString",
                        sinkKeyFormat: "string",
                        sourceKeyFormat: "string",
                        expectedKeyFunc: (doc) => doc.keyString,
                    }));
// json key field
runKafkaTest(kafka, () => mongoToKafkaToMongo({
                        expectDlq: false,
                        sinkKey: "$keyJson",
                        sinkKeyFormat: "json",
                        sourceKeyFormat: "json",
                        expectedKeyFunc: (doc) => doc.keyJson,
                    }));
// invalid json key field
runKafkaTest(kafka, () => mongoToKafkaToMongo({
                        expectDlq: true,
                        sinkKey: "$keyString",
                        sinkKeyFormat: "string",
                        sourceKeyFormat: "json",
                    }));
// sink key format mismatch
runKafkaTest(kafka, () => mongoToKafkaToMongo({
                        expectDlq: true,
                        sinkKey: "$keyBinData",
                        sinkKeyFormat: "int",
                    }));
// invalid key expression
runKafkaTest(kafka, () => mongoToKafkaToMongo({
                        expectDlq: true,
                        sinkKey: {$divide: ["$a", 0]},
                        sinkKeyFormat: "binData",
                    }));
// array headers field
runKafkaTest(kafka, () => mongoToKafkaToMongo({
                        expectDlq: false,
                        sinkHeaders: "$headers",
                    }));
// object headers field
runKafkaTest(kafka, () => mongoToKafkaToMongo({
                        expectDlq: false,
                        sinkHeaders: "$headersObj",
                    }));
// array headers field with varied types
runKafkaTest(kafka, () => mongoToKafkaToMongo({
                        expectDlq: false,
                        sinkHeaders: "$headersVariedArr",
                        // Bindata with base64 encoding for NumberInt(42), NumberLong(200), {a: 1},
                        // "hello" respectively.
                        expectedSerializedHeaders: [
                            {"k": "h1", "v": BinData(0, "AAAAKg==")},
                            {"k": "h2", "v": BinData(0, "AAAAAAAAAMg=")},
                            {"k": "h3", "v": BinData(0, "eyJhIjp7IiRudW1iZXJEb3VibGUiOiIxIn19")},
                            {"k": "h4", "v": BinData(0, "aGVsbG8=")},
                            {"k": "h5", "v": BinData(0, "")},
                            {"k": "h6", "v": BinData(0, "wAQAAAAAAAA=")}
                        ]
                    }));

// object headers field with varied types
runKafkaTest(kafka, () => mongoToKafkaToMongo({
                        expectDlq: false,
                        sinkHeaders: "$headersVariedObj",
                        // Bindata with base64 encoding for NumberInt(42), NumberLong(200), {a: 1},
                        // "hello" respectively.
                        expectedSerializedHeaders: [
                            {"k": "h1", "v": BinData(0, "AAAAKg==")},
                            {"k": "h2", "v": BinData(0, "AAAAAAAAAMg=")},
                            {"k": "h3", "v": BinData(0, "eyJhIjp7IiRudW1iZXJEb3VibGUiOiIxIn19")},
                            {"k": "h4", "v": BinData(0, "aGVsbG8=")},
                            {"k": "h5", "v": BinData(0, "")},
                            {"k": "h6", "v": BinData(0, "wAQAAAAAAAA=")}
                        ]
                    }));

// bad header value type
runKafkaTest(kafka, () => mongoToKafkaToMongo({
                        expectDlq: true,
                        sinkHeaders: "$headersArrBadType",
                    }));

// invalid headers type
runKafkaTest(kafka, () => mongoToKafkaToMongo({
                        expectDlq: true,
                        sinkHeaders: "$a",
                    }));
// invalid headers expression
runKafkaTest(kafka, () => mongoToKafkaToMongo({
                        expectDlq: true,
                        sinkHeaders: {$divide: ["$a", 0]},
                    }));

runKafkaTest(kafka, mongoToKafkaToMongoGetConnectionNames);
