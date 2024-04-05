import {
    LocalDiskCheckpointUtil,
    sanitizeDoc,
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
const tenantId = 'tenantId';
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
    }
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

function getRestoreDirectory(tenantId, processorId) {
    // Return the directory of the latest committed checkpoint ID.
    let util = new LocalDiskCheckpointUtil(checkpointBaseDir, tenantId, processorId);
    if (!util.hasCheckpoint) {
        return null;
    }
    return util.getRestoreDirectory(util.latestCheckpointId);
}

// Makes mongoToKafkaStartCmd for a specific collection name & topic name, being static or dynamic.
function makeMongoToKafkaStartCmd(collName, topicName, connName, key = null, headers = null) {
    const processorId = `processor-coll_${collName}-to-topic`;
    const emitOptions = {
        connectionName: connName,
        topic: topicName,
    };
    if (key !== null) {
        emitOptions.key = key;
    }
    if (headers !== null) {
        emitOptions.headers = headers;
    }
    let options = {
        checkpointOptions: {
            localDisk: {
                writeDirectory: checkpointBaseDir,
                restoreDirectory: getRestoreDirectory(tenantId, processorId)
            },
            // Checkpoint every five seconds.
            debugOnlyIntervalMs: 5000,
        },
        dlq: {connectionName: dbConnName, db: dbName, coll: dlqColl.getName()}
    };
    return {
        streams_startStreamProcessor: '',
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
        tenantId: tenantId
    };
}

// Makes kafkaToMongoStartCmd for a specific topic name & collection name pair.
function makeKafkaToMongoStartCmd(topicName, collName, pipeline = []) {
    const processorId = `processor-topic_${topicName}-to-coll_${collName}`;
    let options = {
        checkpointOptions: {
            localDisk: {
                writeDirectory: checkpointBaseDir,
                restoreDirectory: getRestoreDirectory(tenantId, processorId)
            },
            // Checkpoint every five seconds.
            debugOnlyIntervalMs: 5000,
        },
        dlq: {connectionName: dbConnName, db: dbName, coll: dlqColl.getName()}
    };
    return {
        streams_startStreamProcessor: '',
        name: `${kafkaToMongoNamePrefix}-${topicName}`,
        pipeline: pipeline.length ? pipeline : [
            {
                $source: {
                    connectionName: kafkaPlaintextName,
                    topic: topicName,
                    config: {enableKeysAndHeaders: true}
                }
            },
            {$merge: {into: {connectionName: dbConnName, db: dbName, coll: collName}}}
        ],
        connections: connectionRegistry,
        options: options,
        processorId: processorId,
        tenantId: tenantId
    };
}

// Makes sure that the Kafka topic is created by waiting for sample to return a result. While
// waiting for it, the $emit output makes it to the Kafka topic.
function makeSureKafkaTopicCreated(coll, topicName, connName, count = 1) {
    coll.drop();

    // Start mongoToKafka, which will read from 'coll' and write to the Kafka topic.
    const startCmd = makeMongoToKafkaStartCmd(coll.getName(), topicName, connName);
    jsTestLog(startCmd);
    assert.commandWorked(db.runCommand(startCmd));
    for (let i = 0; i < count; i++) {
        coll.insert({a: i - 1});
    }

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
    removeFile(checkpointBaseDir);
    sourceColl1.drop();
    sourceColl2.drop();
    sinkColl1.drop();
    sinkColl2.drop();
    dlqColl.drop();
}

let numDocumentsToInsert = 10000;
function insertData(coll) {
    let input = [];
    for (let i = 0; i < numDocumentsToInsert; i += 1) {
        const binData = new BinData(0, (i % 1000).toString().padStart(4, "0"));
        input.push({
            a: i,
            gid: i % 2,
            headers: [
                {k: "h1", v: binData},
                {k: "h2", v: binData},
            ],
            headersObj: {h1: binData, h2: binData},
            key: binData
        });
    }
    sourceColl1.insertMany(input);

    return input;
}

// Use a streamProcessor to write data from the source collection changestream to a Kafka topic.
// Then use another streamProcessor to write data from the Kafka topic to a sink collection.
// Verify the data in the sink collection equals to data originally inserted into the source
// collection.
function mongoToKafkaToMongo(expectDlq = false, key = null, headers = null) {
    // Prepare a topic 'topicName1'.
    makeSureKafkaTopicCreated(sourceColl1, topicName1, kafkaPlaintextName);

    // Now the Kafka topic exists, and it has at least 1 event in it.
    // Start kafkaToMongo, which will write from the topic to the sink collection.
    // kafkaToMongo uses the default kafka startAt behavior, which starts reading
    // from the current end of topic. The event we wrote above
    // won't be included in the output in the sink collection.
    const kafkaToMongoStartCmd = makeKafkaToMongoStartCmd(topicName1, sinkColl1.getName());
    const kafkaToMongoProcessorId = kafkaToMongoStartCmd.processorId;
    const kafkaToMongoName = kafkaToMongoStartCmd.name;

    let checkpointUtils =
        new LocalDiskCheckpointUtil(checkpointBaseDir, tenantId, kafkaToMongoProcessorId);
    assert.eq(0, checkpointUtils.checkpointIds);
    assert.commandWorked(db.runCommand(kafkaToMongoStartCmd));
    // Wait for one kafkaToMongo checkpoint to be written, indicating the
    // streamProcessor has started up and picked a starting point.
    assert.soon(() => { return checkpointUtils.checkpointIds.length > 0; });

    // Start the mongoToKafka streamProcessor.
    // This is used to write more data to the Kafka topic used as input in the kafkaToMongo stream
    // processor.
    assert.commandWorked(db.runCommand(makeMongoToKafkaStartCmd(
        sourceColl1.getName(), topicName1, kafkaPlaintextName, key, headers)));

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
            // Verify the Kafka key and headers in metadata.
            const expectedKey = key === null ? undefined : input[i].key;
            const expectedHeaders = headers === null ? undefined : input[i].headers;
            assert.eq(outputDoc._stream_meta.sourceKey, expectedKey, outputDoc);
            assert.eq(outputDoc._stream_meta.sourceHeaders, expectedHeaders, outputDoc);

            outputDoc = sanitizeDoc(outputDoc);
            delete outputDoc._id;
            output.push(outputDoc);
        }
        assert.eq(input, output);
    }

    // Stop the streamProcessors.
    db.runCommand({streams_stopStreamProcessor: '', name: kafkaToMongoName});
    db.runCommand({streams_stopStreamProcessor: '', name: mongoToKafkaName});
}

// Test that the stream metadata are preserved when non-group window operator exists even if the
// pipeline doesn't have explicit dependency on the metadata.
function mongoToKafkaToMongoMaintainStreamMeta(nonGroupWindowStage) {
    // Prepare a topic 'topicName1'.
    makeSureKafkaTopicCreated(sourceColl1, topicName1, kafkaPlaintextName);

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
                config: {enableKeysAndHeaders: true},
                timeField: {$toDate: "$a"},
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
    const kafkaToMongoStartCmd =
        makeKafkaToMongoStartCmd(topicName1, sinkColl1.getName(), nonGroupWindowPipeline);
    const kafkaToMongoProcessorId = kafkaToMongoStartCmd.processorId;
    const kafkaToMongoName = kafkaToMongoStartCmd.name;

    let checkpointUtils =
        new LocalDiskCheckpointUtil(checkpointBaseDir, tenantId, kafkaToMongoProcessorId);
    assert.eq(0, checkpointUtils.checkpointIds);
    assert.commandWorked(db.runCommand(kafkaToMongoStartCmd));
    // Wait for one kafkaToMongo checkpoint to be written, indicating the
    // streamProcessor has started up and picked a starting point.
    assert.soon(() => { return checkpointUtils.checkpointIds.length > 0; });

    // Start the mongoToKafka streamProcessor.
    // This is used to write more data to the Kafka topic used as input in the kafkaToMongo stream
    // processor.
    assert.commandWorked(db.runCommand(
        makeMongoToKafkaStartCmd(sourceColl1.getName(), topicName1, kafkaPlaintextName)));

    // Write input to the 'sourceColl'.
    // mongoToKafka reads the source collection and writes to Kafka.
    // kafkaToMongo reads Kafka and writes to the sink collection.
    insertData(sourceColl1);
    // Verify at least one document shows up in the sink collection as expected.
    waitForCount(sinkColl1, 1, 5 * 60 /* timeout */);
    const results = sinkColl1.find({}).toArray();
    for (let doc of results) {
        assert(doc._stream_meta.hasOwnProperty("sourceType"), doc);
        assert(doc._stream_meta.hasOwnProperty("sourcePartition"), doc);
        assert(doc._stream_meta.hasOwnProperty("sourceOffset"), doc);
        assert(doc._stream_meta.hasOwnProperty("windowStart"), doc);
        assert(doc._stream_meta.hasOwnProperty("windowEnd"), doc);
    }

    // Stop the streamProcessors.
    db.runCommand({streams_stopStreamProcessor: '', name: kafkaToMongoName});
    db.runCommand({streams_stopStreamProcessor: '', name: mongoToKafkaName});
}

// This test uses the same logic as the mongoToKafka test, but uses the connection
// registry entry for the SASL_SSL authenticated listener + SSL validation.
function mongoToKafkaSASLSSL() {
    // Prepare a topic 'topicName1'.
    makeSureKafkaTopicCreated(sourceColl1, topicName1, kafkaSASLSSLName);

    // Now the Kafka topic exists, and it has at least 1 event in it.
    // Start kafkaToMongo, which will write from the topic to the sink collection.
    // kafkaToMongo uses the default kafka startAt behavior, which starts reading
    // from the current end of topic. The event we wrote above
    // won't be included in the output in the sink collection.
    const kafkaToMongoStartCmd = makeKafkaToMongoStartCmd(topicName1, sinkColl1.getName());
    const kafkaToMongoProcessorId = kafkaToMongoStartCmd.processorId;
    const kafkaToMongoName = kafkaToMongoStartCmd.name;

    let checkpointUtils =
        new LocalDiskCheckpointUtil(checkpointBaseDir, tenantId, kafkaToMongoProcessorId);
    assert.eq(0, checkpointUtils.getCheckpointIds());
    assert.commandWorked(db.runCommand(kafkaToMongoStartCmd));
    // Wait for one kafkaToMongo checkpoint to be written, indicating the
    // streamProcessor has started up and picked a starting point.
    assert.soon(() => {
        return checkpointUtils.getCheckpointIds(tenantId, kafkaToMongoProcessorId).length > 0;
    });

    // Start the mongoToKafka streamProcessor.
    assert.commandWorked(db.runCommand(
        makeMongoToKafkaStartCmd(sourceColl1.getName(), topicName1, kafkaSASLSSLName)));

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
    const kafkaToMongoStartCmd1 = makeKafkaToMongoStartCmd(topicName1, sinkColl1.getName());
    const kafkaToMongoProcessorId1 = kafkaToMongoStartCmd1.processorId;
    const kafkaToMongoName1 = kafkaToMongoStartCmd1.name;
    const kafkaToMongoStartCmd2 = makeKafkaToMongoStartCmd(topicName2, sinkColl2.getName());
    const kafkaToMongoProcessorId2 = kafkaToMongoStartCmd2.processorId;
    const kafkaToMongoName2 = kafkaToMongoStartCmd2.name;

    let checkpointUtils =
        new LocalDiskCheckpointUtil(checkpointBaseDir, tenantId, kafkaToMongoProcessorId1);
    assert.eq(0, checkpointUtils.getCheckpointIds());
    let checkpointUtils2 =
        new LocalDiskCheckpointUtil(checkpointBaseDir, tenantId, kafkaToMongoProcessorId2);
    assert.eq(0, checkpointUtils2.getCheckpointIds());
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
    assert.commandWorked(db.runCommand(
        makeMongoToKafkaStartCmd(sourceColl1.getName(), topicNameExpr, kafkaPlaintextName)));
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
}

function kafkaConsumerGroupIdWithNewCheckpointTest(kafka) {
    return function() {
        // Prepare a topic 'topicName1', which will also write atleast one event to
        // it.
        makeSureKafkaTopicCreated(sourceColl1, topicName1, kafkaPlaintextName);

        const checkpointWriteDir = "/tmp/checkpoint";
        mkdir(checkpointWriteDir);

        const consumerGroupId = "consumer-group-1";
        const checkpointInterval = 30 * 1000;
        const maxCheckpointWaitTime = 2 * checkpointInterval;
        const startCmd = {
            streams_startStreamProcessor: '',
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
                        writeDirectory: checkpointWriteDir,
                    },
                    debugOnlyIntervalMs: checkpointInterval
                },
                enableUnnestedWindow: true,
            },
            processorId: `processor-topic_${topicName1}-to-coll_${sinkColl1.getName()}`,
            tenantId: tenantId
        };
        const {processorId, name} = startCmd;
        const checkpointUtil =
            new LocalDiskCheckpointUtil(checkpointWriteDir, tenantId, processorId);
        checkpointUtil.clear();

        // Start the kafka to mongo stream processor.
        assert.commandWorked(db.runCommand(startCmd));
        // Wait for message to be read from Kafka and show up in the sink.
        waitForCount(sinkColl1, 1, /* timeout */ 60);
        // Wait for a current offset of 1 in the consumer group.
        assert.soon(() => {
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
        }, "waiting for current_offset == 1", maxCheckpointWaitTime);
        // Get the most recently committed checkpoint.
        const checkpointId = checkpointUtil.latestCheckpointId;

        // Stop the stream processor.
        db.runCommand({streams_stopStreamProcessor: '', name});

        // Delete all documents in the sink.
        sinkColl1.deleteMany({});

        // Insert more data into the kafka topic.
        // This data will span offsets 1 through (1+input.length).
        assert.commandWorked(db.runCommand(
            makeMongoToKafkaStartCmd(sourceColl1.getName(), topicName1, kafkaPlaintextName)));
        const input = insertData(sourceColl1);

        // Start a new stream processor. This SP will restore from the checkpoint and begin
        // at offset 1.
        startCmd.options.checkpointOptions.localDisk.restoreDirectory =
            checkpointUtil.getRestoreDirectory(checkpointId);
        assert.commandWorked(db.runCommand(startCmd));
        // Verify output 1 throguh 1+input.length shows up in the sink collection as expected.
        assert.soon(() => { return sinkColl1.count() == input.length; });
        // Stop the mongoToKafka SP.
        assert.commandWorked(
            db.runCommand({streams_stopStreamProcessor: '', name: mongoToKafkaName}));

        // Wait for the Kafka consumer group offsets to be updated.
        assert.soon(() => {
            // TODO(SERVER-87997): Sometimes consumerGroup.commitAsync fails, but the next attempt
            // works. So we write more checkpoints to work around that.
            db.runCommand({streams_writeCheckpoint: '', name: name, force: true});
            const res = kafka.getConsumerGroupId(consumerGroupId);
            if (Object.keys(res).length === 0) {
                return false;
            }

            // +1 since we emitted a record to the kafka topic at the beginning
            // of this test.
            return res[0]["current_offset"] == input.length + 1;
        }, `wait for current_offset == ${input.length + 1}`, maxCheckpointWaitTime);

        // Ensure that we only processed `input` documents and not `input + 1`
        // because the stream processor should have resumed from when the last
        // committed offset (1) rather than the "earliest" or "latest".
        assert.eq(input.length, sinkColl1.find({}).count());

        assert.soon(() => {
            const stats = db.runCommand({streams_getStats: '', name, verbose: true});
            return stats["kafkaPartitions"][0]["checkpointOffset"] == input.length + 1;
        });

        const stats = db.runCommand({streams_getStats: '', name, verbose: true});
        assert.commandWorked(stats);
        jsTestLog(stats);
        assert.neq(undefined, stats["kafkaPartitions"]);
        assert.eq(1, stats["kafkaPartitions"].length);

        assert.eq(0, stats["kafkaPartitions"][0]["partition"]);
        assert.eq(input.length + 1, stats["kafkaPartitions"][0]["currentOffset"]);
        assert.eq(input.length + 1, stats["kafkaPartitions"][0]["checkpointOffset"]);

        // Stop the stream processor.
        db.runCommand({streams_stopStreamProcessor: '', name});
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
        tenantId: tenantId
    };
    const {name} = startCmd;
    assert.commandWorked(db.runCommand(startCmd));

    // Ensure that `auto_offset_reset=earliest` was respected and all the
    // messages from the start of the topic were consumed by the stream
    // processor.
    assert.soon(() => {
        const stats = db.runCommand({streams_getStats: "", name});
        assert.commandWorked(stats);
        jsTestLog(stats);
        return stats["outputMessageCount"] == numDocuments;
    });

    assert.commandWorked(db.runCommand({streams_stopStreamProcessor: "", name}));
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
            {$emit: {connectionName: kafkaPlaintextName, topic: "outputTopic"}}
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
runKafkaTest(
    kafka,
    () => mongoToKafkaToMongo(false /* expectDlq */, "$key" /* key */, "$headers" /* headers
    */));
runKafkaTest(kafka,
             () => mongoToKafkaToMongo(
                 false /* expectDlq */, "$key" /* key */, "$headersObj" /* headers */));
runKafkaTest(kafka,
             () => mongoToKafkaToMongo(false /* expectDlq */,
                                       {$getField: "key"} /* key
                                                           */
                                       ,
                                       {$getField: "headers"} /* headers */));
runKafkaTest(
    kafka,
    () => mongoToKafkaToMongo(true /* expectDlq */, "$a" /* key */, "$headers" /* headers */));
runKafkaTest(kafka,
             () => mongoToKafkaToMongo(true /* expectDlq */, "$key" /* key */, "$a" /* headers
             */));
runKafkaTest(kafka,
             () => mongoToKafkaToMongo(
                 true /* expectDlq */, {$divide: ["$a", 0]} /* key */, "$headers" /* headers */
                 ));
runKafkaTest(kafka,
             () => mongoToKafkaToMongo(
                 true /* expectDlq */, "$key" /* key */, {$divide: ["$a", 0]} /* headers */
                 ));
runKafkaTest(kafka, mongoToKafkaToMongo, 12);
runKafkaTest(kafka,
             () => mongoToKafkaToMongoMaintainStreamMeta({$limit: 1} /* nonGroupWindowStage */));
runKafkaTest(
    kafka, () => mongoToKafkaToMongoMaintainStreamMeta({$sort: {a: 1}} /* nonGroupWindowStage */));
runKafkaTest(kafka, mongoToDynamicKafkaTopicToMongo);
runKafkaTest(kafka, mongoToKafkaSASLSSL);
runKafkaTest(kafka, kafkaConsumerGroupIdWithNewCheckpointTest(kafka));
runKafkaTest(kafka, kafkaStartAtEarliestTest);

numDocumentsToInsert = 100000;
runKafkaTest(kafka, mongoToKafkaToMongo, 12);

testBadKafkaEmitAsyncError();
