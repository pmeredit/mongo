import {Thread} from "jstests/libs/parallelTester.js";
import {
    flushUntilStopped,
    LocalDiskCheckpointUtil,
} from "src/mongo/db/modules/enterprise/jstests/streams/checkpoint_helper.js";
import {
    getStats,
    listStreamProcessors,
    sanitizeDoc,
    startSample,
    TEST_TENANT_ID,
    waitForCount,
} from "src/mongo/db/modules/enterprise/jstests/streams/utils.js";
import {
    LocalGWProxyServer
} from "src/mongo/db/modules/enterprise/jstests/streams_kafka/gwproxy/gwproxy_utils.js";
import {
    LocalKafkaCluster
} from "src/mongo/db/modules/enterprise/jstests/streams_kafka/kafka_utils.js";

const kafkaPlaintextName = "kafka1";
const kafkaSASLSSLName = "kafkaSSL1";
const kafkaSASLSSLNameHexKey = "kafkaSSL1HexKey";
const kafkaSASLSSLNameBad = "kafkaSSLBad";
const kafkaSASLSSLNameResolves = "kafkaSASLSSLNameResolves";
const kafkaSASLSSLNameInvalid = "kafkaSASLSSLNameInvalid";
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
    featureFlags: {},
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
            gwproxyEndpoint:
                "172.20.100.10:30000",  // This interface is added by the gwproxy setup script
            gwproxyKey: "14c818b4360a86df715e0c9a3b5b62f9ba932a691d883882c9dbb59468ee22f8",
            auth: {
                saslMechanism: "PLAIN",
                saslUsername: "kafka",
                saslPassword: "kafka",
                securityProtocol: "SASL_SSL",
                caCertificatePath:
                    "src/mongo/db/modules/enterprise/jstests/streams_kafka/lib/certs/ca.pem"
            }
        }
    },
    {
        name: kafkaSASLSSLNameHexKey,
        type: 'kafka',
        options: {
            bootstrapServers: kafkaUriSASLSSL,
            gwproxyEndpoint:
                "172.20.100.10:30000",  // This interface is added by the gwproxy setup script
            // Hex representation of key "abcdefghijklmnopABCDEFGHIJKLMNOP",
            gwproxyKey: "14c818b4360a86df715e0c9a3b5b62f9ba932a691d883882c9dbb59468ee22f8",
            auth: {
                saslMechanism: "PLAIN",
                saslUsername: "kafka",
                saslPassword: "kafka",
                securityProtocol: "SASL_SSL",
                caCertificatePath:
                    "src/mongo/db/modules/enterprise/jstests/streams_kafka/lib/certs/ca.pem"
            }
        }
    },
    {
        name: kafkaSASLSSLNameBad,
        type: 'kafka',
        options: {
            bootstrapServers: kafkaUriSASLSSL,
            gwproxyEndpoint:
                "172.20.100.10:30000",  // This interface is added by the gwproxy setup script
            gwproxyKey:
                "14c818b4360a86df715e0c9a3b5b62f9ba932a691d883882c9dbb59468ee22f9",  // Incorrect
                                                                                     // proxy key
            auth: {
                saslMechanism: "PLAIN",
                saslUsername: "kafka",
                saslPassword: "kafka",
                securityProtocol: "SASL_SSL",
                caCertificatePath:
                    "src/mongo/db/modules/enterprise/jstests/streams_kafka/lib/certs/ca.pem"
            }
        }
    },
    {
        name: kafkaSASLSSLNameResolves,
        type: 'kafka',
        options: {
            bootstrapServers: kafkaUriSASLSSL,
            gwproxyEndpoint: "localhost",  // This interface is added by the gwproxy setup script
            gwproxyKey:
                "14c818b4360a86df715e0c9a3b5b62f9ba932a691d883882c9dbb59468ee22f9",  // Incorrect
                                                                                     // proxy key
            auth: {
                saslMechanism: "PLAIN",
                saslUsername: "kafka",
                saslPassword: "kafka",
                securityProtocol: "SASL_SSL",
                caCertificatePath:
                    "src/mongo/db/modules/enterprise/jstests/streams_kafka/lib/certs/ca.pem"
            }
        }
    },
    {
        name: kafkaSASLSSLNameInvalid,
        type: 'kafka',
        options: {
            bootstrapServers: kafkaUriSASLSSL,
            gwproxyEndpoint: "a-name-that-will-not-resolve.home",  // This interface is added by the
                                                                   // gwproxy setup script
            gwproxyKey:
                "14c818b4360a86df715e0c9a3b5b62f9ba932a691d883882c9dbb59468ee22f9",  // Incorrect
                                                                                     // proxy key
            auth: {
                saslMechanism: "PLAIN",
                saslUsername: "kafka",
                saslPassword: "kafka",
                securityProtocol: "SASL_SSL",
                caCertificatePath:
                    "src/mongo/db/modules/enterprise/jstests/streams_kafka/lib/certs/ca.pem"
            }
        }
    },
    {
        name: "sample_solar_1",
        type: 'sample_solar',
        options: {},
    },
];

const mongoToKafkaName = "mongoToKafka";
const kafkaToMongoNamePrefix = "kafkaToMongo";
const kafkaToMongoFailingNamePrefix = "kafkaToMongoFailing";
const solarToKafkaFailingNamePrefix = "solarToKafkaFailing";

function getRestoreDirectory(processorId) {
    // Return the directory of the latest committed checkpoint ID.
    let util = new LocalDiskCheckpointUtil(checkpointBaseDir, TEST_TENANT_ID, processorId);
    if (!util.hasCheckpoint) {
        return null;
    }
    return util.getRestoreDirectory(util.latestCheckpointId);
}

function stopStreamProcessor(name) {
    jsTestLog(`Stopping ${name}`);
    let result = db.runCommand({streams_listStreamProcessors: '', tenantId: TEST_TENANT_ID});
    assert.commandWorked(result);
    const processor = result.streamProcessors.filter(s => s.name == name)[0];
    assert(processor != null);
    const processorId = processor.processorId;
    let flushThread =
        new Thread(flushUntilStopped, name, TEST_TENANT_ID, processorId, checkpointBaseDir);
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
    parseOnly = false
}) {
    const processorId = `processor-coll_${collName}-to-topic`;
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
        featureFlags: {},
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
        featureFlags: {},
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

// Makes kafkaToMongoStartCmd for a specific topic name & collection name pair.
function makeFailingKafkaToMongoStartCmd({
    topicName,
    collName,
    connName,
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
        featureFlags: {},
    };
    if (parseOnly) {
        options.parseOnly = true;
    }
    return {
        streams_startStreamProcessor: '',
        tenantId: TEST_TENANT_ID,
        name: `${kafkaToMongoFailingNamePrefix}-${topicName}`,
        pipeline: pipeline.length ? pipeline : [
            {
                $source: {
                    connectionName: connName,
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

// Makes solarToKafkaStartCmd.
function makeFailingSolarToKafkaStartCmd({
    topicName,
    collName,
    connName,
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
        featureFlags: {},
    };
    if (parseOnly) {
        options.parseOnly = true;
    }
    return {
        streams_startStreamProcessor: '',
        tenantId: TEST_TENANT_ID,
        name: `${solarToKafkaFailingNamePrefix}-${topicName}`,
        pipeline: pipeline.length ? pipeline : [
            { $source: { connectionName: "sample_solar_1" } },
            { $emit: {connectionName: connName, topic: topicName } }
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
    assert.commandWorked(db.runCommand(startCmd));
    for (let i = 0; i < count; i++) {
        coll.insert({a: i - 1});
    }

    // Start a sample on the stream processor.
    let result = startSample(mongoToKafkaName);
    assert.commandWorked(result);
    const cursorId = result["id"];
    // Insert events and wait for an event to be output, using sample.
    const getMoreCmd = {
        streams_getMoreStreamSample: cursorId,
        tenantId: TEST_TENANT_ID,
        name: mongoToKafkaName
    };
    let sampledDocs = [];
    while (sampledDocs.length < 1) {
        result = db.runCommand(getMoreCmd);
        assert.commandWorked(result);
        assert.eq(result["cursor"]["id"], cursorId);
        sampledDocs = sampledDocs.concat(result["cursor"]["nextBatch"]);
    }

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
            keyBinData: binData,
            keyInt: NumberInt(i),
            keyJson: {a: 1},
            keyLong: NumberLong(i),
            keyString: "s" + i.toString(),
        });
    }
    sourceColl1.insertMany(input);

    return input;
}

// Run a test with invalid GWProxy credentials, and expect a failure.
function mongoToKafkaSASLSSLFailure() {
    // Prepare a topic 'topicName1'.  Use a good/valid GWProxy connection object for
    // this as we want it to succeed here, but fail later.
    makeSureKafkaTopicCreated(sourceColl1, topicName1, kafkaSASLSSLName);

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
    // Note: This should fail during connect.  GWProxy will return a TLS ACCESS DENIED in this
    // case, but rdkafka doesn't propagate that error up, so it'll just look like an
    // "Unknown error -1" in the driver.
    assert.commandFailed(db.runCommand(makeMongoToKafkaStartCmd({
        collName: sourceColl1.getName() + "bad",
        topicName: topicName1,
        connName: kafkaSASLSSLNameBad
    })));

    // Stop the streamProcessors.
    stopStreamProcessor(kafkaToMongoName);
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

// This test uses the same logic as the mongoToKafka test, but uses the connection
// registry entry for the SASL_SSL authenticated listener + SSL validation, and
// tests with a hexadecimal authentication key.
function mongoToKafkaSASLSSLHexKey() {
    // Prepare a topic 'topicName1'.
    makeSureKafkaTopicCreated(sourceColl1, topicName1, kafkaSASLSSLNameHexKey);

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
    assert.commandWorked(db.runCommand(makeMongoToKafkaStartCmd({
        collName: sourceColl1.getName(),
        topicName: topicName1,
        connName: kafkaSASLSSLNameHexKey
    })));

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

// This test will execute a pipeline using the partition consumer, which
// we expect to fail in the resolver callback. We will ensure we get a
// useful error message.
function testPartitionConsumerResolverErrors() {
    const expectedError = "Unable to resolve proxy name - connection currently unavailable";

    // Prepare a topic 'topicName1'.
    makeSureKafkaTopicCreated(sourceColl1, topicName1, kafkaPlaintextName);

    // Start the mongoToKafka streamProcessor.
    let result = db.runCommand(makeFailingKafkaToMongoStartCmd({
        collName: sourceColl1.getName(),
        topicName: topicName1,
        connName: kafkaSASLSSLNameInvalid
    }));
    assert.commandFailed(result);
    assert(result.errmsg.includes(expectedError));
}

// This test will execute a pipeline using the partition consumer, which
// we expect to fail in the connect callback. We will ensure we get a
// useful error message.
function testPartitionConsumerConnectErrors() {
    const expectedError =
        "VPC Proxy for Kafka connection is not ready yet, check connection status";

    // Prepare a topic 'topicName1'.
    makeSureKafkaTopicCreated(sourceColl1, topicName1, kafkaPlaintextName);

    // Start the mongoToKafka streamProcessor.
    let result = db.runCommand(makeFailingKafkaToMongoStartCmd({
        collName: sourceColl1.getName(),
        topicName: topicName1,
        connName: kafkaSASLSSLNameHexKey
    }));
    assert.commandFailed(result);
    assert(result.errmsg.includes(expectedError));
}

// This test will execute a pipeline using the emit operator, which
// we expect to fail. We will ensure we get a useful error message.
function testEmitOperatorErrors() {
    const expectedError =
        "VPC Proxy for Kafka connection is not ready yet, check connection status";

    // Prepare a topic 'topicName1'.
    makeSureKafkaTopicCreated(sourceColl1, topicName1, kafkaPlaintextName);

    // Start the mongoToKafka streamProcessor.
    let result = db.runCommand(makeFailingSolarToKafkaStartCmd({
        collName: sourceColl1.getName(),
        topicName: topicName1,
        connName: kafkaSASLSSLNameHexKey
    }));
    assert.commandFailed(result);
    assert(result.errmsg.includes(expectedError));
}

// This test will execute a pipeline using the emit operator, to write
// to a dynamic topic, which we expect to fail. We will ensure we get
// a useful error message.
function testEmitOperatorErrorsDynamicTopic() {
    const expectedError =
        "VPC Proxy for Kafka connection is not ready yet, check connection status";

    // Prepare a topic 'topicName1'.
    makeSureKafkaTopicCreated(sourceColl1, topicName1, kafkaPlaintextName);

    // Start the mongoToKafka streamProcessor.
    let result = db.runCommand(makeFailingSolarToKafkaStartCmd({
        collName: sourceColl1.getName(),
        topicName: topicNameExpr,
        connName: kafkaSASLSSLNameHexKey
    }));
    assert.commandFailed(result);
    assert(result.errmsg.includes(expectedError));
}

// Runs a test function with a fresh state including a fresh Kafka cluster.
function runKafkaTest(kafka, testFn, partitionCount = 1) {
    kafka.start(partitionCount);
    gwproxy.start();
    try {
        // Clear any previous persistent state so that the test starts with a clean slate.
        dropCollections();
        testFn();
    } finally {
        kafka.stop();
        gwproxy.stop();
    }
}

// Runs a test function without a functional GWProxy endpoint, to simulate
// communications with a failed proxy tier.
function runFailedKafkaTest(kafka, testFn, partitionCount = 1) {
    kafka.start(partitionCount);
    try {
        // Clear any previous persistent state so that the test starts with a clean slate.
        dropCollections();
        testFn();
    } finally {
        kafka.stop();
    }
}

let kafka = new LocalKafkaCluster();
let gwproxy = new LocalGWProxyServer();

runKafkaTest(kafka, mongoToKafkaSASLSSL);
runKafkaTest(kafka, mongoToKafkaSASLSSLFailure);
runKafkaTest(kafka, mongoToKafkaSASLSSLHexKey);
runFailedKafkaTest(kafka, testPartitionConsumerResolverErrors);
runFailedKafkaTest(kafka, testPartitionConsumerConnectErrors);
runFailedKafkaTest(kafka, testEmitOperatorErrors);
runFailedKafkaTest(kafka, testEmitOperatorErrorsDynamicTopic);