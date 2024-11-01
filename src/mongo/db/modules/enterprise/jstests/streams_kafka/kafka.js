import {findMatchingLogLine} from "jstests/libs/log.js";
import {Thread} from "jstests/libs/parallelTester.js";
import {
    flushUntilStopped,
    LocalDiskCheckpointUtil,
} from "src/mongo/db/modules/enterprise/jstests/streams/checkpoint_helper.js";
import {Streams} from "src/mongo/db/modules/enterprise/jstests/streams/fake_client.js";
import {
    getStats,
    listStreamProcessors,
    makeRandomString,
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
const topicName3 = 'outputTopic3';
// Dynamic topic name expression for the $emit operator.
const topicNameExpr = {
    $cond: {if: {$eq: ["$gid", 0]}, then: topicName1, else: topicName2}
};
const dbName = 'test';
const sourceCollName1 = 'sourceColl1';
const sourceCollName2 = 'sourceColl2';
const sourceCollName3 = 'sourceColl3';
const sinkCollName1 = 'sinkColl1';
const sinkCollName2 = 'sinkColl2';
const dlqCollName = 'dlq';
const sourceColl1 = db.getSiblingDB(dbName)[sourceCollName1];
const sourceColl2 = db.getSiblingDB(dbName)[sourceCollName2];
const sourceColl3 = db.getSiblingDB(dbName)[sourceCollName3];
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
    },
    {name: '__testMemory', type: 'in_memory', options: {}},
];

const sp = new Streams(TEST_TENANT_ID, connectionRegistry);

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

function stopStreamProcessor(name, alreadyFlushedIds = [], skipCheckpointFlush = false) {
    jsTestLog(`Stopping ${name}`);
    let result = db.runCommand({streams_listStreamProcessors: '', tenantId: TEST_TENANT_ID});
    assert.commandWorked(result);
    const processor = result.streamProcessors.filter(s => s.name == name)[0];
    assert(processor != null);
    let flushThread = null;
    if (!skipCheckpointFlush) {
        const processorId = processor.processorId;
        flushThread = new Thread(flushUntilStopped,
                                 name,
                                 TEST_TENANT_ID,
                                 processorId,
                                 checkpointBaseDir,
                                 alreadyFlushedIds);
        flushThread.start();
    }

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
    sinkKey,
    sinkKeyFormat,
    sinkHeaders,
    jsonType,
    parseOnly = false,
    compressionType,
    acks,
    processorName = null,
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
    if (compressionType != null) {
        emitOptions.config.compression_type = compressionType;
    }
    if (acks != null) {
        emitOptions.config.acks = acks;
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
    let name = mongoToKafkaName;
    if (processorName != null) {
        name = processorName;
    }
    return {
        streams_startStreamProcessor: '',
        tenantId: TEST_TENANT_ID,
        name: name,
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
    parseOnly = false,
    enableAutoCommit,
    consumerGroupId,
    restoreDirectory = null,
    ephemeral = false,
}) {
    let topicNameForProcessorId;
    if (Array.isArray(topicName)) {
        topicNameForProcessorId = topicName[0];
    } else {
        topicNameForProcessorId = topicName;
    }
    const processorId = `processor-topic_${topicNameForProcessorId}-to-coll_${collName}`;
    jsTestLog(`processorId=${processorId} topicName=${topicName}`);

    if (restoreDirectory == null) {
        restoreDirectory = getRestoreDirectory(processorId);
    }
    let options = {
        checkpointOptions: {
            localDisk: {writeDirectory: checkpointBaseDir, restoreDirectory: restoreDirectory},
            // Checkpoint every five seconds.
            debugOnlyIntervalMs: 5000,
        },
        dlq: {connectionName: dbConnName, db: dbName, coll: dlqColl.getName()},
        featureFlags: {useExecutionPlanFromCheckpoint: true},
        ephemeral,
    };
    if (parseOnly) {
        options.parseOnly = true;
    }

    jsTestLog(`enableAutoCommit=${enableAutoCommit}`);

    return {
        streams_startStreamProcessor: '',
        tenantId: TEST_TENANT_ID,
        name: `${kafkaToMongoNamePrefix}-${topicNameForProcessorId}`,
        pipeline: pipeline.length ? pipeline : [
            {
                $source: {
                    connectionName: kafkaPlaintextName,
                    topic: topicName,
                    config: {
                        keyFormat: sourceKeyFormat,
                        keyFormatError: sourceKeyFormatError,
                        group_id: consumerGroupId,
                        enable_auto_commit: enableAutoCommit,
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
    sourceColl3.drop();
    sinkColl1.drop();
    sinkColl2.drop();
    dlqColl.drop();
}

const debugBuild = db.adminCommand("buildInfo").debug;
const defaultNumDocsToInsert = debugBuild ? 100 : 1000;
let numDocumentsToInsert = defaultNumDocsToInsert;
function generateInput(count = null, begIdx = 0) {
    let num = numDocumentsToInsert;
    if (count != null) {
        num = count;
    }
    let input = [];
    for (let i = begIdx; i < begIdx + num; i += 1) {
        const binData = new BinData(0, (i % defaultNumDocsToInsert).toString().padStart(4, "0"));
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

function insertData(coll, count = null, begIdx = 0) {
    const input = generateInput(count, begIdx);
    coll.insertMany(input);
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
    jsonType,
    compressionType,
    acks,
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
        sourceKeyFormat: sourceKeyFormat,
        sourceKeyFormatError: sourceKeyFormatError
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
        jsonType,
        compressionType,
        acks,
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

        if (results.length) {
            if (compressionType === undefined) {
                // If compressionType is undefined that means we will default to "none"
                // We set compressionType to "none" in these scenarios so we can still test that
                // "none" was the compression type used
                compressionType = "none";
            }

            let compressionTypesUsed = [];
            // We have to iterate over all the partitions because there may not be data in partition
            // 0 (default) even though there is data in the other partitions, and when that happens
            // the test will fail
            for (let i = 0; i < kafka.partitionCount; i++) {
                compressionTypesUsed =
                    compressionTypesUsed.concat(kafka.getCompressCodecDetails(topicName1, i));
            }

            // TODO (SERVER-95209) Fix retrieving compress codec details in amazonlinux2 systems.
            // We should investigate how we can get this working on amazonlinux2 systems so that
            // we can remove this if condition.
            if (compressionTypesUsed.length) {
                assert.gt(compressionTypesUsed.filter(used => used === compressionType).length, 0);
            }
        }

        // Verify that KafkaConsumerOperator is reporting non-zero maxMemoryUsage.
        const kafkaToMongoStatsResult = getStats(kafkaToMongoName);
        const kafkaSourceStats = kafkaToMongoStatsResult.operatorStats[0];
        assert.eq('KafkaConsumerOperator', kafkaSourceStats.name);
        assert.gt(kafkaSourceStats.maxMemoryUsage, 1000, kafkaToMongoStatsResult);

        // Verify that kafkaConsumerGroup is empty for the mongoToKafka stream processor
        const mongoToKafkaStatsResult = getStats(mongoToKafkaName);
        const mongoSourceStats = mongoToKafkaStatsResult.operatorStats[0];
        assert.eq(undefined, mongoSourceStats["kafkaConsumerGroup"]);
        assert.eq('ChangeStreamConsumerOperator', mongoSourceStats.name);
    }

    jsTestLog(`Stats for: ${kafkaToMongoName}, ${tojson(getStats(kafkaToMongoName))}`);
    jsTestLog(`Stats for: ${mongoToKafkaName}, ${tojson(getStats(mongoToKafkaName))}`);

    // Stop the streamProcessors.
    stopStreamProcessor(kafkaToMongoName);
    stopStreamProcessor(mongoToKafkaName);
}

// test that we can resume from a v2 checkpoint
function resumeFromCheckpointVersion2(numPartitions) {
    assert.eq(numPartitions, 12);
    makeSureKafkaTopicCreated(sourceColl1, topicName1, kafkaPlaintextName, 0);
    // Cleanup the source collection.
    sourceColl1.drop();

    // Now the Kafka topic exists, and it has at least count events in it.
    // Start kafkaToMongo, but set it up to resume from a previously taken checkpoint
    const kafkaToMongoProcessorId = `processor-topic_${topicName1}-to-coll_${sinkColl1.getName()}`;
    let checkpointUtils =
        new LocalDiskCheckpointUtil(checkpointBaseDir, TEST_TENANT_ID, kafkaToMongoProcessorId);
    assert.eq(0, checkpointUtils.checkpointIds);

    // This is the stored checkpoint with version2 that we will resume from
    const chkptId = "1726682845981";
    const srcDir =
        "src/mongo/db/modules/enterprise/jstests/streams_kafka/data/resume_test/checkpointVer2/" +
        chkptId;

    // sanity check to make sure that the checkpoint we are reading from is indeed a ver 2
    // checkpoint eslint-disable-next-line
    let manifest = _readDumpFile(srcDir + "/manifest.bson");
    manifest = manifest[0];
    assert.eq(manifest['version'], 2);

    // some other misc sanity checks
    assert.eq(manifest['metadata']['checkpointId'], chkptId);
    assert.eq(manifest['metadata']['operatorStats'][0]['stats']['name'], 'KafkaConsumerOperator');
    assert.eq(manifest['metadata']['operatorStats'][0]['stats']['inputDocs'], 10000);
    assert.eq(manifest['metadata']['operatorStats'][0]['stats']['outputDocs'], 10000);

    const destDir = checkpointUtils.streamProcessorCheckpointDir + "/" + chkptId;
    mkdir(destDir);
    // eslint-disable-next-line
    copyDir(srcDir, destDir);

    const kafkaToMongoStartCmd = makeKafkaToMongoStartCmd({
        topicName: topicName1,
        collName: sinkColl1.getName(),
        restoreDirectory: checkpointUtils.getRestoreDirectory(chkptId),
    });

    const kafkaToMongoName = kafkaToMongoStartCmd.name;
    assert.commandWorked(db.runCommand(kafkaToMongoStartCmd));

    assert.soon(() => {
        let statsResult = getStats(kafkaToMongoName);
        jsTestLog("statsResult *********");
        jsTestLog(statsResult);
        // Verify the expected message counts in the stats response.
        // These are restored from the checkpoint.
        assert.eq(10000, statsResult.inputMessageCount);
        assert.eq(10000, statsResult.outputMessageCount);
        assert.eq(0, statsResult.dlqMessageCount);

        if (!Object.hasOwn(statsResult, 'kafkaPartitions')) {
            return false;
        }

        let kafkaPartitions = statsResult["kafkaPartitions"];
        if (kafkaPartitions.length != 12) {
            return false;
        }

        for (let idx of [1, 4, 5, 7, 8, 9, 10, 11]) {
            if (kafkaPartitions[idx].currentOffset != 0) {
                return false;
            }
        }

        if (kafkaPartitions[0].currentOffset != 5999) {
            return false;
        }

        if (kafkaPartitions[2].currentOffset != 1) {
            return false;
        }

        if (kafkaPartitions[3].currentOffset != 1) {
            return false;
        }

        if (kafkaPartitions[6].currentOffset != 4000) {
            return false;
        }

        return true;
    }, "checking kafkaPartition states of resumed from checkpoint", 90000, 1000);

    // Stop the streamProcessor.
    stopStreamProcessor(kafkaToMongoName);
}

// test that we can resume from a v3 checkpoint
function resumeFromCheckpointVersion3(numPartitions) {
    // To obtain a checkpoint to resume from, follow these steps. (Looking into automating this
    // - mayuresh). The gist of it is to run the mongoToKafkaToMongo test and use the last
    // checkpoint for the kafka to mongo SP which would have been taken when the test stops.
    // 1. Run mongoToKafkaToMongo test by first modifying it to pump 10000 input docs and running it
    // with 12 partitions. It is not required, but is recomended to do this with multiple
    // partitions,
    // 2. Make a note of the final stats as obtained via getStats before stopping the processor.
    // These will have the partition offsets of the kafka partitions.
    // 3. Grab the latest directory in
    // /tmp/checkpointskafka/testTenant/processor-topic_outputTopic1-to-coll_sinkColl1. This
    //    will be the final checkpoint written as part of the processor stop.
    // 4. Convert the MANIFEST in the checkpoint to a regular bson file. This makes it easier to
    // inspect the MANIFEST from the test code: tail -c +5 MANIFEST > manifest.bson
    // 5. Now in a test, this checkpoint can be used as the restore-from checkpoint. To do this,
    // copy the entire checkpoint directory to any path accessible from this file location and
    // then use that directory as the restoreDirectory. Look at the invocation of
    // makeKafkaToMongoStartCmd below.
    assert.eq(numPartitions, 12);
    makeSureKafkaTopicCreated(sourceColl1, topicName1, kafkaPlaintextName, 0);
    // Cleanup the source collection.
    sourceColl1.drop();

    // Now the Kafka topic exists, and it has at least count events in it.
    // Start kafkaToMongo, but set it up to resume from a previously taken checkpoint
    const kafkaToMongoProcessorId = `processor-topic_${topicName1}-to-coll_${sinkColl1.getName()}`;
    let checkpointUtils =
        new LocalDiskCheckpointUtil(checkpointBaseDir, TEST_TENANT_ID, kafkaToMongoProcessorId);
    assert.eq(0, checkpointUtils.checkpointIds);

    // This is the stored checkpoint with version3 that we will resume from
    const chkptId = "1726778091150";
    const srcDir =
        "src/mongo/db/modules/enterprise/jstests/streams_kafka/data/resume_test/checkpointVer3/" +
        chkptId;

    // sanity check to make sure that the checkpoint we are reading from is indeed a ver 3
    // checkpoint eslint-disable-next-line
    let manifest = _readDumpFile(srcDir + "/manifest.bson");
    manifest = manifest[0];
    assert.eq(manifest['version'], 3);

    // sanity check to make sure that in the execution plan, the $source option has the topic
    // specified as a string. If topic is in an array format, that means this checkpoint was taken
    // by a SP that had started using the new syntax and so cannot be rolled back.
    assert.eq(manifest['metadata']['executionPlan'][0]['$source']['topic'], 'outputTopic1');

    // there should not be a summaryStats or pipelineVersion field
    assert(!manifest['metadata'].hasOwnProperty('summaryStats'));
    assert(!manifest['metadata'].hasOwnProperty('pipelineVersion'));

    // some other misc sanity checks.
    assert.eq(manifest['metadata']['checkpointId'], chkptId);
    assert.eq(manifest['metadata']['operatorStats'][0]['stats']['name'], 'KafkaConsumerOperator');
    assert.eq(manifest['metadata']['operatorStats'][0]['stats']['inputDocs'], 10000);
    assert.eq(manifest['metadata']['operatorStats'][0]['stats']['outputDocs'], 10000);

    const destDir = checkpointUtils.streamProcessorCheckpointDir + "/" + chkptId;
    mkdir(destDir);
    // eslint-disable-next-line
    copyDir(srcDir, destDir);

    const kafkaToMongoStartCmd = makeKafkaToMongoStartCmd({
        topicName: topicName1,
        collName: sinkColl1.getName(),
        restoreDirectory: checkpointUtils.getRestoreDirectory(chkptId),
    });

    const kafkaToMongoName = kafkaToMongoStartCmd.name;
    assert.commandWorked(db.runCommand(kafkaToMongoStartCmd));

    // Make sure that the useExecutionPlanFromCheckpoint feature flag is set to true. This
    // ensures that the current code is indeed parsing the pipeline from the checkpoint instead
    // of using the pipeline from the start request's options (where we will not have the array of
    // topics syntax)
    assert.soon(() => {
        let result = db.runCommand({
            streams_testOnlyGetFeatureFlags: '',
            tenantId: TEST_TENANT_ID,
            name: kafkaToMongoName
        });
        return (result.featureFlags.hasOwnProperty("useExecutionPlanFromCheckpoint") &&
                result.featureFlags['useExecutionPlanFromCheckpoint'] == true);
    });

    assert.soon(() => {
        let statsResult = getStats(kafkaToMongoName);
        jsTestLog("statsResult *********");
        jsTestLog(statsResult);
        // Verify the expected message counts in the stats response.
        // These are restored from the checkpoint.
        assert.eq(10000, statsResult.inputMessageCount);
        assert.eq(10000, statsResult.outputMessageCount);
        assert.eq(0, statsResult.dlqMessageCount);

        if (!Object.hasOwn(statsResult, 'kafkaPartitions')) {
            return false;
        }

        let kafkaPartitions = statsResult["kafkaPartitions"];
        if (kafkaPartitions.length != 12) {
            return false;
        }

        for (let idx of [0, 2, 3, 5, 8, 9, 10, 11]) {
            if (kafkaPartitions[idx].currentOffset != 0) {
                return false;
            }
        }

        if (kafkaPartitions[1].currentOffset != 5671) {
            return false;
        }

        if (kafkaPartitions[4].currentOffset != 4328) {
            return false;
        }

        if (kafkaPartitions[6].currentOffset != 1) {
            return false;
        }

        if (kafkaPartitions[7].currentOffset != 1) {
            return false;
        }

        return true;
    }, "checking kafkaPartition states of resumed from checkpoint", 90000, 1000);

    // Stop the streamProcessor.
    stopStreamProcessor(kafkaToMongoName);
}

// test that we can resume from a v4 checkpoint
function resumeFromCheckpointVersion4(numPartitions) {
    assert.eq(numPartitions, 12);
    makeSureKafkaTopicCreated(sourceColl1, topicName1, kafkaPlaintextName, 0);
    // Cleanup the source collection.
    sourceColl1.drop();

    // Now the Kafka topic exists, and it has at least count events in it.
    // Start kafkaToMongo, but set it up to resume from a previously taken checkpoint
    const kafkaToMongoProcessorId = `processor-topic_${topicName1}-to-coll_${sinkColl1.getName()}`;
    let checkpointUtils =
        new LocalDiskCheckpointUtil(checkpointBaseDir, TEST_TENANT_ID, kafkaToMongoProcessorId);
    assert.eq(0, checkpointUtils.checkpointIds);

    // This is the stored checkpoint with version4 that we will resume from
    const chkptId = "1728399161398";
    const srcDir =
        "src/mongo/db/modules/enterprise/jstests/streams_kafka/data/resume_test/checkpointVer4/" +
        chkptId;

    // sanity check to make sure that the checkpoint we are reading from is indeed a ver 4
    // checkpoint eslint-disable-next-line
    let manifest = _readDumpFile(srcDir + "/manifest.bson");
    manifest = manifest[0];
    assert.eq(manifest['version'], 4);

    // sanity check to make sure that in the execution plan, the $source option has the topic
    // specified as a string. If topic is in an array format, that means this checkpoint was taken
    // by a SP that had started using the new syntax and so cannot be rolled back.
    assert.eq(manifest['metadata']['executionPlan'][0]['$source']['topic'], 'outputTopic1');

    // there should be a summaryStats and pipelineVersion field
    assert(manifest['metadata'].hasOwnProperty('summaryStats'));
    assert(manifest['metadata'].hasOwnProperty('pipelineVersion'));

    // some other misc sanity checks.
    assert.eq(manifest['metadata']['checkpointId'], chkptId);
    assert.eq(manifest['metadata']['operatorStats'][0]['stats']['name'], 'KafkaConsumerOperator');
    assert.eq(manifest['metadata']['operatorStats'][0]['stats']['inputDocs'], 10000);
    assert.eq(manifest['metadata']['operatorStats'][0]['stats']['outputDocs'], 10000);

    const destDir = checkpointUtils.streamProcessorCheckpointDir + "/" + chkptId;
    mkdir(destDir);
    // eslint-disable-next-line
    copyDir(srcDir, destDir);

    const kafkaToMongoStartCmd = makeKafkaToMongoStartCmd({
        topicName: topicName1,
        collName: sinkColl1.getName(),
        restoreDirectory: checkpointUtils.getRestoreDirectory(chkptId),
    });

    const kafkaToMongoName = kafkaToMongoStartCmd.name;
    assert.commandWorked(db.runCommand(kafkaToMongoStartCmd));

    // Make sure that the useExecutionPlanFromCheckpoint feature flag is set to true. This
    // ensures that the current code is indeed parsing the pipeline from the checkpoint instead
    // of using the pipeline from the start request's options (where we will not have the array of
    // topics syntax)
    assert.soon(() => {
        let result = db.runCommand({
            streams_testOnlyGetFeatureFlags: '',
            tenantId: TEST_TENANT_ID,
            name: kafkaToMongoName
        });
        return (result.featureFlags.hasOwnProperty("useExecutionPlanFromCheckpoint") &&
                result.featureFlags['useExecutionPlanFromCheckpoint'] == true);
    });

    let statsResult = getStats(kafkaToMongoName);
    jsTestLog("statsResult *********");
    jsTestLog(statsResult);
    // Verify the expected message counts in the stats response.
    // These are restored from the checkpoint.
    assert.eq(10000, statsResult.inputMessageCount);
    assert.eq(10000, statsResult.outputMessageCount);
    assert.eq(0, statsResult.dlqMessageCount);

    assert(Object.hasOwn(statsResult, 'kafkaPartitions'));

    let kafkaPartitions = statsResult["kafkaPartitions"];
    assert.eq(kafkaPartitions.length, 12);
    for (let partition = 0; partition < 12; partition += 1) {
        assert.eq("outputTopic1", kafkaPartitions[0].topic);
        assert.eq(partition, kafkaPartitions[partition].partition);
    }

    // Verify the offsets in the stats (from the restore checkpoint).
    assert.eq(4223, kafkaPartitions[0].currentOffset);
    assert.eq(0, kafkaPartitions[1].currentOffset);
    assert.eq(0, kafkaPartitions[2].currentOffset);
    assert.eq(0, kafkaPartitions[3].currentOffset);
    assert.eq(1, kafkaPartitions[4].currentOffset);
    assert.eq(0, kafkaPartitions[5].currentOffset);
    assert.eq(0, kafkaPartitions[6].currentOffset);
    assert.eq(0, kafkaPartitions[7].currentOffset);
    assert.eq(0, kafkaPartitions[8].currentOffset);
    assert.eq(5776, kafkaPartitions[9].currentOffset);
    assert.eq(1, kafkaPartitions[10].currentOffset);
    assert.eq(0, kafkaPartitions[11].currentOffset);

    // Stop the streamProcessor.
    stopStreamProcessor(kafkaToMongoName);
}

// Test that when a pipeline involves a group, the _stream_meta.source.topic/partition are no
// longer projected.
function mongoToKafkaToMongoGroupStreamMeta({
    numPartitions,
} = {}) {
    assert.eq(numPartitions, 2);

    makeSureKafkaTopicCreated(sourceColl1, topicName1, kafkaPlaintextName);
    sourceColl1.drop();

    const groupPipeline = [
        {
            $source: {
                connectionName: kafkaPlaintextName,
                topic: topicName1,
                config: {
                    keyFormat: 'binData',
                    keyFormatError: 'dlq',
                },
                timeField: {$toDate: {$toLong: "$a"}},
            }
        },
        {$addFields: {topic: "$_stream_meta.source.topic"}},
        {$addFields: {partition: "$_stream_meta.source.partition"}},
        {
            // We are expecting to be able to access "_stream_meta.source.topic/partition in the
            // $group accumulators. They will stop being projected once the window closes
            $tumblingWindow: {
                interval: {size: NumberInt(5), unit: "second"},
                allowedLateness: {size: NumberInt(0), unit: "second"},
                pipeline: [{
                    $group: {
                        _id: "$a",
                        minPartition: {$min: "$partition"},
                        maxPartition: {$max: "$_stream_meta.source.partition"}
                    }
                }],
                idleTimeout: {size: NumberInt(2), unit: 'second'}
            },
        },
        // These will not get added since topic/partition no longer are projected
        {$addFields: {topicAfterGroup: "$_stream_meta.source.topic"}},
        {$addFields: {partitionAfterGroup: "$_stream_meta.source.partition"}},
        {$merge: {into: {connectionName: dbConnName, db: dbName, coll: sinkCollName1}}}
    ];

    const kafkaToMongoStartCmd = makeKafkaToMongoStartCmd({
        topicName: topicName1,
        collName: sinkColl1.getName(),
        pipeline: groupPipeline,
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

    let sinkKey = "$k";
    let sinkKeyFormat = "int";

    assert.commandWorked(db.runCommand(makeMongoToKafkaStartCmd({
        collName: sourceColl1.getName(),
        topicName: topicName1,
        connName: kafkaPlaintextName,
        sinkKey: sinkKey,
        sinkKeyFormat: sinkKeyFormat,
    })));

    // This input will form one window of 5 sec span
    let input = [
        {a: NumberInt(0), k: NumberInt(23129877)},
        {a: NumberInt(1), k: NumberInt(98897878)},
        {a: NumberInt(2), k: NumberInt(77888776)},
        {a: NumberInt(3), k: NumberInt(33123444)},
        {a: NumberInt(4), k: NumberInt(13122233)},
    ];
    sourceColl1.insertMany(input);
    let sourceCount = sourceColl1.find({}).count();

    // Each document in window will be in a separate group
    let expectedSinkCount = 5;

    assert.soon(() => {
        let dlqCount = dlqColl.find({}).count();
        if (dlqCount > 0) {
            let results = dlqColl.find({}).sort({a: 1}).toArray();
            for (let i = 0; i < results.length; i++) {
                jsTestLog(results[i]);
            }
        }
        assert.eq(dlqCount, 0);
        let sinkCount = sinkColl1.find({}).count();
        return sinkCount == expectedSinkCount;
    });

    // Verify output shows up in the sink collection as expected.
    let results = sinkColl1.find({}).sort({a: 1}).toArray();
    let partitionsSeen1 = new Set();
    let partitionsSeen2 = new Set();
    for (let i = 0; i < results.length; i++) {
        let outputDoc = results[i];
        jsTestLog(outputDoc);

        assert(Object.hasOwn(outputDoc['_stream_meta']['source'], 'type'));
        assert(!Object.hasOwn(outputDoc['_stream_meta']['source'], 'topic'));
        assert(!Object.hasOwn(outputDoc['_stream_meta']['source'], 'partition'));
        assert(Object.hasOwn(outputDoc['_stream_meta']['window'], 'start'));
        assert(Object.hasOwn(outputDoc['_stream_meta']['window'], 'end'));

        assert.eq(outputDoc["_stream_meta"]["source"]["type"], "kafka");

        assert(!Object.hasOwn(outputDoc, 'topicAfterGroup'));
        assert(!Object.hasOwn(outputDoc, 'partitionAfterGroup'));

        partitionsSeen1.add(outputDoc['minPartition']);
        partitionsSeen2.add(outputDoc['maxPartition']);
    }

    assert.eq(partitionsSeen1.size, 2);
    assert.eq(partitionsSeen2.size, 2);

    // Verify that KafkaConsumerOperator is reporting non-zero maxMemoryUsage.
    let statsResult = getStats(kafkaToMongoName);
    const sourceStats = statsResult.operatorStats[0];
    assert.eq('KafkaConsumerOperator', sourceStats.name);
    assert.gt(sourceStats.maxMemoryUsage, 1000, statsResult);

    // Stop the streamProcessors.
    stopStreamProcessor(kafkaToMongoName);
    stopStreamProcessor(mongoToKafkaName);
}

function mongoToKafkaToMongoMultiTopic({
    expectDlq,
    sinkKey,
    sinkKeyFormat,
    expectedKeyFunc,
    sinkHeaders,
    expectedSerializedHeaders,
    sourceKeyFormat,
    sourceKeyFormatError,
    jsonType,
    numTopics,
    numPartitions
} = {}) {
    assert(numTopics != null && numTopics > 0 && numTopics <= 3);
    assert(numPartitions != null && numPartitions > 0);

    let topicNames = [topicName1, topicName2, topicName3].slice(0, numTopics);
    let sourceColls = [sourceColl1, sourceColl2, sourceColl3].slice(0, numTopics);

    for (let i = 0; i < numTopics; i++) {
        makeSureKafkaTopicCreated(sourceColls[i], topicNames[i], kafkaPlaintextName);
        sourceColls[i].drop();
    }

    // Now the Kafka topics exist, and have least 1 events in them.
    // Start kafkaToMongo, which will write from the topic to the sink collection.
    // kafkaToMongo uses the default kafka startAt behavior, which starts reading
    // from the current end of topic. The event we wrote above
    // won't be included in the output in the sink collection.
    const kafkaToMongoStartCmd = makeKafkaToMongoStartCmd({
        topicName: topicNames,
        collName: sinkColl1.getName(),
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

    // Make sure that we are subscribed to the expected number of topics
    let partitions = getStats(kafkaToMongoName)["kafkaPartitions"];
    jsTestLog(partitions);
    assert.eq(partitions.length, numPartitions * numTopics);
    let foundTopics = [];
    for (let partition of partitions) {
        if (foundTopics.indexOf(partition.topic) == -1) {
            foundTopics.push(partition.topic);
        }
    }
    foundTopics.sort();
    assert.eq(foundTopics, topicNames);

    jsTestLog(`Verified that we are subscribed to ${numTopics} topics with ${
        numPartitions} partitions each`);

    // Start the mongoToKafka streamProcessors.
    // These are used to write more data to the Kafka topics used as input in the kafkaToMongo
    // stream processor.
    let processors = [];
    for (let i = 0; i < numTopics; i++) {
        let mongoToKafkaStartCmd = makeMongoToKafkaStartCmd({
            collName: sourceColls[i].getName(),
            topicName: topicNames[i],
            connName: kafkaPlaintextName,
            processorName: `mongoToKafka${i}`,
            sinkKey,
            sinkKeyFormat,
            sinkHeaders,
            jsonType,
        });
        assert.commandWorked(db.runCommand(mongoToKafkaStartCmd));
        processors.push(mongoToKafkaStartCmd.name);
    }

    // Write input to the 'sourceColl'.
    // mongoToKafka reads the source collection and writes to Kafka.
    // kafkaToMongo reads Kafka and writes to the sink collection.
    var input = [];
    for (let i = 0; i < numTopics; i++) {
        input = input.concat(insertData(sourceColls[i], numDocumentsToInsert, input.length));
    }

    jsTestLog(`Inserted input of length - ${input.length} - across - ${numTopics} topics`);

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

            assert.eq(outputDoc._stream_meta.source.topic,
                      topicNames[Math.floor(i / numDocumentsToInsert)],
                      outputDoc);
            assert.eq(outputDoc._stream_meta.source.key, expectedKey, outputDoc);
            assert.eq(outputDoc._stream_meta.source.headers, expectedHeaders, outputDoc);

            outputDoc = sanitizeDoc(outputDoc);
            delete outputDoc._id;
            assert.docEq(input[i], outputDoc, outputDoc);
        }

        // Verify that KafkaConsumerOperator is reporting non-zero maxMemoryUsage.
        let statsResult = getStats(kafkaToMongoName);
        const sourceStats = statsResult.operatorStats[0];
        assert.eq('KafkaConsumerOperator', sourceStats.name);
        assert.gt(sourceStats.maxMemoryUsage, 1000, statsResult);
    }

    // Stop the streamProcessors.
    stopStreamProcessor(kafkaToMongoName);
    for (let processor of processors) {
        stopStreamProcessor(processor);
    }
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
                interval: {size: NumberInt(100), unit: "ms"},
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
    // This is used to write more data to the Kafka topic used as input in the kafkaToMongo
    // stream processor.
    assert.commandWorked(db.runCommand(makeMongoToKafkaStartCmd(
        {collName: sourceColl1.getName(), topicName: topicName1, connName: kafkaPlaintextName})));

    // Write input to the 'sourceColl'.
    // mongoToKafka reads the source collection and writes to Kafka.
    // kafkaToMongo reads Kafka and writes to the sink collection.
    insertData(sourceColl1, 102);
    // Sleep for a bit so the next insert will advance the watermark and close the window.
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
    assert.eq(kafkaToMongoStartResult.connections.sort(),
              [
                  {name: kafkaPlaintextName, stage: "$source"},
                  {name: dbConnName, stage: "$merge"},
                  {name: dbConnName}
              ],
              kafkaToMongoStartResult);
    const mongoToKafkaStartResult = assert.commandWorked(db.runCommand(makeMongoToKafkaStartCmd({
        collName: sourceColl1.getName(),
        topicName: topicName1,
        connName: kafkaPlaintextName,
        parseOnly: true
    })));
    assert.eq(mongoToKafkaStartResult.connections.sort(),
              [
                  {name: dbConnName, stage: "$source"},
                  {name: kafkaPlaintextName, stage: "$emit"},
                  {name: dbConnName}
              ],
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

// Use a streamProcessor to write data from the source collection changestream to Kafka topics
// using dynamic name expression. Then use another streamProcessors to write data from those
// Kafka topics to the sink collections. Verify the data in sink collections equals to data that
// are suppoed to be inserted to each sink collection.
function mongoToDynamicKafkaTopicToMongo() {
    // Prepare topics.
    makeSureKafkaTopicCreated(sourceColl1, topicName1, kafkaPlaintextName);
    jsTestLog(`Created topic ${topicName1}`);
    makeSureKafkaTopicCreated(sourceColl2, topicName2, kafkaPlaintextName);
    jsTestLog(`Created topic ${topicName2}`);

    // Now the Kafka topic exists, and it has at least 1 event in it.
    // Start kafkaToMongo, which will write from the topics to the sink collections. The
    // kafkaToMongo uses the default kafka startAt behavior, which starts reading from the
    // current end of topic. The event we wrote above won't be included in the output in the
    // sink collections.
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

function kafkaConsumerGroupOffsetWithEnableAutoCommit(kafka, {enableAutoCommit, ephemeral} = {}) {
    makeSureKafkaTopicCreated(sourceColl1, topicName1, kafkaPlaintextName);

    const topicName = topicName1;
    const collName = sinkCollName1;
    const consumerGroupId = "consumer-group-withEnableAutoCommit";

    // Start KafkaToMongo SP with default enableAutoCommit (true).
    const startCmd = makeKafkaToMongoStartCmd(
        {topicName, collName, consumerGroupId, enableAutoCommit, ephemeral});
    assert.commandWorked(db.runCommand(startCmd));

    const docsToInsert = [{a: 1}, {b: 1}, {c: 1}];
    writeToTopic(topicName1, docsToInsert);

    // Because enableAutoCommit was enabled, the consumer group offset should be updated
    // every 500ms (at time of writing).
    // There is an initial period where the consumer group cannot commit/store offsets
    // due to it needing to rebalance partitions it's subscribed to. We should keep
    // inserting documents until the consumer group is ready to accept commits.
    let additionalDocsCount = 0;
    assert.soon(() => {
        additionalDocsCount++;
        writeToTopic(topicName1, [{a: additionalDocsCount}]);

        const res = kafka.getConsumerGroupId(consumerGroupId);
        if (!res || Object.keys(res).length === 0) {
            return false;
        }

        return res[0]?.current_offset > 0;
    }, `waiting for current_offset > ${docsToInsert.length}`);

    jsTestLog("waiting for consumer group to catch up");
    assert.soon(() => {
        const res = kafka.getConsumerGroupId(consumerGroupId);
        if (!res || Object.keys(res).length === 0) {
            return false;
        }

        return res[0]?.current_offset == docsToInsert.length + additionalDocsCount;
    }, `waiting for current_offset == ${docsToInsert.length + additionalDocsCount}`);

    if (ephemeral) {
        stopStreamProcessor(startCmd.name, [], true /* skipCheckpoint */);
    } else {
        stopStreamProcessor(startCmd.name);
    }
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
                            enable_auto_commit: false,
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
            // Mark all checkpoints as flushed. The streams Agent does this after the upload to
            // S3 completes.
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
            return res[0]?.current_offset == 1 && groupMembers.length == 1;
        }, "waiting for current_offset == 1");
        jsTestLog(`Last committed checkpoint: ${checkpointUtil.latestCheckpointId}`);

        // Write 3 more documents.
        writeToTopic(topicName1, [{a: 1}, {b: 1}, {c: 1}]);
        const numDocsBeforeStop = 4;
        // Wait for the processor to read the documents.
        assert.soon(() => getStats(startCmd.name).inputMessageCount == numDocsBeforeStop);

        // Ensure that even after the SP source reads more docs, the committed offset is still the
        // value from the last checkpoint. (because enable_auto_commit is false).
        const kafkaGroupIdResponse = kafka.getConsumerGroupId(consumerGroupId);
        assert.eq(1, kafkaGroupIdResponse[0]?.current_offset);

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
            // Mark all checkpoints as flushed. The streams Agent does this after the upload to
            // S3 completes.
            checkpointUtil.flushAll();

            // TODO(SERVER-87997): Sometimes consumerGroup.commitAsync fails, but the next
            // attempt works. So we write more checkpoints to work around that.
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
        assert.eq(consumerGroupId, stats["kafkaConsumerGroup"]);

        assert.eq(0, stats["kafkaPartitions"][0]["partition"]);
        assert.eq(input.length + numDocsBeforeStop, stats["kafkaPartitions"][0]["currentOffset"]);
        assert.eq(input.length + numDocsBeforeStop,
                  stats["kafkaPartitions"][0]["checkpointOffset"]);

        alreadyFlushedIds = checkpointUtil.flushAll();
        // Stop the stream processor.
        stopStreamProcessor(name, alreadyFlushedIds);
    };
}

function kafkaMultiTopicCheckpointTest(kafka, numPartitions) {
    assert.eq(numPartitions, 2);
    // Prepare two topics and write 1 event each to them
    makeSureKafkaTopicCreated(sourceColl1, topicName1, kafkaPlaintextName);
    makeSureKafkaTopicCreated(sourceColl2, topicName2, kafkaPlaintextName);

    sourceColl1.drop();
    sourceColl2.drop();
    insertData(sourceColl1, 1, 0);
    insertData(sourceColl2, 1, 1);

    const consumerGroupId = "consumer-group-1";
    const startCmd = {
        streams_startStreamProcessor: '',
        tenantId: TEST_TENANT_ID,
        name: `${kafkaToMongoNamePrefix}-${topicName1}`,
        pipeline: [
            {
                $source: {
                    connectionName: kafkaPlaintextName,
                    topic: [topicName1, topicName2],
                    config: {
                        auto_offset_reset: "earliest",
                        group_id: consumerGroupId,
                        enable_auto_commit: false,
                    },
                }
            },
            {$merge: {into: {connectionName: dbConnName, db: dbName, coll: sinkColl1.getName()}}}
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
        processorId: `processor-topic_${topicName1}_${topicName2}-to-coll_${sinkColl1.getName()}`,
    };
    const {processorId, name} = startCmd;
    const checkpointUtil =
        new LocalDiskCheckpointUtil(checkpointBaseDir, TEST_TENANT_ID, processorId, name);
    checkpointUtil.clear();

    // Start the kafka to mongo stream processor.
    assert.commandWorked(db.runCommand(startCmd));
    // Wait for messages to be read from Kafka and show up in the sink.
    waitForCount(sinkColl1, 2, /* timeout */ 60);
    // Wait for a current offset of 1 in the consumer group for each topic
    assert.soon(() => {
        // Mark all checkpoints as flushed. The streams Agent does this after the upload to S3
        // completes.
        checkpointUtil.flushAll();

        let groupMembers = kafka.getConsumerGroupMembers(consumerGroupId);
        if (!groupMembers) {
            return false;
        }
        jsTestLog("groupMembers=");
        jsTestLog(groupMembers);
        if (groupMembers.length != 1) {
            return false;
        }

        // TODO(SERVER-87997): Sometimes the consumerGroup.commitAsync fails if the stream
        // processor was just started... but the next attempt works. So we write more
        // checkpoints to work around that.
        db.runCommand(
            {streams_writeCheckpoint: '', tenantId: TEST_TENANT_ID, name: name, force: true});

        sleep(500);
        const res = kafka.getConsumerGroupId(consumerGroupId, true);
        if (res == null || Object.keys(res).length != 4) {
            return false;
        }

        // Only one message was sent to the kafka broker for each topic
        let current_offset = 0;
        for (let i = 0; i < 4; i++) {
            jsTestLog(`res[i=${i}]:`);
            jsTestLog(res[i]);
            try {
                let n = Number(res[i]["current_offset"]);
                current_offset += n;
            } catch (err) {
                return false;
            }
        }
        return current_offset == 2;
    }, "waiting for current_offset");
    jsTestLog(`Last committed checkpoint: ${checkpointUtil.latestCheckpointId}`);

    // Write 3 more documents to each topic
    writeToTopic(topicName1, [{a: 2}, {b: 2}, {c: 2}]);
    writeToTopic(topicName2, [{a: 3}, {b: 3}, {c: 3}]);
    const numDocsBeforeStop = 8;
    // Wait for the processor to read the documents.
    assert.soon(() => getStats(startCmd.name).inputMessageCount == numDocsBeforeStop);

    let alreadyFlushedIds = checkpointUtil.flushAll();
    // With the default checkpoint interval of (5 minutes), chances are we have not
    // written another checkpoint by now. Stop the processor which should write a final
    // checkpoint.
    stopStreamProcessor(name, alreadyFlushedIds);
    // Get the last committed checkpoint.
    const checkpointId = checkpointUtil.latestCheckpointId;

    // Delete all documents in the sink.
    sinkColl1.deleteMany({});

    // Insert more data into the kafka topic.
    // This data will span offsets 8 through (8+input.length).
    let input1 = generateInput(6 /* count */);
    writeToTopic(topicName1, input1);
    let input2 = generateInput(6, input1.length);
    writeToTopic(topicName2, input2);

    // Start a new stream processor. This SP will restore from the checkpoint and begin
    // at offset 8.
    const checkpointsFromLastRun = checkpointUtil.getCheckpointIds();
    checkpointUtil.flushedIds = checkpointUtil.flushedIds.concat(checkpointsFromLastRun);
    startCmd.options.checkpointOptions.localDisk.restoreDirectory =
        checkpointUtil.getRestoreDirectory(checkpointId);
    assert.commandWorked(db.runCommand(startCmd));
    for (const id of checkpointsFromLastRun) {
        // Delete these checkpoints so we don't call flush on them below.
        checkpointUtil.deleteCheckpointDirectory(id);
    }
    // Verify output 8 through 8+input.length shows up in the sink collection as expected.
    assert.soon(() => { return sinkColl1.count() == input1.length + input2.length; });

    // Wait for the Kafka consumer group offsets to be updated.
    assert.soon(() => {
        // Mark all checkpoints as flushed. The streams Agent does this after the upload to S3
        // completes.
        checkpointUtil.flushAll();

        // TODO(SERVER-87997): Sometimes consumerGroup.commitAsync fails, but the next attempt
        // works. So we write more checkpoints to work around that.
        db.runCommand(
            {streams_writeCheckpoint: '', tenantId: TEST_TENANT_ID, name: name, force: true});

        let res = kafka.getConsumerGroupId(consumerGroupId, true);
        if (Object.keys(res).length === 0) {
            return false;
        }

        let currOffset = 0;
        for (let i = 0; i < 4; i++) {
            currOffset += Number(res[i]["current_offset"]);
        }
        // +numDocsBeforeStop
        return currOffset == input1.length + input2.length + numDocsBeforeStop;
    }, `wait for current_offset == ${input1.length + input2.length + numDocsBeforeStop}`);

    // Ensure that we only processed `input` documents and not `input + 2`
    // because the stream processor should have resumed from when the last
    // committed offsets (1,1) rather than the "earliest" or "latest".
    assert.eq(input1.length + input2.length, sinkColl1.find({}).count());

    let totalMessages = input1.length + input2.length + numDocsBeforeStop;
    assert.soon(() => {
        const stats = getStats(name);
        assert.commandWorked(stats);
        assert.neq(undefined, stats["kafkaPartitions"]);
        assert.eq(4, stats["kafkaPartitions"].length);

        if (stats["inputMessageCount"] != totalMessages ||
            stats["outputMessageCount"] != totalMessages) {
            return false;
        }

        let currOffset = 0;
        let checkpointOffset = 0;
        for (let i = 0; i < stats["kafkaPartitions"].length; i++) {
            currOffset += Number(stats["kafkaPartitions"][i]["currentOffset"]);
            checkpointOffset += Number(stats["kafkaPartitions"][i]["checkpointOffset"]);
        }
        return currOffset == input1.length + input2.length + numDocsBeforeStop &&
            checkpointOffset == input1.length + input2.length + numDocsBeforeStop;
    });

    alreadyFlushedIds = checkpointUtil.flushAll();
    // Stop the stream processor.
    stopStreamProcessor(name, alreadyFlushedIds);
}

function kafkaStartAtEarliestTest() {
    // Create a new topic and write two documents to it.
    const numDocuments = 2;
    makeSureKafkaTopicCreated(
        sourceColl1, topicName1, kafkaPlaintextName, /* count */ numDocuments);

    const processorId = `processor-topic_${topicName1}-to-coll_${sinkColl1.getName()}`;
    // TODO(SERVER-93198): Remove this once the feature flag is removed.
    // We're just doing this to ensure the basic flow works when the feature flag
    // is off.
    let options = startOptions;
    options.featureFlags.kafkaEmitUserDeliveryCallback = false;

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
        options: options,
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

// Write data to only a single partition, validate partitionIdleTimeout works.
function testPartitionIdleTimeout() {
    dropCollections();

    const partitionCount = 32;
    let kafka = new LocalKafkaCluster();
    kafka.start(partitionCount);

    // Start a mongo->kafka processor and insert some data.
    const startCmd = makeMongoToKafkaStartCmd(
        {collName: sourceColl1.getName(), topicName: topicName1, connName: kafkaPlaintextName});
    startCmd.pipeline[startCmd.pipeline.length - 1]["$emit"].testOnlyPartition = NumberInt(0);
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
                    partitionIdleTimeout: {unit: "second", size: NumberInt(10)}
                }
            },
            {
                $tumblingWindow: {
                    interval: {unit: "second", size: NumberInt(1)},
                    pipeline: [{$group: {_id: null, count: {$count: {}}}}]
                }
            },
            {$project: {_id: 0}},
            {$merge: {into: {connectionName: dbConnName, db: dbName, coll: sinkColl1.getName()}}}
        ],
        connections: connectionRegistry,
        options: startOptions,
        processorId: kafkaToMongoName,
    }));

    assert.soon(() => {
        // Insert some data that the mongo->Kafka SP will pickup.
        sourceColl1.insert({a: 1});
        let stats = getStats(kafkaToMongoName);
        if (stats["outputMessageCount"] == 0) {
            return false;
        }
        // Validate partitions other than 0 are marked as idle in stats.
        for (let partition = 1; partition < partitionCount; partition += 1) {
            assert(stats["kafkaPartitions"][partition].isIdle);
        }
        return true;
    });

    // Now stop both stream processors.
    stopStreamProcessor(kafkaToMongoName);
    stopStreamProcessor(startCmd.name);
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
        // Kafka $emit errors are detected during flush. Write some data so checkpoints and
        // flushes happen and we detect the error.
        sourceColl1.insert({a: 1});
        let result = listStreamProcessors();
        let mongoToKafka = result.streamProcessors.filter(s => s.name == "mongoToKafka")[0];
        jsTestLog(mongoToKafka);
        return mongoToKafka.status == "error" &&
            mongoToKafka.error.reason.includes("Kafka $emit encountered error");

        // TODO(SERVER-89760): Figure out a reliable way to detect source connection failure.
        // let kafkaToMongo = result.streamProcessors.filter(s => s.name ==
        // kafkaToMongoName)[0]; return kafkaToMongo.status == "error" &&
        //     kafkaToMongo.error.reason.includes("Kafka $source partition 0 encountered error")
        //     && kafkaToMongo.error.reason.includes(
        //         "Connect to ipv4#127.0.0.1:9092 failed: Connection refused");
    }, "expected mongoToKafka processor to go into failed state");

    // Now stop both stream processors.
    stopStreamProcessor(kafkaToMongoName);
    stopStreamProcessor(startCmd.name);
}

// Verify that a streamProcessor goes into an error status when emitting to new topics for a Kafka
// broker with setting auto.create.topic = false
function testKafkaSinkAutoCreateTopicFalseError() {
    dropCollections();

    // Bring up a Kafka.
    const partitionCount = 1;
    let kafkaThatWillFail = new LocalKafkaCluster();
    kafkaThatWillFail.start(partitionCount, {KAFKA_AUTO_CREATE_TOPICS_ENABLE: false});

    // Start a mongo->kafka processor
    const startCmd = makeMongoToKafkaStartCmd(
        {collName: sourceColl1.getName(), topicName: topicName1, connName: kafkaPlaintextName});
    assert.commandWorked(db.runCommand(startCmd));

    // Sleep prior to the first document being produced. This ensures Kafka producer fetches topic
    // metadata and KafkaEmitOperator::_producer.produce returns the expected error when processing
    // the stream doc.
    sleep(30 * 1000);

    // Insert on source collection
    sourceColl1.insert({a: 1});

    // Assert that SP goes into error state with the expected error
    assert.soon(() => {
        let result = listStreamProcessors();
        let mongoToKafka = result.streamProcessors.filter(s => s.name == mongoToKafkaName)[0];
        jsTestLog(mongoToKafka);
        return mongoToKafka.status == "error" &&
            mongoToKafka.error.reason ===
            `Failed to emit to topic ${topicName1} due to error: Local: Unknown topic (-188)`;
    }, "expected mongoToKafka processor to go into failed state");

    // Now stop stream processors.
    stopStreamProcessor(startCmd.name);
}

// Verify that starting a stream processor returns an error when a Kafka with setting
// auto.create.topic = true is set as source with a non-existent topic
// TODO(SERVER-80885): Change this test assert that stream processor is succesfully started
function testKafkaSourceAutoCreateTopicTrueError() {
    dropCollections();

    // Bring up a Kafka.
    const partitionCount = 1;
    let kafkaThatWillFail = new LocalKafkaCluster();
    kafkaThatWillFail.start(partitionCount, {KAFKA_AUTO_CREATE_TOPICS_ENABLE: true});

    // Build command to start a processor with kafka source that specifies a non-existent topic
    const nonExistentTopic = "nonExistentTopic";
    const startCmd = makeKafkaToMongoStartCmd({
        topicName: nonExistentTopic,
        collName: sinkColl1,
    });

    // Assert that starting the processor returns the expected error
    let result = db.runCommand(startCmd);
    assert.neq(result["errmsg"], undefined);
    assert.eq(result["errmsg"],
              `no partitions found in topic ${nonExistentTopic}. Does the topic exist?`);
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
    // This is used to write more data to the Kafka topic used as input in the kafkaToMongo
    // stream processor.
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

// Test that KafkaConsumerOperator stays within the configured memory usage limits.
function honorSourceBufferSizeLimit() {
    // Start a stream processor that reads from sourceColl1 and writes to the Kafka topic.
    let processorName = 'sp';
    sp.createStreamProcessor(processorName, [
        {$source: {connectionName: dbConnName, db: dbName, coll: sourceColl1.getName()}},
        {$match: {operationType: "insert"}},
        {$replaceRoot: {newRoot: "$fullDocument"}},
        {$project: {_stream_meta: 0}},
        {
            $emit: {
                connectionName: kafkaPlaintextName,
                topic: topicName1,
                config: {outputFormat: 'canonicalJson'}
            }
        }
    ]);

    let processor = sp[processorName];
    processor.start();

    // Add 1000 input docs each of size ~1KB to the input collection.
    let numDocs = 10000;
    Random.setRandomSeed(42);
    const str = makeRandomString(1000);
    Array.from({length: numDocs}, (_, i) => i)
        .forEach(idx => { assert.commandWorked(sourceColl1.insert({_id: idx, str: str})); });

    // Wait until all the docs are emitted.
    assert.soon(() => {
        let statsResult = getStats(processorName);
        return statsResult.outputMessageCount == numDocs;
    });

    processor.stop();

    // Test that maxMemoryUsage reported by KafkaConsumerOperator stays within the configured limit.
    let numProcessors = 10;
    let featureFlags = {
        sourceBufferTotalSize: NumberLong(500 * 1024 * 1024),
        sourceBufferMaxSize: NumberLong(5 * 1024),
        sourceBufferMinPageSize: NumberLong(1024),
        sourceBufferMaxPageSize: NumberLong(1024)
    };

    // Start all the stream processors.
    for (let spIdx = 1; spIdx <= numProcessors; ++spIdx) {
        processorName = "sp" + spIdx;
        sp.createStreamProcessor(processorName, [
            {
                $source: {
                    connectionName: kafkaPlaintextName,
                    topic: topicName1,
                    config: {auto_offset_reset: "earliest"},
                }
            },
            {$emit: {connectionName: '__testMemory'}}
        ]);

        const processor = sp[processorName];
        processor.start({featureFlags: featureFlags});
    }

    // Wait until all stream processors are done processing all the input docs.
    assert.soon(() => {
        let allDone = true;
        for (let spIdx = 1; spIdx <= numProcessors; ++spIdx) {
            const processorName = "sp" + spIdx;
            let statsResult = getStats(processorName);
            if (statsResult.outputMessageCount < numDocs) {
                allDone = false;
            }
        }
        return allDone;
    });

    // Verify stats and stop all stream processors.
    for (let spIdx = 1; spIdx <= numProcessors; ++spIdx) {
        const processorName = "sp" + spIdx;
        let statsResult = getStats(processorName);
        jsTestLog(statsResult);
        const sourceStats = statsResult.operatorStats[0];
        assert.eq('KafkaConsumerOperator', sourceStats.name);
        assert.eq(sourceStats.stateSize, 0, statsResult);
        assert.gt(sourceStats.maxMemoryUsage, 1000, statsResult);
        // KafkaPartitionConsumer sets consume.callback.max.messages to 500. So it can only enforce
        // memory limits after reading up to 500 docs. So we need to have higher tolerance in our
        // checks than we'd like.
        assert.lt(sourceStats.maxMemoryUsage, 1.5 * 1024 * 1024, statsResult);
        sp[processorName].stop();
    }
    sourceColl1.drop();
}

function testSourceBufferManagerErrorHandling() {
    let processorName = 'sp';
    sp.createStreamProcessor(processorName, [
        {$source: {connectionName: dbConnName, db: dbName, coll: sourceColl1.getName()}},
        {$match: {operationType: "insert"}},
        {$replaceRoot: {newRoot: "$fullDocument"}},
        {$project: {_stream_meta: 0}},
        {
            $emit: {
                connectionName: kafkaPlaintextName,
                topic: topicName1,
                config: {outputFormat: 'canonicalJson'}
            }
        }
    ]);

    let processor = sp[processorName];
    processor.start();

    // Add 100 input docs to the input collection.
    let numDocs = 100;
    Array.from({length: numDocs}, (_, i) => i)
        .forEach(idx => { assert.commandWorked(sourceColl1.insert({_id: idx})); });

    // Wait until all the docs are emitted.
    assert.soon(() => {
        let statsResult = getStats(processorName);
        return statsResult.outputMessageCount == numDocs;
    });

    processor.stop();

    sp.createStreamProcessor(processorName, [
        {
            $source: {
                connectionName: kafkaPlaintextName,
                topic: topicName1,
                config: {auto_offset_reset: "earliest"},
            }
        },
        {$emit: {connectionName: '__testMemory'}}
    ]);

    processor = sp[processorName];

    let featureFlags = {
        sourceBufferTotalSize: NumberLong(2 * 1024),
        sourceBufferPreallocationFraction: 1.0,
        sourceBufferMaxSize: NumberLong(1024),
        sourceBufferPageSize: NumberLong(1024)
    };
    let result = processor.start({featureFlags: featureFlags}, /* assertWorked */ false);
    assert.commandFailedWithCode(result, ErrorCodes.InternalError);
    assert(result.errmsg.includes(
               "Cannot preallocate even a single page to all the available source buffers"),
           tojson(result));
    result = processor.stop(/* assertWorked */ false);
    assert.commandFailedWithCode(result, ErrorCodes.StreamProcessorDoesNotExist);

    sourceColl1.drop();
}

// Test that content-based routing feature of $emit works as expected.
function contentBasedRouting() {
    // Start a stream processor that reads from sourceColl1 and writes to the Kafka topic.
    let processorName = 'sp';
    sp.createStreamProcessor(processorName, [
        {$source: {connectionName: dbConnName, db: dbName, coll: sourceColl1.getName()}},
        {$match: {operationType: "insert"}},
        {$replaceRoot: {newRoot: "$fullDocument"}},
        {$project: {_stream_meta: 0}},
        {
            $emit: {
                connectionName: kafkaPlaintextName,
                topic: topicName1,
                config: {outputFormat: 'canonicalJson'}
            }
        }
    ]);

    let processor = sp[processorName];
    processor.start();

    // Add 200 input docs to the input collection.
    let numInputDocs = 200;
    Random.setRandomSeed(42);
    const str = makeRandomString(100);
    Array.from({length: numInputDocs}, (_, i) => i)
        .forEach(
            idx => { assert.commandWorked(sourceColl1.insert({_id: idx.toString(), str: str})); });

    // Wait until all the docs are emitted.
    assert.soon(() => {
        let statsResult = getStats(processorName);
        return statsResult.outputMessageCount == numInputDocs;
    });

    processor.stop();

    // Start a stream processor that writes 100 docs each to 200 Kafka topics.
    sp.createStreamProcessor(processorName, [
        {
            $source: {
                connectionName: kafkaPlaintextName,
                topic: topicName1,
                config: {auto_offset_reset: "earliest"},
            }
        },
        {$addFields: {i: {$range: [0, 10]}}},
        {$unwind: "$i"},
        {
            $addFields:
                {ii: {$range: [{$multiply: ["$i", 10]}, {$multiply: [{$add: ["$i", 1]}, 10]}]}}
        },
        {$unwind: "$ii"},
        {$emit: {connectionName: kafkaPlaintextName, topic: "$_id"}}
    ]);

    processor = sp[processorName];

    let options = {
        dlq: {connectionName: dbConnName, db: dbName, coll: dlqColl.getName()},
        featureFlags: {},
    };
    processor.start(options);

    // Wait until all stream processors are done processing all the input docs.
    let numOutputDocs = numInputDocs * 100;
    assert.soon(() => {
        let statsResult = getStats(processorName);
        return statsResult.outputMessageCount == numOutputDocs;
    });

    processor.stop();
    sourceColl1.drop();
}

// A test to help repro some Kafka $source OOM issues we saw in prod.
// The test writes and reads a lot of data to a 32 partition Kafka broker.
// Use the below to run a local mongod in a cgroup with limited memory:
//  systemd-run --user --scope -p MemoryMax=2G ./mongod --port 27017 --dbpath tmpdata \
//  --logpath log1 --bind_ip localhost --setParameter featureFlagStreams=true --replSet "rs0" --fork
function bigKafka() {
    Random.setRandomSeed(42);

    // Use the SP10's default setting of 128MB total in its rdkafka source queues.
    const options = {featureFlags: {"kafkaTotalQueuedBytes": 128 * 1024 * 1024}};

    // Write a bunch to a 32 partition Kafka topic.
    const numDocsInBatch = 5001;
    let arr = [];
    const str = makeRandomString(128);
    for (let i = 0; i < numDocsInBatch; i += 1) {
        arr.push({i: i});
    }
    let inputData = [];

    let numInput = 2;
    let runBigKafka = _getEnv("RUN_BIG_KAFKA");
    if (runBigKafka === "true") {
        jsTestLog("Running big kafka!");
        numInput = 5000;
    }
    for (let i = 0; i < numInput; i += 1) {
        inputData.push({a: i, arr: arr, str: str});
    }
    let spName = "writer";
    const totalInput = arr.length * inputData.length;
    sp.createStreamProcessor(spName, [
        {$source: {'connectionName': '__testMemory'}},
        {$unwind: "$arr"},
        {
            $emit: {
                connectionName: kafkaPlaintextName,
                topic: topicName1,
            }
        }
    ]);
    sp[spName].start(options);
    for (const doc of inputData) {
        sp[spName].testInsert(doc);
    }
    assert.soon(() => { return sp[spName].stats().outputMessageCount == totalInput; },
                "took too long to write",
                60 * 1000 * 10);
    sp[spName].stop();

    // Now read from that topic into a $merge.
    spName = "reader";
    sp.createStreamProcessor(spName, [
        {
            $source: {
                connectionName: kafkaPlaintextName,
                topic: topicName1,
                config: {auto_offset_reset: "earliest"}
            }
        },
        {$project: {_id: {a: "$a", b: "$arr.i"}}},
        {
            $emit: {
                connectionName: kafkaPlaintextName,
                topic: topicName1 + "out",
            }
        }
    ]);
    // Without this feature flag, the test will fail targetting a mongod
    // with only 1GB of memory.
    sp[spName].start(options);
    assert.soon(() => { return sp[spName].stats().outputMessageCount == totalInput; },
                "took too long to read",
                60 * 1000 * 10);
    jsTestLog(sp[spName].stats());
    sp[spName].stop();

    // Use the logs to verify we set rdkafka's queue size.
    const log = assert.commandWorked(db.adminCommand({getLog: "global"})).log;
    const line = findMatchingLogLine(log, {id: 9649600});
    const entry = JSON.parse(line);
    assert.eq("Setting rdkafka queue size", entry.msg);
    assert.eq(4096, entry.attr.queuedMaxMessagesKBytes);
    assert.eq(4128768, entry.attr.fetchMaxBytes);
}

let kafka = new LocalKafkaCluster();
runKafkaTest(kafka, mongoToKafkaToMongo);
runKafkaTest(kafka, mongoToKafkaToMongo, 12);

runKafkaTest(kafka, () => resumeFromCheckpointVersion2(12), 12);
runKafkaTest(kafka, () => resumeFromCheckpointVersion3(12), 12);
runKafkaTest(kafka, () => resumeFromCheckpointVersion4(12), 12);

runKafkaTest(kafka, () => mongoToKafkaToMongoMultiTopic({numTopics: 1, numPartitions: 10}), 10);
runKafkaTest(kafka, () => mongoToKafkaToMongoMultiTopic({numTopics: 2, numPartitions: 5}), 5);
runKafkaTest(kafka, () => mongoToKafkaToMongoMultiTopic({numTopics: 3, numPartitions: 1}), 1);
runKafkaTest(kafka, () => mongoToKafkaToMongoMultiTopic({numTopics: 3, numPartitions: 3}), 3);
runKafkaTest(kafka, () => kafkaMultiTopicCheckpointTest(kafka, 2), 2);
runKafkaTest(kafka, () => mongoToKafkaToMongoGroupStreamMeta({numPartitions: 2}), 2);

runKafkaTest(kafka, () => mongoToKafkaToMongo({compressionType: "gzip"}));
runKafkaTest(kafka, () => mongoToKafkaToMongo({compressionType: "snappy"}));
runKafkaTest(kafka, () => mongoToKafkaToMongo({compressionType: "lz4"}));
runKafkaTest(kafka, () => mongoToKafkaToMongo({compressionType: "zstd"}));
runKafkaTest(kafka, () => mongoToKafkaToMongo({compressionType: "none"}));

runKafkaTest(kafka, () => mongoToKafkaToMongo({acks: "all"}));
runKafkaTest(kafka, () => mongoToKafkaToMongo({acks: "-1"}));
runKafkaTest(kafka, () => mongoToKafkaToMongo({acks: "0"}));
runKafkaTest(kafka, () => mongoToKafkaToMongo({acks: "1"}));

runKafkaTest(kafka,
             () => mongoToKafkaToMongoMaintainStreamMeta({$limit: 1} /* nonGroupWindowStage */));
runKafkaTest(kafka, () => mongoToKafkaToMongoMaintainStreamMeta({
                        $sort: {a: 1}
                    } /* nonGroupWindowStage
                       */));
runKafkaTest(kafka, mongoToDynamicKafkaTopicToMongo);
runKafkaTest(kafka, mongoToKafkaSASLSSL);
runKafkaTest(kafka, kafkaConsumerGroupIdWithNewCheckpointTest(kafka));
runKafkaTest(kafka, () => kafkaConsumerGroupOffsetWithEnableAutoCommit(kafka));
runKafkaTest(kafka,
             () => kafkaConsumerGroupOffsetWithEnableAutoCommit(kafka, {enableAutoCommit: true}));
runKafkaTest(kafka, () => kafkaConsumerGroupOffsetWithEnableAutoCommit(kafka, {ephemeral: true}));
runKafkaTest(kafka, kafkaStartAtEarliestTest);

// offset lag in verbose stats
runKafkaTest(kafka, testKafkaOffsetLag);

if (!debugBuild) {
    // Use a large number of documents on release builds.
    // This takes to long on debug builds.
    numDocumentsToInsert = 100000;
}
runKafkaTest(kafka, mongoToKafkaToMongo, 12);

numDocumentsToInsert = defaultNumDocsToInsert;
runKafkaTest(kafka, () => mongoToKafkaToMongo({expectDlq: false, jsonType: "relaxedJson"}));
runKafkaTest(kafka, () => mongoToKafkaToMongo({expectDlq: false, jsonType: "canonicalJson"}));

testKafkaAsyncError();
testKafkaSinkAutoCreateTopicFalseError();
testKafkaSourceAutoCreateTopicTrueError();

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

runKafkaTest(kafka, honorSourceBufferSizeLimit);

runKafkaTest(kafka, testSourceBufferManagerErrorHandling, 10);

runKafkaTest(kafka, () => {
    // The tests many small batches of 1 document flowing into $emit.
    const numInputDocs = 100001;
    let inputData = [];
    for (let i = 0; i < numInputDocs; i += 1) {
        inputData.push({a: i});
    }
    const spName = "manyTinyBatches";
    sp.createStreamProcessor(spName, [
        {$source: {'connectionName': '__testMemory'}},
        {
            $emit: {
                connectionName: kafkaPlaintextName,
                topic: topicName1,
            }
        }
    ]);
    sp[spName].start();
    for (const doc of inputData) {
        sp[spName].testInsert(doc);
    }
    assert.soon(() => { return sp[spName].stats().outputMessageCount == inputData.length; });
    sp[spName].stop();
});

runKafkaTest(kafka, () => {
    // This tests a single large batch (500k documents) flowing into $emit.
    const numDocsInBatch = 500001;
    let arr = [];
    for (let i = 0; i < numDocsInBatch; i += 1) {
        arr.push({i: i});
    }
    let inputData = [{a: 1, arr: arr}];
    const spName = "manyTinyBatches";
    sp.createStreamProcessor(spName, [
        {$source: {'connectionName': '__testMemory'}},
        {$unwind: "$arr"},
        {
            $emit: {
                connectionName: kafkaPlaintextName,
                topic: topicName1,
            }
        }
    ]);
    sp[spName].start();
    for (const doc of inputData) {
        sp[spName].testInsert(doc);
    }
    assert.soon(() => { return sp[spName].stats().outputMessageCount == numDocsInBatch; });
    sp[spName].stop();
});

runKafkaTest(kafka, contentBasedRouting);

testPartitionIdleTimeout();

runKafkaTest(kafka, bigKafka, 32 /* partitionCount */);
