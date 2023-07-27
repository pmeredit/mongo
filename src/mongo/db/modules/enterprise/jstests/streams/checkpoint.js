/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */
(function() {
"use strict";

load("jstests/aggregation/extras/utils.js");  // For assertErrorCode().
load('src/mongo/db/modules/enterprise/jstests/streams/fake_client.js');
load('src/mongo/db/modules/enterprise/jstests/streams/utils.js');

function generateInput(size, msPerDocument) {
    let input = [];
    let baseTs = ISODate("2023-01-01T00:00:00.000Z");
    for (let i = 0; i < size; i++) {
        let ts = new Date(baseTs.getTime() + msPerDocument * i);
        input.push({
            ts: ts,
            idx: i,
            a: 1,
        });
    }
    return input;
}

function uuidStr() {
    return UUID().toString().split('"')[1];
}

function removeProjections(doc) {
    delete doc._ts;
    delete doc._stream_meta;
}

class TestHelper {
    constructor(input, middlePipeline, interval = 0) {
        this.input = input;
        this.uri = 'mongodb://' + db.getMongo().host;
        this.kafkaConnectionName = "kafka1";
        this.kafkaBootstrapServers = "localhost:9092";
        this.kafkaIsTest = true;
        this.kafkaTopic = "topic1";
        this.dbConnectionName = "db1";
        this.dbName = "test";
        this.dlqCollName = uuidStr();
        this.outputCollName = uuidStr();
        this.processorId = uuidStr();
        this.tenantId = uuidStr();
        this.outputColl = db.getSiblingDB(this.dbName)[this.outputCollName];
        this.spName = uuidStr();
        this.checkpointCollName = uuidStr();
        this.checkpointColl = db.getSiblingDB(this.dbName)[this.checkpointCollName];
        this.checkpointIntervalMs = NumberInt(interval);
        this.startOptions = {
            dlq: {connectionName: this.dbConnectionName, db: this.dbName, coll: this.dlqCollName},
            checkpointOptions: {
                storage: {
                    uri: this.uri,
                    db: this.dbName,
                    coll: this.checkpointCollName,
                },
                debugOnlyIntervalMs: this.checkpointIntervalMs,
            },
        };
        this.connectionRegistry = [
            {name: this.dbConnectionName, type: 'atlas', options: {uri: this.uri}},
            {
                name: this.kafkaConnectionName,
                type: 'kafka',
                options: {
                    bootstrapServers: this.kafkaBootstrapServers,
                    isTestKafka: this.kafkaIsTest,
                },
            }
        ];

        this.pipeline = [{
            $source: {
                connectionName: this.kafkaConnectionName,
                topic: this.kafkaTopic,
                partitionCount: NumberInt(1),
                timeField: {
                    $toDate: "$ts",
                },
                allowedLateness: {size: NumberInt(0), unit: "second"}
            }
        }];
        for (let stage of middlePipeline) {
            this.pipeline.push(stage);
        }
        this.pipeline.push({
            $merge: {
                into: {
                    connectionName: this.dbConnectionName,
                    db: this.dbName,
                    coll: this.outputCollName
                }
            }
        });
        this.sp = new Streams(this.connectionRegistry);
    }

    // Helper functions.
    run() {
        this.sp.createStreamProcessor(this.spName, this.pipeline);
        this.sp[this.spName].start(this.startOptions, this.processorId, this.tenantId);
        // Insert the input.
        assert.commandWorked(db.runCommand({
            streams_testOnlyInsert: '',
            name: this.spName,
            documents: this.input,
        }));
    }

    stop() {
        this.sp[this.spName].stop();
    }

    getCheckpointIds() {
        return this.checkpointColl.find({_id: {$regex: "^checkpoint"}}).sort({_id: -1}).toArray();
    }

    getResults() {
        return this.outputColl.find({}).sort({idx: 1}).toArray();
    }

    getStartOffsetFromCheckpoint(checkpointId) {
        let sourceState =
            this.checkpointColl
                .find({
                    _id: {$regex: `^operator/${checkpointId.replace("checkpoint/", "")}/00000000`}
                })
                .sort({_id: -1})
                .toArray();
        assert.eq(1, sourceState.length, "expected only 1 state document for $source");
        return sourceState[0]["state"]["partitions"][0]["offset"];
    }
}

function smokeTestCorrectness() {
    const input = generateInput(2000);
    let test = new TestHelper(input, []);

    // Run the streamProcessor for the first time.
    test.run();
    // Wait until the last doc in the input appears in the output collection.
    waitForDoc(test.outputColl, (doc) => doc.idx == input.length - 1, /* maxWaitSeconds */ 60);
    test.stop();
    // Verify the output matches the input.
    let results = test.getResults();
    assert.eq(results.length, input.length);
    for (let i = 0; i < results.length; i++) {
        assert.eq(input.length[i], removeProjections(results[i]));
    }
    // Get the checkpoint IDs from the run.
    let ids = test.getCheckpointIds();
    // Note: this minimum is an implementation detail subject to change.
    // KafkaConsumerOperator has a maximum docs per run, hardcoded to 500.
    // Since the checkpoint interval is 0, we should see at least see a checkpoint
    // after every batch of 500 docs.
    const maxDocsPerRun = 500;
    const expectedMinimumCheckpoints = input.length / maxDocsPerRun;
    assert.gte(ids.length,
               expectedMinimumCheckpoints,
               `expected at least ${expectedMinimumCheckpoints} checkpoints`);

    // Replay from each written checkpoint and verify the results are expected.
    let replaysWithData = 0;
    while (ids.length > 0) {
        const id = ids.shift();
        const startingOffset = test.getStartOffsetFromCheckpoint(id._id);
        const expectedOutputCount = input.length - startingOffset;

        // Clean the output.
        test.outputColl.deleteMany({});
        // Run the streamProcessor.
        test.run();
        if (expectedOutputCount > 0) {
            // If we're expected to output anything from this checkpoint,
            // wait for the last document in the input.
            waitForDoc(
                test.outputColl, (doc) => doc.idx == input.length - 1, /* maxWaitSeconds */ 60);
            replaysWithData++;
        } else {
            // Else, just sleep 5 seconds, we will verify nothing is output after this.
            sleep(1000 * 5);
        }
        test.stop();
        // Verify the results.
        let results = test.getResults();
        // ex. if the checkpoint starts at offset 500, we verify the output
        // to contain input[500] to input[length - 1].
        assert.eq(results.length, expectedOutputCount);
        for (let i = 0; i < results.length; i++) {
            assert.eq(input.length[i + startingOffset], removeProjections(results[i]));
        }
        // This deletes the id we just verified, and any new
        // checkpoints this replay created.
        test.checkpointColl.deleteMany({_id: {$nin: ids.map(id => id._id), $not: /^operator/}});
    }
    assert.gt(replaysWithData, 0);
}

function smokeTestCorrectnessTumblingWindow() {
    const maxDocsPerRun = 500;
    const inputSize = 10 * maxDocsPerRun;
    const msPerDoc = 1;
    const windowInterval = {size: NumberInt(1), unit: "second"};
    const windowSizeMs = 1000;
    const docsPerWindow = windowSizeMs / msPerDoc;
    const expectedWindowCount = Math.floor(inputSize * msPerDoc / windowSizeMs);
    const input = generateInput(inputSize + 2, msPerDoc);
    const pipeline = [
        {
            $tumblingWindow: {
                interval: windowInterval,
                pipeline: [{
                    $group: {
                        _id: null,
                        sum: {$sum: "$a"},
                        minIdx: {$min: "$idx"},
                        maxIdx: {$max: "$idx"}
                    }
                }]
            }
        },
        {$project: {_id: {$toString: "$minIdx"}}}
    ];
    let test = new TestHelper(input, pipeline);

    test.run();
    waitForCount(test.outputColl, expectedWindowCount, 60);
    test.stop();
    let originalResults = test.getResults();
    assert.eq(expectedWindowCount, originalResults.length);
    let ids = test.getCheckpointIds();
    assert.gt(ids.length, 0, `expected some checkpoints`);

    // Replay from each written checkpoint and verify the results are expected.
    let replaysWithData = 0;
    while (ids.length > 0) {
        const id = ids.shift();
        const startingOffset = test.getStartOffsetFromCheckpoint(id._id);
        const expectedInputCount = input.length - startingOffset;
        const expectedOutputCount = Math.floor(expectedInputCount / docsPerWindow);
        // Clean the output.
        test.outputColl.deleteMany({});
        // Run the streamProcessor.
        test.run();
        if (expectedOutputCount > 0) {
            // Wait for the expected output to arrive.
            waitForCount(test.outputColl, expectedOutputCount);
            replaysWithData++;
        } else {
            // Else, just sleep 5 seconds, we will verify nothing is output after this.
            sleep(1000 * 5);
        }
        test.stop();
        // Verify the results.
        let results = test.getResults();
        assert.eq(results.length, expectedOutputCount);
        assert.lte(results.length, originalResults.length);
        for (let i = 0; i < results.length; i++) {
            let originalResult = originalResults[originalResults.length - 1 - i];
            let restoreResult = results[results.length - 1 - i];
            assert.eq(originalResult, restoreResult);
        }
        // This deletes the id we just verified, and any new
        // checkpoints this replay created.
        test.checkpointColl.deleteMany({_id: {$nin: ids.map(id => id._id), $not: /^operator/}});
    }
    assert.gt(replaysWithData, 0);
}

function smokeTestCheckpointOnStop() {
    // Use a large input so it is not finished by the time we call stop.
    const input = generateInput(12347);
    // Use a long checkpoint interval so they don't automatically happen.
    let test = new TestHelper(input, [], /* interval */ 1000000000);
    // Run the streamProcessor the streamProcessor.
    test.run();
    // Wait until some output appears in the output collection.
    waitForCount(test.outputColl, 1, /* maxWaitSeconds */ 60);
    test.stop();
    // Verify we did not output everything.
    let results = test.getResults();
    assert.lt(results.length, input.length);
    for (let i = 0; i < results.length; i++) {
        assert.eq(input.length[i], removeProjections(results[i]));
    }
    // Get the checkpoint IDs from the run.
    let ids = test.getCheckpointIds();
    // There should be 1 checkpoint ID from the coordinator startup, and
    // 1 checkpoint ID from the stop.
    assert.eq(2, ids.length);
}

smokeTestCorrectness();
smokeTestCorrectnessTumblingWindow();
smokeTestCheckpointOnStop();
}());