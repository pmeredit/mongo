/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */

import {findMatchingLogLines} from "jstests/libs/log.js";
import {Streams} from "src/mongo/db/modules/enterprise/jstests/streams/fake_client.js";
import {waitForCount, waitForDoc} from "src/mongo/db/modules/enterprise/jstests/streams/utils.js";

function generateInput(size, msPerDocument = 1) {
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
    constructor(
        input, middlePipeline, interval = 0, sourceType = "kafka", useNewCheckpointing = false) {
        this.sourceType = sourceType;
        this.input = input;
        this.uri = 'mongodb://' + db.getMongo().host;
        this.kafkaConnectionName = "kafka1";
        this.kafkaBootstrapServers = "localhost:9092";
        this.kafkaIsTest = true;
        this.kafkaTopic = "topic1";
        this.dbConnectionName = "db1";
        this.dbName = "test";
        this.dlqCollName = uuidStr();
        this.inputCollName = uuidStr();
        this.outputCollName = uuidStr();
        this.processorId = uuidStr();
        this.tenantId = uuidStr();
        this.outputColl = db.getSiblingDB(this.dbName)[this.outputCollName];
        this.inputColl = db.getSiblingDB(this.dbName)[this.inputCollName];
        this.dlqColl = db.getSiblingDB(this.dbName)[this.dlqCollName];
        this.spName = uuidStr();
        this.checkpointCollName = uuidStr();
        this.checkpointColl = db.getSiblingDB(this.dbName)[this.checkpointCollName];
        this.checkpointIntervalMs = null;  // Use the default.

        this.useNewCheckpointing = useNewCheckpointing;
        this.writeDir = "/tmp/checkpoint";
        this.restoreDir = "/tmp/checkpoint";
        this.spWriteDir = `${this.writeDir}/${this.tenantId}/${this.processorId}`;
        this.spRestoreDir = `${this.restoreDir}/${this.tenantId}/${this.processorId}`;
        mkdir(this.writeDir);
        mkdir(this.restoreDir);

        if (interval !== null) {
            this.checkpointIntervalMs = NumberInt(interval);
        }

        let checkpointOptions = {
            storage: {
                uri: this.uri,
                db: this.dbName,
                coll: this.checkpointCollName,
            },
            debugOnlyIntervalMs: this.checkpointIntervalMs,
        };
        if (this.useNewCheckpointing) {
            checkpointOptions.storage = null;
            checkpointOptions.localDisk = {writeDirectory: this.writeDir};
        }

        this.startOptions = {
            dlq: {connectionName: this.dbConnectionName, db: this.dbName, coll: this.dlqCollName},
            checkpointOptions: checkpointOptions
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

        // Setup the pipeline.
        this.pipeline = [];
        // First, append either a kafka or changestream source.
        if (this.sourceType === 'kafka') {
            this.pipeline.push({
                $source: {
                    connectionName: this.kafkaConnectionName,
                    topic: this.kafkaTopic,
                    testOnlyPartitionCount: NumberInt(1),
                    timeField: {
                        $toDate: "$ts",
                    }
                }
            });
        } else {
            this.pipeline.push({
                $source: {
                    connectionName: this.dbConnectionName,
                    db: this.dbName,
                    coll: this.inputCollName,
                    timeField: {
                        $toDate: "$fullDocument.ts",
                    }
                }
            });
        }
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
        if (this.useNewCheckpointing) {
            this.sp.setUseUnnestedWindow(true);
        }
    }

    // Helper functions.
    run(firstStart = true) {
        if (this.useNewCheckpointing) {
            let idsOnDisk = this.getCheckpointIds();
            if (idsOnDisk.length > 0) {
                this.startOptions.checkpointOptions.localDisk.restoreDirectory =
                    `${this.restoreDir}/${this.tenantId}/${this.processorId}/${idsOnDisk[0]}`;
            } else {
                this.startOptions.checkpointOptions.localDisk.restoreDirectory = null;
            }
        }

        this.sp.createStreamProcessor(this.spName, this.pipeline);
        jsTestLog(`Starting ${this.spName} with options ${tojson(this.startOptions)}`);
        this.sp[this.spName].start(this.startOptions, this.processorId, this.tenantId);
        if (this.sourceType === 'kafka') {
            // Insert the input.
            assert.commandWorked(db.runCommand({
                streams_testOnlyInsert: '',
                name: this.spName,
                documents: this.input,
            }));
        } else if (firstStart == true) {
            // For a changestream source, we only insert data into the colletion
            // on the first start. Subsequent attempts replay from the changestream/oplog.
            assert.commandWorked(this.inputColl.insertMany(this.input));
        }
    }

    stop(assertWorked = true) {
        this.sp[this.spName].stop(assertWorked);
    }

    stats() {
        return this.sp[this.spName].stats();
    }

    // returns the commit checkpoint IDs
    // the 0-th index of the returned array is the most recently committed checkpoint
    getCheckpointIds() {
        if (this.useNewCheckpointing) {
            if (!pathExists(this.spWriteDir)) {
                return [];
            }
            let ids = listFiles(this.spWriteDir)
                          // Get the committed checkpoint directories by looking for manifest files.
                          .filter((checkpointDir) => {
                              let manifestFile = listFiles(checkpointDir.name)
                                                     .find(file => file.name.includes("MANIFEST"));
                              return manifestFile !== null;
                          })
                          // Retrieve the checkpointID from each directory.
                          .map(checkpointDir => checkpointDir.name.split('/').pop());
            // Sort by checkpoint ID.
            ids.sort();
            // Reverse the array so the most recently committed checkpoint ID is at index 0.
            return ids.reverse();
        } else {
            return this.checkpointColl.find({_id: {$regex: "^checkpoint"}})
                .sort({_id: -1})
                .toArray()
                .map(i => i._id);
        }
    }

    getResults() {
        return this.outputColl.find({}).sort({idx: 1}).toArray();
    }

    getStartOffsetFromCheckpoint(checkpointId) {
        assert(this.sourceType === 'kafka', "only valid to call for kafka source type");
        jsTestLog(`Getting start offset from ${checkpointId}`);
        let stateObject = null;
        if (this.useNewCheckpointing) {
            const log = assert.commandWorked(db.adminCommand({getLog: "global"}));
            // TODO(SERVER-81440): We should find a better way to determine the start offset.
            // One option is to expose it in (verbose) stats.
            // Log lines are unreliable because they get truncated.
            for (const line of findMatchingLogLines(log.log, {id: 77177})) {
                let entry = JSON.parse(line);
                if (entry.attr.checkpointId == checkpointId) {
                    stateObject = entry.attr.state;
                    jsTestLog(stateObject);
                }
            }
            assert(stateObject != null, "didn't find expected state object in the logs");
        } else {
            let sourceState =
                this.checkpointColl
                    .find({
                        _id: {
                            $regex: `^operator/${checkpointId.replace("checkpoint/", "")}/00000000`
                        }
                    })
                    .sort({_id: -1})
                    .toArray();
            stateObject = sourceState[0]["state"];
        }
        let startOffset = stateObject["partitions"][0]["offset"];
        jsTestLog(`Start offset for ${checkpointId} is ${startOffset}`);
        return startOffset;
    }

    getSourceState(checkpointId) {
        let sourceState =
            this.checkpointColl
                .find({
                    _id: {$regex: `^operator/${checkpointId.replace("checkpoint/", "")}/00000000`}
                })
                .sort({_id: -1})
                .toArray();
        assert.eq(1, sourceState.length, "expected only 1 state document for $source");
        return sourceState[0];
    }

    getStartingPointFromCheckpoint(checkpointId) {
        if (this.sourceType != 'changestream') {
            throw 'only supported with changestream $source';
        }

        if (this.useNewCheckpointing) {
            const log = assert.commandWorked(db.adminCommand({getLog: "global"}));
            let startingPoint = null;
            // TODO(SERVER-81440): We should find a better way to determine the start offset.
            // One option is to expose it in (verbose) stats.
            // Log lines are unreliable because they get truncated.
            for (const line of findMatchingLogLines(log.log, {id: 7788506})) {
                let entry = JSON.parse(line);
                if (entry.attr.checkpointId == checkpointId) {
                    startingPoint = JSON.parse(entry.attr.state)["startingPoint"];
                }
            }
            assert(startingPoint != null, "didn't find expected state object in the logs");
            if (startingPoint.hasOwnProperty("$timestamp")) {
                let ts = startingPoint["$timestamp"];
                return {resumeToken: null, startAtOperationTime: new Timestamp(ts["t"], ts["i"])};
            } else {
                return {resumeToken: startingPoint, startAtOperationTime: null};
            }
        } else {
            // This is a variant<object, Timestamp> corresponding to a resumeToken or
            // timestamp.
            let startingPoint = this.getSourceState(checkpointId)["state"]["startingPoint"];
            if (startingPoint instanceof Timestamp) {
                return {
                    resumeToken: null,
                    startAtOperationTime: startingPoint,
                };
            } else {
                return {resumeToken: startingPoint, startAtOperationTime: null};
            }
        }
    }

    getNextEvent(resumeAfterToken) {
        if (this.sourceType != 'changestream') {
            throw 'only supported with changestream $source';
        }

        const stream = this.inputColl.watch([], {resumeAfter: resumeAfterToken});
        if (stream.hasNext()) {
            return stream.next();
        } else {
            return null;
        }
    }

    errStr() {
        return `checkpointCollName: ${this.checkpointCollName}`;
    }

    removeCheckpointsNotInList(ids) {
        if (this.useNewCheckpointing) {
            let idsOnDisk = listFiles(this.spWriteDir).map(f => f.name.split('/').pop());
            for (let idOnDisk of idsOnDisk) {
                if (!ids.includes(idOnDisk)) {
                    let path = `${this.spWriteDir}/${idOnDisk}`;
                    removeFile(path);
                }
            }
        } else {
            // This deletes the id we just verified, and any new
            // checkpoints this replay created.
            this.checkpointColl.deleteMany({_id: {$nin: ids.map(id => id), $not: /^operator/}});
        }
    }
}

function testBoth(useNewCheckpointing) {
    function smokeTestCorrectness() {
        const input = generateInput(2000);
        let test =
            new TestHelper(input, [], /* checkpointInterval */ 0, "kafka", useNewCheckpointing);

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
        let offsets = ids.map(id => test.getStartOffsetFromCheckpoint(id));
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
            const startingOffset = offsets.shift();
            const expectedOutputCount = input.length - startingOffset;
            jsTestLog("Running");
            jsTestLog(
                {id: id, startingOffset: startingOffset, expectedOutputCount: expectedOutputCount});

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

            test.removeCheckpointsNotInList(ids);
        }
        assert.gt(replaysWithData, 0);
    }

    function smokeTestCorrectnessTumblingWindow() {
        const inputSize = 5000;
        const msPerDoc = 1;
        const windowInterval = {size: NumberInt(1), unit: "second"};
        const windowSizeMs = 1000;
        const allowedLatenessInteval = {size: NumberInt(0), unit: "second"};
        const docsPerWindow = windowSizeMs / msPerDoc;
        const input = generateInput(inputSize + 2, msPerDoc);
        const expectedTotalOutput = [
            {
                "_id": {"min": 0, "max": 999, "sum": 1000},
                "_stream_meta": {
                    "sourceType": "kafka",
                    "windowStartTimestamp": ISODate("2023-01-01T00:00:00Z"),
                    "windowEndTimestamp": ISODate("2023-01-01T00:00:01Z")
                }
            },
            {
                "_id": {"min": 1000, "max": 1999, "sum": 1000},
                "_stream_meta": {
                    "sourceType": "kafka",
                    "windowStartTimestamp": ISODate("2023-01-01T00:00:01Z"),
                    "windowEndTimestamp": ISODate("2023-01-01T00:00:02Z")
                }
            },
            {
                "_id": {"min": 2000, "max": 2999, "sum": 1000},
                "_stream_meta": {
                    "sourceType": "kafka",
                    "windowStartTimestamp": ISODate("2023-01-01T00:00:02Z"),
                    "windowEndTimestamp": ISODate("2023-01-01T00:00:03Z")
                }
            },
            {
                "_id": {"min": 3000, "max": 3999, "sum": 1000},
                "_stream_meta": {
                    "sourceType": "kafka",
                    "windowStartTimestamp": ISODate("2023-01-01T00:00:03Z"),
                    "windowEndTimestamp": ISODate("2023-01-01T00:00:04Z")
                }
            },
            {
                "_id": {"min": 4000, "max": 4999, "sum": 1000},
                "_stream_meta": {
                    "sourceType": "kafka",
                    "windowStartTimestamp": ISODate("2023-01-01T00:00:04Z"),
                    "windowEndTimestamp": ISODate("2023-01-01T00:00:05Z")
                }
            },
        ];
        const pipeline = [
            {
                $tumblingWindow: {
                    interval: windowInterval,
                    allowedLateness: allowedLatenessInteval,
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
            {
                $project: {
                    _id: {
                        min: "$minIdx",
                        max: "$maxIdx",
                        sum: "$sum",
                    }
                }
            }
        ];
        const test = new TestHelper(
            input, pipeline, 0 /* interval */, "kafka" /* sourceType */, useNewCheckpointing);

        let getSortedResults =
            () => { return test.outputColl.find({}).sort({"_id.min": 1}).toArray(); };

        test.run();
        waitForCount(test.outputColl, expectedTotalOutput.length, 60);
        test.stop();
        let originalResults = getSortedResults();
        assert.eq(expectedTotalOutput.length, originalResults.length);
        for (let i = 0; i < expectedTotalOutput.length; i += 1) {
            originalResults[i] = expectedTotalOutput[i];
        }
        let ids = test.getCheckpointIds();
        let offsets = ids.map(id => test.getStartOffsetFromCheckpoint(id));
        assert.gt(ids.length, 0, `expected some checkpoints`);

        // Replay from each written checkpoint and verify the results are expected.
        let replaysWithData = 0;
        while (ids.length > 0) {
            const id = ids.shift();
            const startingOffset = offsets.shift();
            const expectedInputCount = input.length - startingOffset;
            const expectedOutputCount = useNewCheckpointing
                ? Math.min(Math.ceil(expectedInputCount / docsPerWindow),
                           expectedTotalOutput.length)
                : Math.floor(expectedInputCount / docsPerWindow);
            const replay = {
                id: id,
                startingOffset: startingOffset,
                expectedInputCount: expectedInputCount,
                expectedOutputCount: expectedOutputCount
            };

            // Clean the output.
            test.outputColl.deleteMany({});
            // Run the streamProcessor.
            test.run();
            if (replay.expectedOutputCount > 0) {
                // Wait for the expected output to arrive.
                waitForCount(test.outputColl, replay.expectedOutputCount);
                replaysWithData++;
            } else {
                // Else, just sleep 5 seconds, we will verify nothing is output after this.
                sleep(1000 * 5);
            }
            test.stop();

            // Verify the results.
            const results = getSortedResults();
            assert.eq(replay.expectedOutputCount,
                      results.length,
                      `Unexpected result\n${tojson(replay)}\n${tojson(results)}`);
            for (let i = 0; i < results.length; i++) {
                const originalResult = expectedTotalOutput[expectedTotalOutput.length - 1 - i];
                const restoreResult = results[results.length - 1 - i];
                assert.eq(originalResult, restoreResult);
            }
            test.removeCheckpointsNotInList(ids);
        }
        assert.gt(replaysWithData, 0);
    }

    function smokeTestStopStartWindow() {
        const startTS = ISODate("2023-12-01T00:00:00.000Z");
        const endTs = ISODate("2023-12-01T01:00:00.000Z");
        const windowInterval = {size: NumberInt(1), unit: "hour"};
        const allowedLatenessInteval = {size: NumberInt(0), unit: "second"};
        const pipeline = [{
            $tumblingWindow: {
                interval: windowInterval,
                allowedLateness: allowedLatenessInteval,
                pipeline: [{
                    $group: {
                        _id: "$fullDocument.customerId",
                        sum: {$sum: "$fullDocument.a"},
                        min: {$min: "$fullDocument.a"},
                        max: {$max: "$fullDocument.a"}
                    }
                }]
            }
        }];
        const input = [{ts: startTS, customerId: 0, a: 1}, {ts: startTS, customerId: 1, a: 2}];
        const expectedOutputDocs = [
            {
                "_id": 0,
                "_stream_meta": {
                    "sourceType": "atlas",
                    "windowStartTimestamp": ISODate("2023-12-01T00:00:00Z"),
                    "windowEndTimestamp": ISODate("2023-12-01T01:00:00Z")
                },
                "max": 1,
                "min": 1,
                "sum": 1
            },
            {
                "_id": 1,
                "_stream_meta": {
                    "sourceType": "atlas",
                    "windowStartTimestamp": ISODate("2023-12-01T00:00:00Z"),
                    "windowEndTimestamp": ISODate("2023-12-01T01:00:00Z")
                },
                "max": 2,
                "min": 2,
                "sum": 2
            },
        ];

        let test = new TestHelper(input,
                                  pipeline,
                                  0 /* interval */,
                                  "changestream" /* sourceType */,
                                  useNewCheckpointing);
        test.run();
        // Wait for all the messages to be read.
        assert.soon(() => { return test.stats()["inputMessageCount"] == input.length; });
        assert.eq(0, test.getResults().length, "expected no output");
        test.stop();
        assert.eq(0, test.getResults().length, "expected no output");

        let ids = test.getCheckpointIds();
        assert.gt(ids.length, 0, `expected some checkpoints`);
        // Run the streamProcessor, expecting to resume from a checkpoint.
        test.run(false /* firstStart */);
        // Insert an event that will close the window.
        assert.commandWorked(test.inputColl.insert({ts: new Date(endTs.getTime() + 1)}));
        assert.soon(() => { return test.getResults().length == expectedOutputDocs.length; });
        // Verify the window emits the expected results after restore.
        let results = test.getResults();
        results.sort((a, b) => a._id - b._id);
        assert.eq(expectedOutputDocs.length, results.length);
        for (let i = 0; i < results.length; i++) {
            assert.eq(expectedOutputDocs[i], results[i]);
        }
        test.stop();
    }

    function innerStopStartHoppingWindowTest(
        {pipeline, inputBeforeStop, inputAfterStop, expectedOutput, resultsSortDoc = null}) {
        let test = new TestHelper(inputBeforeStop,
                                  pipeline,
                                  0 /* interval */,
                                  "changestream" /* sourceType */,
                                  useNewCheckpointing);
        const getResults = () => {
            let query = test.outputColl.find({});
            if (resultsSortDoc != null) {
                query = query.sort(resultsSortDoc);
            }
            return query.toArray();
        };

        test.run();
        // Wait for all the messages to be read.
        assert.soon(() => { return test.stats()["inputMessageCount"] == inputBeforeStop.length; });
        assert.eq(0, getResults().length, "expected no output");
        test.stop();
        assert.eq(0, getResults().length, "expected no output");

        let ids = test.getCheckpointIds();
        assert.gt(ids.length, 0, `expected some checkpoints`);
        // Run the streamProcessor, expecting to resume from a checkpoint.
        test.run(false /* firstStart */);
        assert.commandWorked(test.inputColl.insertMany(inputAfterStop));
        assert.neq(expectedOutput, null);
        assert.soon(() => {
            let results = getResults();
            if (results.length == expectedOutput.length) {
                return true;
            }
            jsTestLog(results);
            return false;
        });

        // Verify the window emits the expected results after restore.
        let results = getResults();
        assert.eq(expectedOutput.length, results.length);
        for (let i = 0; i < results.length; i++) {
            delete results[i]._id;
            assert.eq(expectedOutput[i], results[i]);
        }
        test.stop();
    }

    function hoppingWindowSortTest() {
        const pipeline = [
            {$replaceRoot: {newRoot: "$fullDocument"}},
            {$project: {_id: 0}},
            {
                $hoppingWindow: {
                    interval: {size: NumberInt(1), unit: "hour"},
                    hopSize: {size: NumberInt(20), unit: "minute"},
                    allowedLateness: {size: NumberInt(0), unit: "second"},
                    pipeline: [
                        {$sort: {"customerId": 1, "a": 1}},
                        {$group: {_id: null, results: {$push: "$$ROOT"}}}
                    ]
                }
            },
            {$project: {_id: 0, results: 1}}
        ];
        const docTs = ISODate("2023-12-01T01:00:00.000Z");
        const input = [
            {ts: docTs, customerId: 1, a: 4},
            {ts: docTs, customerId: 0, a: 1},
            {ts: docTs, customerId: 1, a: 2},
            {ts: docTs, customerId: 0, a: 10},
        ];
        const expectedResultsArray = [
            {ts: docTs, customerId: 0, a: 1},
            {ts: docTs, customerId: 0, a: 10},
            {ts: docTs, customerId: 1, a: 2},
            {ts: docTs, customerId: 1, a: 4},
        ];
        let expectedOutputDocs = [
            {
                "_stream_meta": {
                    "sourceType": "atlas",
                    "windowStartTimestamp": ISODate("2023-12-01T00:20:00Z"),
                    "windowEndTimestamp": ISODate("2023-12-01T01:20:00Z")
                },
                results: expectedResultsArray
            },
            {
                "_stream_meta": {
                    "sourceType": "atlas",
                    "windowStartTimestamp": ISODate("2023-12-01T00:40:00Z"),
                    "windowEndTimestamp": ISODate("2023-12-01T01:40:00Z")
                },
                results: expectedResultsArray
            },
            {
                "_stream_meta": {
                    "sourceType": "atlas",
                    "windowStartTimestamp": ISODate("2023-12-01T01:00:00Z"),
                    "windowEndTimestamp": ISODate("2023-12-01T02:00:00Z")
                },
                results: expectedResultsArray
            },
        ];
        innerStopStartHoppingWindowTest({
            pipeline: pipeline,
            inputBeforeStop: input,
            inputAfterStop: [{ts: ISODate("2023-12-01T02:00:00.001Z")}],
            expectedOutput: expectedOutputDocs,
            resultsSortDoc: {
                "_stream_meta.windowStartTimestamp": 1,
            }
        });
    }

    function hoppingWindowGroupTest() {
        innerStopStartHoppingWindowTest({
            pipeline: [
                {
                    $hoppingWindow: {
                        interval: {size: NumberInt(1), unit: "hour"},
                        hopSize: {size: NumberInt(20), unit: "minute"},
                        allowedLateness: {size: NumberInt(0), unit: "second"},
                        pipeline: [{
                            $group: {
                                _id: "$fullDocument.customerId",
                                sum: {$sum: "$fullDocument.a"},
                                min: {$min: "$fullDocument.a"},
                                max: {$max: "$fullDocument.a"}
                            }
                        }]
                    }
                },
                {$project: {customerId: "$_id", max: 1, min: 1, sum: 1, _id: 0}}
            ],
            inputBeforeStop: [
                {ts: ISODate("2023-12-01T01:00:00.000Z"), customerId: 0, a: 1},
                {ts: ISODate("2023-12-01T01:00:00.000Z"), customerId: 0, a: 10},
                {ts: ISODate("2023-12-01T01:00:00.000Z"), customerId: 1, a: 2},
                {ts: ISODate("2023-12-01T01:00:00.000Z"), customerId: 1, a: 4}
            ],
            inputAfterStop: [{ts: ISODate("2023-12-01T02:00:00.001Z")}],
            expectedOutput: [
                {
                    "_stream_meta": {
                        "sourceType": "atlas",
                        "windowStartTimestamp": ISODate("2023-12-01T00:20:00Z"),
                        "windowEndTimestamp": ISODate("2023-12-01T01:20:00Z")
                    },
                    "customerId": 0,
                    "max": 10,
                    "min": 1,
                    "sum": 11
                },
                {
                    "_stream_meta": {
                        "sourceType": "atlas",
                        "windowStartTimestamp": ISODate("2023-12-01T00:20:00Z"),
                        "windowEndTimestamp": ISODate("2023-12-01T01:20:00Z")
                    },
                    "customerId": 1,
                    "max": 4,
                    "min": 2,
                    "sum": 6
                },
                {
                    "_stream_meta": {
                        "sourceType": "atlas",
                        "windowStartTimestamp": ISODate("2023-12-01T00:40:00Z"),
                        "windowEndTimestamp": ISODate("2023-12-01T01:40:00Z")
                    },
                    "customerId": 0,
                    "max": 10,
                    "min": 1,
                    "sum": 11
                },
                {
                    "_stream_meta": {
                        "sourceType": "atlas",
                        "windowStartTimestamp": ISODate("2023-12-01T00:40:00Z"),
                        "windowEndTimestamp": ISODate("2023-12-01T01:40:00Z")
                    },
                    "customerId": 1,
                    "max": 4,
                    "min": 2,
                    "sum": 6
                },
                {
                    "_stream_meta": {
                        "sourceType": "atlas",
                        "windowStartTimestamp": ISODate("2023-12-01T01:00:00Z"),
                        "windowEndTimestamp": ISODate("2023-12-01T02:00:00Z")
                    },
                    "customerId": 0,
                    "max": 10,
                    "min": 1,
                    "sum": 11
                },
                {
                    "_stream_meta": {
                        "sourceType": "atlas",
                        "windowStartTimestamp": ISODate("2023-12-01T01:00:00Z"),
                        "windowEndTimestamp": ISODate("2023-12-01T02:00:00Z")
                    },
                    "customerId": 1,
                    "max": 4,
                    "min": 2,
                    "sum": 6
                },
            ],
            resultsSortDoc: {"_stream_meta.windowStartTimestamp": 1, "customerId": 1}
        });
    }

    // TODO(SERVER-83688): Finish this test.
    function hoppingWindowLargeGroupTest() {
        // 0,1,...,100
        let unwindArraySize = 100;
        let unwindArray = Array.from({length: unwindArraySize + 1}, (_, index) => index);
        let docsPerBatch = 100;
        // Will belong in 180 hopping windows.
        let ts = ISODate("2023-12-01T01:00:00.000Z");

        const pipeline = [
            {$replaceRoot: {newRoot: "$fullDocument"}},
            {$unwind: "$unwindArray"},
            {
                $hoppingWindow: {
                    interval: {size: NumberInt(3), unit: "hour"},
                    hopSize: {size: NumberInt(1), unit: "minute"},
                    allowedLateness: {size: NumberInt(0), unit: "second"},
                    pipeline: [{$group: {_id: "$customerId", matches: {$push: "$$ROOT"}}}]
                }
            },
            {
                $project: {
                    customerId: "$_id",
                    matches: 1,
                    _id: 0,
                }
            }
        ];

        const inputAfterStop = [{ts: ISODate("2023-12-01T02:00:00.001Z")}];

        let test = new TestHelper([],
                                  pipeline,
                                  999999999 /* interval */,
                                  "changestream" /* sourceType */,
                                  useNewCheckpointing);
        let expectedKeys = 0;

        test.run();
        // Wait for all the messages to be read.

        const numMB = 20;
        const stateSizeBytes = numMB * 1000000;
        let stateSize = test.stats()["operatorStats"][3]["stateSize"];
        while (stateSize < stateSizeBytes) {
            jsTestLog("Stats: " + tojson(test.stats()));
            jsTestLog("Stats: " + tojson(test.stats()["operatorStats"][3]));
            let batch = Array.from({length: docsPerBatch + 1}, (_, index) => {
                return {
                    ts: ts,
                    customerId: uuidStr(),
                    comment: uuidStr(),
                    unwindArray: unwindArray
                };
            });
            test.inputColl.insertMany(batch);
            expectedKeys += batch.length;
            stateSize = test.stats()["operatorStats"][3]["stateSize"];
        }
        jsTestLog("stopped");
        test.stop();

        // let ids = test.getCheckpointIds();
        // assert.gt(ids.length, 0, `expected some checkpoints`);
        // // Run the streamProcessor, expecting to resume from a checkpoint.
        // test.run(false /* firstStart */);
        // assert.commandWorked(test.inputColl.insertMany(inputAfterStop));
        // assert.neq(expectedOutput, null);
        // assert.soon(() => {
        //     return test.outputColl.find({}).count() > 0
        // });
        // test.stop();
    }

    function smokeTestCheckpointOnStop() {
        // Use a large input so it is not finished by the time we call stop.
        const input = generateInput(330000);
        // Use a long checkpoint interval so they don't automatically happen.
        let test = new TestHelper(
            input, [], 1000000000 /* interval */, "kafka" /* sourceType */, useNewCheckpointing);
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
        // There should be 1 checkpoint for the start of a fresh streamProcessor,
        // 1 checkpoint from the stop.
        assert.eq(2, ids.length);
    }

    function smokeTestCorrectnessChangestream() {
        const input = generateInput(503);
        let test = new TestHelper(input, [], 0, 'changestream', useNewCheckpointing);

        test.run();
        // Wait until the last doc in the input appears in the output collection.
        waitForCount(test.outputColl, input.length, /* maxWaitSeconds */ 60);
        test.stop();
        // Verify the output matches the input.
        let results = test.getResults();
        assert.eq(results.length, input.length);
        for (let i = 0; i < results.length; i++) {
            assert.eq(input.length[i], removeProjections(results[i]));
        }

        // Get the checkpoint IDs from the run.
        let ids = test.getCheckpointIds();
        assert.gt(ids.length, 0);
        jsTestLog(ids);
        let offsets = ids.map(id => test.getStartingPointFromCheckpoint(id));

        // Replay from each written checkpoint and verify the results are expected.
        let firstExtraDocIdx = input.length;
        let extraDocsInserted = 0;
        let newIdx = firstExtraDocIdx;
        let totalReplays = 0;
        let replaysWithData = 0;
        let replaysWithZeroData = 0;
        let replaysFromResumeToken = 0;
        while (ids.length > 0) {
            const id = ids.shift();
            let expectedMinIdx;
            let startingPoint = offsets.shift();
            assert.neq(null, startingPoint);
            if (startingPoint.resumeToken != null) {
                jsTestLog(`Starting from ${tojson(startingPoint)}`);
                let expectedFirstEvent = test.getNextEvent(startingPoint.resumeToken);
                expectedMinIdx = expectedFirstEvent == null ? firstExtraDocIdx
                                                            : expectedFirstEvent.fullDocument.idx;
                replaysFromResumeToken += 1;
            } else {
                assert.neq(null, startingPoint.startAtOperationTime);
                expectedMinIdx = 0;
            }
            let details = {
                checkpointId: id,
                resumeToken: startingPoint.resumeToken,
                testUtils: test.errStr(),
                ids: ids,
                sourceState: startingPoint,
                expectedMinIdx: expectedMinIdx,
            };
            jsTestLog(`Run ${tojson(details)}`);

            // Clean the output.
            assert.commandWorked(test.outputColl.deleteMany({}));
            // Run the streamProcessor.
            test.run(false);
            // Insert a new doc and wait for it to show up.
            assert.commandWorked(test.inputColl.insert({ts: new Date(), idx: newIdx}));
            extraDocsInserted += 1;
            waitForDoc(
                test.outputColl, (doc) => doc.fullDocument.idx == newIdx, /* maxWaitSeconds */ 60);
            test.stop();
            // Verify the results.
            let results = test.getResults();
            let originalDocsReplayed = results.length - extraDocsInserted;
            jsTestLog(`Origial input replayed: ${originalDocsReplayed}`);
            if (originalDocsReplayed == 0) {
                replaysWithZeroData += 1;
            } else {
                replaysWithData += 1;
            }

            let expectedMaxIdx = newIdx;
            // Get the actual min/max idx values in the output.
            let min = null;
            for (let doc of results) {
                if (min === null || doc.fullDocument.idx < min) {
                    min = doc.fullDocument.idx;
                }
            }
            let max = null;
            for (let doc of results) {
                if (max === null || doc.fullDocument.idx > max) {
                    max = doc.fullDocument.idx;
                }
            }
            assert.eq(expectedMinIdx, min, details);
            assert.eq(expectedMaxIdx, max, details);

            // This deletes the id we just verified, and any new
            // checkpoints this replay created.
            test.removeCheckpointsNotInList(ids);
            newIdx += 1;
            totalReplays += 1;
        }
        assert.gt(replaysWithZeroData, 0);
        assert.gt(replaysWithData, 0);
        assert.gt(replaysFromResumeToken, 0);
    }

    function failPointTestAfterFirstOutput() {
        try {
            assert.commandWorked(db.adminCommand(
                {'configureFailPoint': 'failAfterRemoteInsertSucceeds', 'mode': 'alwaysOn'}));
            const input = generateInput(200);
            let test = new TestHelper(
                input, [], 99999999 /* long interval */, 'changestream', useNewCheckpointing);
            test.sp.createStreamProcessor(test.spName, test.pipeline);
            assert.commandWorked(
                test.sp[test.spName].start(test.startOptions, test.processorId, test.tenantId));
            // The streamProcessor will crash after the first document is output.
            assert.commandWorked(test.inputColl.insertMany(test.input));
            // Wait for one checkpoint to be committed.
            let checkpointIds = [];
            assert.soon(() => {
                checkpointIds = test.getCheckpointIds();
                return checkpointIds.length == 1;
            });
            let checkpointId = test.getCheckpointIds()[0];
            // Verify the first checkpoint for a changestream $source uses a Timestamp.
            let startingPoint =
                test.getStartingPointFromCheckpoint(checkpointId).startAtOperationTime;
            assert(startingPoint instanceof Timestamp);
            // Sleep for a while and verify the first document in the input is in the output
            // collection.
            sleep(5000);
            assert.eq(input[0].idx,
                      test.outputColl.find({"fullDocument.idx": input[0].idx})
                          .toArray()[0]
                          .fullDocument.idx);
            // Stop the streamProcessor that failed. We don't assert success
            // here, there is a small chance the StreamManager has already cleaned this erroring
            // streamProcessor up.
            test.stop(false /* assertWorked */);
            // Clean the output.
            assert.commandWorked(test.outputColl.deleteMany({}));
            assert.commandWorked(test.inputColl.insert({ts: new Date(), idx: input.length}));
            // This will start the streamProcessor ~5 seconds after the inserts
            // above. But since we should be resuming from a checkpoint, this streamProcessor
            // should still pick up the very first inserted event.
            test.run(false);
            assert.commandWorked(test.inputColl.insert({ts: new Date(), idx: input.length + 1}));
            waitForCount(test.outputColl, /* count */ 1);
            assert.eq(input[0].idx,
                      test.outputColl.find({"fullDocument.idx": input[0].idx})
                          .toArray()[0]
                          .fullDocument.idx);
            // TODO(SERVER-79801): Remove this sleep once the deadlock is fixed.
            sleep(3000);
            test.stop(false /* assertWorked */);
        } finally {
            assert.commandWorked(db.adminCommand(
                {'configureFailPoint': 'failAfterRemoteInsertSucceeds', 'mode': 'off'}));
        }
    }

    function smokeTestStatsInCheckpoint() {
        const input = generateInput(503);
        let test = new TestHelper(input,
                                  [] /* pipeline */,
                                  null /* default interval */,
                                  'changestream',
                                  useNewCheckpointing);
        test.run();
        // Wait until the last doc in the input appears in the output collection.
        waitForCount(test.outputColl, input.length, /* maxWaitSeconds */ 60);
        assert.soon(() => input.length == test.sp[test.spName].stats(false).inputMessageCount);
        // Stop the streamProcessor, which will write a checkpoint.
        test.stop();
        // Restart the stream processor.
        test.run(false);
        let stats = test.sp[test.spName].stats(false);
        assert.eq(input.length, stats.inputMessageCount);
        assert.eq(input.length, stats.outputMessageCount);
        // Re-insert the input.
        assert.commandWorked(test.inputColl.insertMany(input));
        // Verify the stats are updated.
        waitForCount(test.outputColl, input.length * 2, /* maxWaitSeconds */ 60);
        assert.soon(() => input.length * 2 == test.sp[test.spName].stats(false).inputMessageCount);
        assert.soon(() => input.length * 2 == test.sp[test.spName].stats(false).outputMessageCount);
        // Restart the stream processor.
        test.stop();
        test.run(false);
        // Verify the stats were persisted.
        stats = test.sp[test.spName].stats(false);
        assert.eq(input.length * 2, stats.inputMessageCount);
        assert.eq(input.length * 2, stats.outputMessageCount);
        test.stop();
    }

    function smokeTestEmptyChangestream() {
        // In SERVER-80668 we found a bug where an changestream $source with zero events after start
        // would cause stop to hang with checkpointing enabled. This test covers that scenario.
        let test = new TestHelper([] /* empty input */,
                                  [] /* pipeline */,
                                  null /* default interval */,
                                  'changestream',
                                  useNewCheckpointing);
        test.run();
        test.stop();
        test.run(false);
        test.stop();
    }

    // Validates that the resume token advances for a changestream $source,
    // even when the collection changestream is empty.
    function emptyChangestreamResumeTokenAdvances() {
        let test = new TestHelper(
            [] /* empty input */, [] /* pipeline */, 0, 'changestream', useNewCheckpointing);
        test.run();
        // Slowly insert events into another collection on the same database so there will
        // be new resume tokens to advance to.
        let otherColl = db.getSiblingDB(test.dbName)["otherCollection"];
        let waitForNumCheckpoints = 5;
        let i = 0;
        otherColl.insert({a: i++});
        while (test.getCheckpointIds().length < waitForNumCheckpoints) {
            otherColl.insert({a: i++});
            sleep(100);
        }
        test.stop();

        let ids = test.getCheckpointIds();
        assert.gte(ids.length, waitForNumCheckpoints);
        // Verify we have at least 2 unique resume tokens in our checkpoints.
        let resumeTokens = ids.map(id => test.getStartingPointFromCheckpoint(id))
                               .filter(startingPoint => startingPoint.resumeToken != null);
        assert.gte(new Set(resumeTokens).size, 2);
    }

    smokeTestCorrectness();
    smokeTestCorrectnessTumblingWindow();
    smokeTestStopStartWindow();
    hoppingWindowGroupTest();
    hoppingWindowSortTest();
    smokeTestCheckpointOnStop();
    smokeTestCorrectnessChangestream();
    failPointTestAfterFirstOutput();
    smokeTestStatsInCheckpoint();
    smokeTestEmptyChangestream();
    emptyChangestreamResumeTokenAdvances();
    // TODO(SERVER-83688): Finish this test.
    // hoppingWindowLargeGroupTest();
}

testBoth(true /* useNewCheckpointing */);
testBoth(false /* useNewCheckpointing */);