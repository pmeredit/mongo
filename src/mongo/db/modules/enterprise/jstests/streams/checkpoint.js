/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */

import {
    removeProjections,
    TestHelper,
} from "src/mongo/db/modules/enterprise/jstests/streams/checkpoint_helper.js";
import {
    listStreamProcessors,
    makeRandomString,
    sanitizeDoc,
    verifyDocsEqual,
    waitForCount,
    waitForDoc,
} from "src/mongo/db/modules/enterprise/jstests/streams/utils.js";

import {} from "src/mongo/db/modules/enterprise/jstests/streams/utils.js";

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

function testBoth(useNewCheckpointing, useRestoredExecutionPlan) {
    function smokeTestCorrectness() {
        const input = generateInput(2000);
        let test = new TestHelper(input,
                                  [],
                                  /* checkpointInterval */ 0,
                                  "kafka",
                                  useNewCheckpointing,
                                  useRestoredExecutionPlan);

        // Run the streamProcessor for the first time.
        test.run();
        // Wait until the last doc in the input appears in the output collection.
        waitForDoc(test.outputColl, (doc) => doc.idx == input.length - 1, /* maxWaitSeconds */ 60);
        test.stop();
        // Verify the output matches the input.
        let results = test.getResults();
        assert.eq(results.length, input.length);
        for (let i = 0; i < results.length; i++) {
            verifyDocsEqual(input[i], results[i]);
        }
        // Get the checkpoint IDs from the run.
        let ids = test.getCheckpointIds();
        assert.gte(ids.length, 1, `expected at least 1 checkpoint`);

        // Replay from each written checkpoint and verify the results are expected.
        let replaysWithData = 0;
        while (ids.length > 0) {
            const id = ids.shift();

            jsTestLog(`Running restoreId ${id}`);
            // Clean the output.
            test.outputColl.deleteMany({});
            // Run the streamProcessor.
            test.run();
            // Get the starting offset from the checkpoint.
            const startingOffset = test.getStartOffsetFromCheckpoint(id);
            const expectedOutputCount = input.length - startingOffset;
            jsTestLog(
                {id: id, startingOffset: startingOffset, expectedOutputCount: expectedOutputCount});

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
                verifyDocsEqual(input[i + startingOffset], results[i]);
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
                    "source": {
                        "type": "kafka",
                    },
                    "window": {
                        "start": ISODate("2023-01-01T00:00:00Z"),
                        "end": ISODate("2023-01-01T00:00:01Z"),
                    }
                }
            },
            {
                "_id": {"min": 1000, "max": 1999, "sum": 1000},
                "_stream_meta": {
                    "source": {
                        "type": "kafka",
                    },
                    "window": {
                        "start": ISODate("2023-01-01T00:00:01Z"),
                        "end": ISODate("2023-01-01T00:00:02Z"),
                    }
                }
            },
            {
                "_id": {"min": 2000, "max": 2999, "sum": 1000},
                "_stream_meta": {
                    "source": {
                        "type": "kafka",
                    },
                    "window": {
                        "start": ISODate("2023-01-01T00:00:02Z"),
                        "end": ISODate("2023-01-01T00:00:03Z"),
                    }
                }
            },
            {
                "_id": {"min": 3000, "max": 3999, "sum": 1000},
                "_stream_meta": {
                    "source": {
                        "type": "kafka",
                    },
                    "window": {
                        "start": ISODate("2023-01-01T00:00:03Z"),
                        "end": ISODate("2023-01-01T00:00:04Z"),
                    }
                }
            },
            {
                "_id": {"min": 4000, "max": 4999, "sum": 1000},
                "_stream_meta": {
                    "source": {
                        "type": "kafka",
                    },
                    "window": {
                        "start": ISODate("2023-01-01T00:00:04Z"),
                        "end": ISODate("2023-01-01T00:00:05Z"),
                    }
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
        const test = new TestHelper(input,
                                    pipeline,
                                    0 /* interval */,
                                    "kafka" /* sourceType */,
                                    useNewCheckpointing,
                                    useRestoredExecutionPlan);

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
        assert.gt(ids.length, 0, `expected some checkpoints`);

        // Replay from each written checkpoint and verify the results are expected.
        let replaysWithData = 0;
        while (ids.length > 0) {
            const id = ids.shift();
            // Clean the output.
            test.outputColl.deleteMany({});
            // Run the streamProcessor.
            test.run();
            const startingOffset = test.getStartOffsetFromCheckpoint(id);
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
                verifyDocsEqual(originalResult, restoreResult);
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
                    "source": {
                        "type": "atlas",
                    },
                    "window": {
                        "start": ISODate("2023-12-01T00:00:00Z"),
                        "end": ISODate("2023-12-01T01:00:00Z"),
                    }
                },
                "max": 1,
                "min": 1,
                "sum": 1
            },
            {
                "_id": 1,
                "_stream_meta": {
                    "source": {
                        "type": "atlas",
                    },
                    "window": {
                        "start": ISODate("2023-12-01T00:00:00Z"),
                        "end": ISODate("2023-12-01T01:00:00Z"),
                    }
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
                                  useNewCheckpointing,
                                  useRestoredExecutionPlan);
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
            verifyDocsEqual(expectedOutputDocs[i], results[i]);
        }
        test.stop();
    }

    // This test helper function does the following:
    //  1. Starts a stream processor.
    //  2. Inserts the "inputBeforeStop" into the input collection, which the stream processor
    //  reads.
    //  3. Stops the stream processor.
    //  4. Inserts the "inputAfterStop" into the input collection, while the stream processor is not
    //  running.
    //  5. Starts the stream processor again, resuming from the last checkpoint.
    //  6. Verifies the expected output.
    function innerStopStartTest({
        pipeline,
        inputBeforeStop,
        inputAfterStop,
        expectedOutput,
        resultsSortDoc = null,
        minimumExpectedStateSize = 0,
        shouldHeapProfile = false,
        extraLogKeys = null,
        interval = null,
        waitTime = 60 * 1000,
        expectedOuptutAfterFirstStop = [],
        useTimeField = true
    }) {
        // Create a test helper.
        let test = new TestHelper(
            inputBeforeStop,
            pipeline,
            interval,
            "changestream" /* sourceType */,
            useNewCheckpointing,
            useRestoredExecutionPlan,
            null /* writeDir */,
            null /* restoreDir */,
            db /* dbForTest */,
            null /* targetSourceMergeDb */,
            useTimeField,
        );
        // Helper function to get the results in the output collection.
        const getResults = () => {
            let query = test.outputColl.find({});
            if (resultsSortDoc != null) {
                query = query.sort(resultsSortDoc);
            }
            return query.toArray();
        };
        // Helper function to verify the current output.
        const verifyOuptut = (expectedOutput) => {
            const output = getResults();
            assert.eq(expectedOutput.length, output.length);
            for (let i = 0; i < output.length; i++) {
                verifyDocsEqual(
                    expectedOutput[i], output[i], ["_id", "_ts", "_stream_meta"] /*ignoreFields*/);
            }
        };

        // This starts the stream processor and inserts the "inputBeforeStop" into the input
        // collection.
        test.run();
        // Wait for all the messages to be read.
        assert.soon(() => { return test.stats()["inputMessageCount"] == inputBeforeStop.length; },
                    "waiting for input",
                    waitTime);
        let stats = test.stats();

        // Verify expected state size (if any).
        assert.gte(stats["stateSize"], minimumExpectedStateSize, "expected more state size");
        let heapProfile = null;
        if (shouldHeapProfile) {
            let result = db.serverStatus({wiredTiger: 0, storageEngine: 0, metrics: 0});
            assert.commandWorked(result);
            jsTestLog(JSON.stringify(result));
            heapProfile = result["heapProfile"];
        }

        // Wait for the expected output to show up (if any).
        assert.soon(() => {
            return test.stats().outputMessageCount == expectedOuptutAfterFirstStop.length;
        });

        // Stop the stream processor.
        test.stop();
        // Verify the output so far is expected.
        verifyOuptut(expectedOuptutAfterFirstStop);
        // Verify some checkpoints were written.
        let ids = test.getCheckpointIds();
        assert.gt(ids.length, 0, `expected some checkpoints`);

        // Insert the "after stop" input, and wait for the expected output.
        // Note that this input is inserted while the stream processor is not running.
        assert.commandWorked(test.inputColl.insertMany(inputAfterStop));

        // Run the streamProcessor, resuming from the last checkpoint.
        // Since we're restoring a checkpoint, we should resume from a resumeToken
        // that is before the "inputAfterStop" input above. So the stream processor
        // should still read the "inputAfterStop".
        test.run(false /* firstStart */);

        // Take a heap profile if asked.
        let heapProfileAfterCheckpoint = null;
        if (shouldHeapProfile) {
            let result = db.serverStatus({wiredTiger: 0, storageEngine: 0, metrics: 0});
            assert.commandWorked(result);
            jsTestLog(JSON.stringify(result));
            heapProfileAfterCheckpoint = result["heapProfile"];
        }
        let statsAfterCheckpoint = test.stats();

        // Verify expected state size (if any).
        assert.soon(
            () => { return statsAfterCheckpoint["stateSize"] >= minimumExpectedStateSize; });

        // Wait for all the output.
        assert.neq(expectedOutput, null);
        assert.soon(() => { return getResults().length == expectedOutput.length; },
                    "waiting for output",
                    waitTime);

        // Verify the output.
        verifyOuptut(expectedOutput);

        // Take a heap profile if asked.
        let heapProfileAfterWindowClose = null;
        if (shouldHeapProfile) {
            sleep(5000);
            let result = db.serverStatus({wiredTiger: 0, storageEngine: 0, metrics: 0});
            assert.commandWorked(result);
            jsTestLog(JSON.stringify(result));
            heapProfileAfterWindowClose = result["heapProfile"];
        }
        let statsAfterWindowClose = test.stats();

        // Stop the stream processor.
        test.stop();

        // Print heap profile information if asked.
        if (shouldHeapProfile) {
            // The parse.py script parses this log line for heapProfile and stats information.
            jsTestLog(JSON.stringify({
                opName: "innerStopStartHoppingWindowTest",
                extra: extraLogKeys,
                pipeline: pipeline,
                heapProfileBeforeCheckpoint: heapProfile,
                statsBeforeCheckpoint: stats,
                heapProfileAfterCheckpoint: heapProfileAfterCheckpoint,
                statsAfterCheckpoint: statsAfterCheckpoint,
                heapProfileAfterWindowClose: heapProfileAfterWindowClose,
                statsAfterWindowClose: statsAfterWindowClose,
            }));
        }
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
            {$project: {_id: 0, results: 1, _stream_meta: 1}}
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
                    "source": {
                        "type": "atlas",
                    },
                    "window": {
                        "start": ISODate("2023-12-01T00:20:00Z"),
                        "end": ISODate("2023-12-01T01:20:00Z"),
                    }
                },
                results: expectedResultsArray
            },
            {
                "_stream_meta": {
                    "source": {
                        "type": "atlas",
                    },
                    "window": {
                        "start": ISODate("2023-12-01T00:40:00Z"),
                        "end": ISODate("2023-12-01T01:40:00Z"),
                    }
                },
                results: expectedResultsArray
            },
            {
                "_stream_meta": {
                    "source": {
                        "type": "atlas",
                    },
                    "window": {
                        "start": ISODate("2023-12-01T01:00:00Z"),
                        "end": ISODate("2023-12-01T02:00:00Z"),
                    }
                },
                results: expectedResultsArray
            },
        ];
        innerStopStartTest({
            pipeline: pipeline,
            inputBeforeStop: input,
            inputAfterStop: [{ts: ISODate("2023-12-01T02:00:00.001Z")}],
            expectedOutput: expectedOutputDocs,
            resultsSortDoc: {
                "_stream_meta.window.start": 1,
            }
        });
    }

    function basicMatchStopStartTest() {
        innerStopStartTest({
            pipeline: [
                {$replaceRoot: {newRoot: "$fullDocument"}},
                {$match: {a: 1}},
                {$project: {_id: 0, _stream_meta: 0}},
            ],
            inputBeforeStop: [{a: 1}, {a: 4}, {a: 2}, {a: 1}],
            inputAfterStop: [
                {a: 1},
                {a: 42},
            ],
            expectedOutput: [
                {a: 1},
                {a: 1},
                {a: 1},
            ],
            expectedOuptutAfterFirstStop: [{a: 1}, {a: 1}],
            useTimeField: false
        });
    }

    // Tests a large $tumblingWindow with inputStateSizeMB.
    // This test also enables heap profiling information.
    function largeTumblingWindow(inputStateSizeMB) {
        const state = inputStateSizeMB * 1000000;
        // The pipeline includes an $unwind stage to duplicate every input doc this many times.
        const multiplierPerInputDoc = 2000;
        const strSize = 100;
        const prefixSize = 16;
        const statePerInputDoc = multiplierPerInputDoc * (prefixSize + strSize);
        const numInputDocs = Math.ceil(state / statePerInputDoc);
        const unwindArray = [...Array(multiplierPerInputDoc).keys()];
        Random.setRandomSeed(42);
        const str = makeRandomString(strSize);
        const input = [...Array(numInputDocs).keys()].map(idx => {
            return {
                ts: ISODate("2023-12-01T01:00:00.000Z"),
                customerId: idx,
                unwindArray: unwindArray,
                str: str
            };
        });
        const closeWindowInput = [{ts: ISODate("2023-12-01T02:00:00.001Z")}];

        let id =
            `${(numInputDocs - 1).toString()}/${(multiplierPerInputDoc - 1).toString()}/${str}`;
        innerStopStartTest({
            pipeline: [
                // The unwind stage "multiplies" the input docs for faster testing.
                {$unwind: "$fullDocument.unwindArray"},
                // For each doc, set str = customerId + unwindArray + str.
                // Now each doc will have a unique str.
                {
                    $project: {
                        str: {
                            $concat: [
                                {$toString: "$fullDocument.customerId"},
                                "/",
                                {$toString: "$fullDocument.unwindArray"},
                                "/",
                                "$fullDocument.str"
                            ]
                        },
                        customerId: "$fullDocument.customerId",
                        unwindArray: "$fullDocument.unwindArray"
                    }
                },
                {
                    $tumblingWindow: {
                        interval: {size: NumberInt(1), unit: "hour"},
                        allowedLateness: {size: NumberInt(0), unit: "second"},
                        pipeline: [
                            {$group: {_id: "$str", count: {$sum: 1}}},
                        ]
                    }
                },
                {$match: {_id: id}},
                {
                    $project: {
                        id: "$_id",
                        count: 1,
                    }
                }
            ],
            inputBeforeStop: input,
            inputAfterStop: closeWindowInput,
            expectedOutput: [
                {
                    _stream_meta: {
                        "sourceType": "atlas",
                        "windowStartTimestamp": ISODate("2023-12-01T01:00:00Z"),
                        "windowEndTimestamp": ISODate("2023-12-01T02:00:00Z")
                    },
                    count: 1,
                    id: id
                },
            ],
            minimumExpectedStateSize: state,
            shouldHeapProfile: true,
            extraLogKeys: {
                inputSize: state,
                numInputDocs: numInputDocs,
                inputDocMultiplier: multiplierPerInputDoc,
                statePerInputDoc: statePerInputDoc
            },
            waitTime: 60 * 60 * 1000,
            // Use a large interval so only a stop request will write a checkpoint.
            interval: 3600000
        });
    }

    function hoppingWindowGroupTest() {
        innerStopStartTest({
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
                        "source": {
                            "type": "atlas",
                        },
                        "window": {
                            "start": ISODate("2023-12-01T00:20:00Z"),
                            "end": ISODate("2023-12-01T01:20:00Z"),
                        }
                    },
                    "customerId": 0,
                    "max": 10,
                    "min": 1,
                    "sum": 11
                },
                {
                    "_stream_meta": {
                        "source": {
                            "type": "atlas",
                        },
                        "window": {
                            "start": ISODate("2023-12-01T00:20:00Z"),
                            "end": ISODate("2023-12-01T01:20:00Z"),
                        }
                    },
                    "customerId": 1,
                    "max": 4,
                    "min": 2,
                    "sum": 6
                },
                {
                    "_stream_meta": {
                        "source": {
                            "type": "atlas",
                        },
                        "window": {
                            "start": ISODate("2023-12-01T00:40:00Z"),
                            "end": ISODate("2023-12-01T01:40:00Z"),
                        }
                    },
                    "customerId": 0,
                    "max": 10,
                    "min": 1,
                    "sum": 11
                },
                {
                    "_stream_meta": {
                        "source": {
                            "type": "atlas",
                        },
                        "window": {
                            "start": ISODate("2023-12-01T00:40:00Z"),
                            "end": ISODate("2023-12-01T01:40:00Z"),
                        }
                    },
                    "customerId": 1,
                    "max": 4,
                    "min": 2,
                    "sum": 6
                },
                {
                    "_stream_meta": {
                        "source": {
                            "type": "atlas",
                        },
                        "window": {
                            "start": ISODate("2023-12-01T01:00:00Z"),
                            "end": ISODate("2023-12-01T02:00:00Z"),
                        }
                    },
                    "customerId": 0,
                    "max": 10,
                    "min": 1,
                    "sum": 11
                },
                {
                    "_stream_meta": {
                        "source": {
                            "type": "atlas",
                        },
                        "window": {
                            "start": ISODate("2023-12-01T01:00:00Z"),
                            "end": ISODate("2023-12-01T02:00:00Z"),
                        }
                    },
                    "customerId": 1,
                    "max": 4,
                    "min": 2,
                    "sum": 6
                },
            ],
            resultsSortDoc: {"_stream_meta.window.start": 1, "customerId": 1}
        });
    }

    function smokeTestCheckpointOnStop() {
        const input = generateInput(3333);
        // Use a long checkpoint interval so they don't automatically happen.
        let test = new TestHelper(input,
                                  [],
                                  1000000000 /* interval */,
                                  "kafka" /* sourceType */,
                                  useNewCheckpointing,
                                  useRestoredExecutionPlan);
        // Run the streamProcessor the streamProcessor.
        test.run();
        // Wait until some output appears in the output collection.
        waitForCount(test.outputColl, 1, /* maxWaitSeconds */ 60);
        test.stop();
        // Get the checkpoint IDs from the run.
        let ids = test.getCheckpointIds();
        // There should be 1 checkpoint for the start of a fresh streamProcessor,
        // 1 checkpoint from the stop.
        assert.eq(2, ids.length);
    }

    // Validate that when SP is started with checkpointOnStart option, it always writes a checkpoint
    function smokeTestCheckpointOnStart() {
        const input = generateInput(3333);
        // Use a long checkpoint interval so they don't automatically happen.
        let test = new TestHelper(input,
                                  [],
                                  1000000000 /* interval */,
                                  "kafka" /* sourceType */,
                                  useNewCheckpointing,
                                  useRestoredExecutionPlan);
        // Run the streamProcessor the streamProcessor.
        test.run();
        // Wait until some output appears in the output collection.
        waitForCount(test.outputColl, 1, /* maxWaitSeconds */ 60);
        test.stop();
        // Get the checkpoint IDs from the run.
        let ids = test.getCheckpointIds();
        // There should be 1 checkpoint for the start of a fresh streamProcessor,
        // 1 checkpoint from the stop.
        assert.eq(2, ids.length);

        // Start the processor with checkpointOnStart=true
        test.startFromLatestCheckpoint(false /* assertWorked */, true /* checkpointOnStart */);
        assert.soon(() => {
            ids = test.getCheckpointIds();
            return ids.length == 3;
        });
        test.stop();
    }

    function smokeTestCorrectnessChangestream() {
        const input = generateInput(503);
        let test = new TestHelper(
            input, [], 0, 'changestream', useNewCheckpointing, useRestoredExecutionPlan);

        test.run();
        // Wait until the last doc in the input appears in the output collection.
        waitForCount(test.outputColl, input.length, /* maxWaitSeconds */ 60);
        test.stop();
        // Verify the output matches the input.
        let results = test.getResults(true);
        assert.eq(results.length, input.length);
        for (let i = 0; i < results.length; i++) {
            verifyDocsEqual(input[i], results[i].fullDocument);
        }

        // Get the checkpoint IDs from the run.
        let ids = test.getCheckpointIds();
        assert.gt(ids.length, 0);
        jsTestLog(ids);

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
            // Clean the output.
            assert.commandWorked(test.outputColl.deleteMany({}));
            // Run the streamProcessor.
            test.run(false);
            // Get the expected starting point when restoring from this checkpoint.
            const startingPoint = test.getStartingPointFromCheckpoint(id);
            let expectedMinIdx;
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
            let test = new TestHelper(input,
                                      [],
                                      99999999 /* long interval */,
                                      'changestream',
                                      useNewCheckpointing,
                                      useRestoredExecutionPlan);
            test.sp.createStreamProcessor(test.spName, test.pipeline);
            assert.commandWorked(test.sp[test.spName].start(test.startOptions));
            // The streamProcessor will crash after the first document is output.
            assert.commandWorked(test.inputColl.insertMany(test.input));
            // Wait for one checkpoint to be committed.
            let checkpointIds = [];
            assert.soon(() => {
                checkpointIds = test.getCheckpointIds();
                return checkpointIds.length == 1;
            });
            let checkpointId = test.getCheckpointIds()[0];
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
            // Verify the first checkpoint for a changestream $source uses a Timestamp.
            let startingPoint =
                test.getStartingPointFromCheckpoint(checkpointId).startAtOperationTime;
            assert(startingPoint instanceof Timestamp);
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
                                  useNewCheckpointing,
                                  useRestoredExecutionPlan);
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
        stats = test.sp[test.spName].stats(true);
        jsTestLog(stats);
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
                                  useNewCheckpointing,
                                  useRestoredExecutionPlan);
        test.run();
        test.stop();
        test.run(false);
        test.stop();
    }

    // Validates that the resume token advances for a changestream $source,
    // even when the collection changestream is empty.
    function emptyChangestreamResumeTokenAdvances() {
        let test = new TestHelper([] /* empty input */,
                                  [] /* pipeline */,
                                  0,
                                  'changestream',
                                  useNewCheckpointing,
                                  useRestoredExecutionPlan);
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

        let resumeTokens = [];
        while (ids.length > 0) {
            const id = ids.shift();
            test.run(false);
            let startingPoint = test.getStartingPointFromCheckpoint(id);
            if (startingPoint.resumeToken != null) {
                resumeTokens.push(startingPoint.resumeToken);
            }
            test.stop();
            test.removeCheckpointsNotInList(ids);
        }
        // Verify we have at least 2 unique resume tokens in our checkpoints.
        assert.gte(new Set(resumeTokens).size, 2);
    }

    // TODO(SERVER-92447): Remove this.
    // Validate that trying to restore from a checkpoint with operators that don't match the
    // supplied pipeline will fail.
    function mismatchedCheckpointOperators() {
        // Run the processor, write a few checkpoints.
        let test = new TestHelper([] /* empty input */,
                                  [{$match: {a: 1}}] /* pipeline */,
                                  0,
                                  'changestream',
                                  useNewCheckpointing,
                                  false);
        let source = test.pipeline[0];
        let sink = test.pipeline[2];
        test.run();
        let waitForNumCheckpoints = 2;
        let i = 0;
        while (test.getCheckpointIds().length < waitForNumCheckpoints) {
            test.inputColl.insert({a: i++});
            sleep(100);
        }
        test.stop();
        let ids = test.getCheckpointIds();
        assert.gte(ids.length, waitForNumCheckpoints);

        // Modify the pipeline.
        test.pipeline = [source, {$match: {a: 1}}, {$project: {a: 1}}, sink];
        // Start the processor restoring from the checkpoint, with the modified pipeline supplied in
        // start.
        let result = test.startFromLatestCheckpoint(false /* assertWorked */);
        assert.commandFailedWithCode(result, ErrorCodes.InternalError);
        assert.eq("Invalid checkpoint. Checkpoint has 3 operators, OperatorDag has 4",
                  result.errmsg);

        test.pipeline = [source, {$project: {a: 1}}, sink];
        result = test.startFromLatestCheckpoint(false /* assertWorked */);
        assert.commandFailedWithCode(result, ErrorCodes.InternalError);
        assert.eq(
            "Invalid checkpoint. Checkpoint operator 1 name is MatchOperator, OperatorDag name is ProjectOperator",
            result.errmsg);
    }

    basicMatchStopStartTest();
    smokeTestCorrectness();
    smokeTestCorrectnessTumblingWindow();
    smokeTestStopStartWindow();
    hoppingWindowGroupTest();
    hoppingWindowSortTest();
    smokeTestCheckpointOnStop();
    smokeTestCheckpointOnStart();
    smokeTestCorrectnessChangestream();
    failPointTestAfterFirstOutput();
    smokeTestStatsInCheckpoint();
    smokeTestEmptyChangestream();
    emptyChangestreamResumeTokenAdvances();
    mismatchedCheckpointOperators();

    const buildInfo = db.runCommand("buildInfo");
    assert(buildInfo.hasOwnProperty("allocator"));

    if (buildInfo.allocator === "tcmalloc-google") {
        assert.commandWorked(
            db.adminCommand({setParameter: 1, heapProfilingSampleIntervalBytes: 1024 * 1024}));
        assert.soon(() => {
            let result = db.serverStatus();
            assert.commandWorked(result);
            return result.hasOwnProperty("heapProfile");
        }, "Heap profile serverStatus section is not present");

        // Uncomment these to test 1GB and 4GB windows locally.
        // largeTumblingWindow(1000);
        // largeTumblingWindow(4000);
        largeTumblingWindow(10);
    }
}

testBoth(true /* useNewCheckpointing */, true /* useRestoredExecutionPlan */);
// TODO(SERVER-92447): Remove this.
testBoth(true /* useNewCheckpointing */, false /* useRestoredExecutionPlan */);

assert.eq(listStreamProcessors()["streamProcessors"].length, 0);
