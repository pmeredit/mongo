/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */

import {
    resultsEq,
} from "jstests/aggregation/extras/utils.js";
import {
    TestHelper,
    uuidStr
} from "src/mongo/db/modules/enterprise/jstests/streams/checkpoint_helper.js";
import {listStreamProcessors} from "src/mongo/db/modules/enterprise/jstests/streams/utils.js";

const string100KB = new Array(1024 * 100).join('a');

// This utility function tests the basic modify(modifiedPipeline) flow:
// 1. Process data with the originalPipeline.
// 2. Stop the processor.
// 3. Validate the modify request.
// 4. Modify the processor to modifiedPipeline and start it, resuming from old checkpoint.
// 5. Process some more data.
// 6. Validate the expected stats and output.
function testRunner({
    originalPipeline,
    modifiedPipeline,
    inputForOriginalPipeline = [],
    inputAfterStopBeforeModify = [],
    expectedOutput = [],
    expectedDlqBeforeModify = [],
    expectedDlqAfterModify = [],
    expectedOperatorStatsAfterModify,
    resultsQuery = [],
    validateShouldSucceed = true,
    expectedValidateError = "",
    modifiedOutputCollection,
    modifiedPipeline2,
    inputAfterStopBeforeModify2 = [],
    modifyPipelineFunc,
    modifySourceFunc,
    resumeFromCheckpoint = true,
    expectedTotalInputMessages,
    oplogSizeMB,
    useTimeField = false,
    validateFailureCode = ErrorCodes.StreamProcessorInvalidOptions,
    modifyPipelineUseEarliestStartTimestamp,
    removeCheckpointsBeforeModify,
    sourceType = "changestream",
}) {
    const waitTimeMs = 30000;
    // Run the stream processor with the originalPipeline.
    let test = new TestHelper(inputForOriginalPipeline,
                              originalPipeline,
                              null,       /* interval */
                              sourceType, /* sourceType */
                              true,       /* useNewCheckpointing */
                              true,       /* useRestoredExecutionPlan */
                              null,       /* writeDir */
                              null,       /* restoreDir */
                              null,       /* dbForTest */
                              null,       /* targetSourceMergeDb */
                              useTimeField,
                              undefined,
                              undefined,
                              oplogSizeMB);

    let earliestStartTimestamp;
    if (modifyPipelineUseEarliestStartTimestamp) {
        const res = test.inputColl.insertOne({a: -1});
        earliestStartTimestamp = res.insertedId.getTimestamp();
    }

    test.run();
    // Wait for all the messages to be read.
    assert.soon(
        () => { return test.stats()["inputMessageCount"] == inputForOriginalPipeline.length; },
        tojson(test.stats()),
        waitTimeMs);
    // Wait for any expected DLQ messages to show up.
    assert.soon(() => { return test.dlqColl.count() == expectedDlqBeforeModify.length; },
                "waiting for dlqMessageCount",
                waitTimeMs);
    assert(resultsEq(expectedDlqBeforeModify,
                     test.dlqColl.aggregate([{$replaceRoot: {newRoot: "$doc"}}]).toArray(),
                     true /* verbose */,
                     ["_id"]));
    // Stop the stream processor, writing a final checkpoint.
    test.stop();

    // Insert the inputAfterStopBeforeModify. Note the stream processor is not running
    // at this point.
    test.inputColl.insertMany(inputAfterStopBeforeModify);
    const outputBeforeModify = test.outputColl.aggregate([]).toArray();

    if (removeCheckpointsBeforeModify) {
        // Remove all checkpoints
        test.checkpointUtil.clear();

        // This works around a quirk of changestreams where if we don't advance cluster timestamp
        // then the last inserted document, in this case inputAfterStopBeforeModify, will be
        // included in the changestream for the modified pipeline.
        test.targetSourceMergeDb["someCollection"].insertOne(
            {justAdvancingTheClusterTimestamp: true});
    }

    // Resume the stream processor on the new pipeline.
    jsTestLog(`Starting modified processor ${tojson(modifiedPipeline)}`);
    if (modifyPipelineFunc) {
        modifiedPipeline = modifyPipelineFunc(test);
    }
    let modifiedSource = undefined;
    if (modifySourceFunc) {
        modifiedSource = modifySourceFunc(test);
    }
    if (modifyPipelineUseEarliestStartTimestamp) {
        if (modifiedSource == null) {
            // initialize to $source stage
            modifiedSource = test.pipeline[0]["$source"];
        }

        modifiedSource.config = {
            ...modifiedSource.config,
            startAtOperationTime: earliestStartTimestamp,
        };
    }

    if (oplogSizeMB) {
        // Wait for the oplog to be truncated.
        test.targetSourceMergeDb.getSiblingDB("admin").runCommand(
            {replSetResizeOplog: 1, size: oplogSizeMB});
        const oplog = test.targetSourceMergeDb.getSiblingDB("local").oplog.rs;
        assert.soon(function() {
            const dataSize = oplog.dataSize();
            // The oplog milestone system allows the oplog to grow to 110% its max size.
            const targetSize = 1.1 * (oplogSizeMB * 1024 * 1024);
            jsTestLog("current oplog size: " + dataSize +
                      " bytes. Waiting until it is <= " + targetSize + " bytes.");
            return dataSize <= targetSize;
        }, "waiting for oplog to be truncated", 5 * 60 * 1000, 5 * 1000);
    }

    // Validate the modify request and, if validateShouldSucceed=true, start the modified processor.
    const validateResult = test.modifyAndStart({
        newPipeline: modifiedPipeline,
        validateShouldSucceed: validateShouldSucceed,
        modifyCollectionName: modifiedOutputCollection,
        modifiedSourceSpec: modifiedSource,
        resumeFromCheckpointAfterModify: resumeFromCheckpoint
    });
    if (!validateShouldSucceed) {
        assert.commandFailedWithCode(validateResult, validateFailureCode);
        assert(validateResult.errmsg.includes(expectedValidateError));
        // validate the SP doesn't exist in list results
        const results = test.list();
        assert.eq(0, results.length);
        return;
    }

    // Wait for all the input messages to be processed.
    let totalInputMessages = inputForOriginalPipeline.length + inputAfterStopBeforeModify.length;
    if (expectedTotalInputMessages) {
        totalInputMessages = expectedTotalInputMessages;
    }

    if (removeCheckpointsBeforeModify) {
        // Clearing the checkpoints invalidates the expected stats, so the expected value will no
        // longer line up. This may require more parameters in the future, but for now just setting
        // the value to 0 is sufficient.
        totalInputMessages = 0;
    }

    // if sourceType is sample we should just return true here because the input will be
    // inconsistent
    assert.soon(() => {
        return totalInputMessages == test.stats()["inputMessageCount"] || sourceType === "sample";
    });

    if (modifiedPipeline2) {
        test.stop();
        // Insert the inputAfterStopBeforeModify2. Note the stream processor is not running
        // at this point.
        test.inputColl.insertMany(inputAfterStopBeforeModify2);
        jsTestLog(`Starting processor modified a second time ${tojson(modifiedPipeline2)}`);
        test.modifyAndStart({
            newPipeline: modifiedPipeline2,
            validateShouldSucceed: true /* validateShouldSucceed */
        });
    }

    // Validate the expected output and summary stats.
    assert.soon(() => {
        let expectedOutputLength = expectedOutput.length;
        if (modifyPipelineUseEarliestStartTimestamp) {
            expectedOutputLength += outputBeforeModify.length;
        }

        if (removeCheckpointsBeforeModify) {
            // Clearing the checkpoints invalidates the expected stats, so the expected value will
            // no longer line up. This may require more parameters in the future, but for now just
            // setting the value to 0 is sufficient.
            expectedOutputLength = 0;
        }

        return test.stats()["outputMessageCount"] == expectedOutputLength &&
            test.stats()["dlqMessageCount"] == expectedDlqAfterModify.length;
    }, "waiting for expected output", waitTimeMs);

    // Validate we see the expected results in the output collection.
    const output = test.outputColl.aggregate(resultsQuery).toArray();
    if (modifiedOutputCollection) {
        output.push(...outputBeforeModify);
    }
    assert(resultsEq(expectedOutput, output, true /* verbose */, ["_id"] /* fieldsToSkip */));
    // Validate we see the expected results in the DLQ collection.
    assert.soon(() => {
        return resultsEq(expectedDlqAfterModify,
                         test.dlqColl.aggregate([{$replaceRoot: {newRoot: "$doc"}}]).toArray(),
                         true /* verbose */,
                         ["_id"] /* fieldsToSkip */);
    }, "waiting for expected DLQ output", waitTimeMs);

    // Validate the summary stats contain input, output, and DLQ counts for all
    // versions of the stream processor.
    const stats = test.stats();
    let expectedInputMessages = inputForOriginalPipeline.length +
        inputAfterStopBeforeModify.length + inputAfterStopBeforeModify2.length;
    if (expectedTotalInputMessages) {
        expectedInputMessages = expectedTotalInputMessages;
    }

    if (removeCheckpointsBeforeModify) {
        // Clearing the checkpoints invalidates the expected stats, so the expected value will no
        // longer line up. This may require more parameters in the future, but for now just setting
        // the value to 0 is sufficient.
        expectedInputMessages = 0;
    }

    assert.eq(expectedInputMessages, stats["inputMessageCount"]);
    assert.eq(expectedDlqAfterModify.length, stats["dlqMessageCount"]);
    let expectedOutputLength = expectedOutput.length;
    if (modifyPipelineUseEarliestStartTimestamp) {
        expectedOutputLength += outputBeforeModify.length;
    }

    if (removeCheckpointsBeforeModify) {
        // Clearing the checkpoints invalidates the expected stats, so the expected value will no
        // longer line up. This may require more parameters in the future, but for now just setting
        // the value to 0 is sufficient.
        expectedOutputLength = 0;
    }

    assert.eq(expectedOutputLength, stats["outputMessageCount"]);

    // Validate the per-operator stats of the processor. The per-operator
    // stats are for the events "after the modify".
    if (expectedOperatorStatsAfterModify) {
        jsTestLog(`Expected operator stats: ${tojson(expectedOperatorStatsAfterModify)}`);
        jsTestLog(`Actual operator stats: ${tojson(stats.operatorStats)}`);
        // Validate the per operator stats only contain information for the
        // latest version of the stream processor.
        assert.eq(stats.operatorStats.length, expectedOperatorStatsAfterModify.length);
        for (let opIdx = 0; opIdx < stats.operatorStats.length; opIdx += 1) {
            const actual = stats.operatorStats[opIdx];
            const expected = expectedOperatorStatsAfterModify[opIdx];
            assert.eq(actual.name, expected.name);
            assert.eq(actual.inputMessageCount, expected.inputMessageCount);
            assert.eq(actual.outputMessageCount, expected.outputMessageCount);
            assert.eq(actual.dlqMessageCount, expected.dlqMessageCount);
        }
    }

    // Stop the stream processor.
    test.stop();
}

const testCases = [
    {
        originalPipeline: [
            {$match: {operationType: "insert"}},
            {$replaceRoot: {newRoot: "$fullDocument"}},
            {$match: {a: 1}},
            {$project: {_stream_meta: 0}}
        ],
        modifiedPipeline: [
            {$match: {operationType: "insert"}},
            {$replaceRoot: {newRoot: "$fullDocument"}},
            {$match: {a: 1, b: 1}},
            {$addFields: {c: 42}},
            {$project: {_stream_meta: 0}}
        ],
        inputForOriginalPipeline: [{a: 1, b: 0}, {a: 1, b: 2}, {a: 0, b: 5}],
        inputAfterStopBeforeModify: [{a: 1, b: 0}, {a: 0, b: 1}, {a: 1, b: 1}],
        expectedOutput: [
            // Before the edit.
            {a: 1, b: 0},
            {a: 1, b: 2},
            // After the edit.
            {a: 1, b: 1, c: 42}
        ],
        // The per-operator stats after the modify are only for
        // docs processed after the modify.
        expectedOperatorStatsAfterModify: [
            {
                "name": "ChangeStreamConsumerOperator",
                "inputMessageCount": NumberLong(3),
                "outputMessageCount": NumberLong(3),
                "dlqMessageCount": NumberLong(0),
            },
            {
                "name": "MatchOperator",
                "inputMessageCount": NumberLong(3),
                "outputMessageCount": NumberLong(1),
                "dlqMessageCount": NumberLong(0),
            },
            {
                "name": "ReplaceRootOperator",
                "inputMessageCount": NumberLong(1),
                "outputMessageCount": NumberLong(1),
                "dlqMessageCount": NumberLong(0),
            },
            {
                "name": "AddFieldsOperator",
                "inputMessageCount": NumberLong(1),
                "outputMessageCount": NumberLong(1),
                "dlqMessageCount": NumberLong(0),
            },
            {
                "name": "ProjectOperator",
                "inputMessageCount": NumberLong(1),
                "outputMessageCount": NumberLong(1),
                "dlqMessageCount": NumberLong(0),
            },
            {
                "name": "MergeOperator",
                "inputMessageCount": NumberLong(1),
                "outputMessageCount": NumberLong(1),
                "dlqMessageCount": NumberLong(0),
            }
        ]
    },
    {
        originalPipeline: [],
        modifiedPipeline: [{$match: {"fullDocument.a": 1}}],
        inputForOriginalPipeline: [{a: 1, b: 0}, {a: 0, b: 2}, {a: 0, b: 5}],
        inputAfterStopBeforeModify: [
            {a: 0, b: 0},
            {a: 1, b: 42},
        ],
        expectedOutput: [
            // Before the edit.
            {a: 1, b: 0},
            {a: 0, b: 2},
            {a: 0, b: 5},
            // After the edit.
            {a: 1, b: 42},
        ],
        resultsQuery: [{$project: {a: "$fullDocument.a", b: "$fullDocument.b"}}],
        expectedOperatorStatsAfterModify: [
            {
                "name": "ChangeStreamConsumerOperator",
                "inputMessageCount": NumberLong(2),
                "outputMessageCount": NumberLong(2),
                "dlqMessageCount": NumberLong(0),
            },
            {
                "name": "MatchOperator",
                "inputMessageCount": NumberLong(2),
                "outputMessageCount": NumberLong(1),
                "dlqMessageCount": NumberLong(0),
            },
            {
                "name": "MergeOperator",
                "inputMessageCount": NumberLong(1),
                "outputMessageCount": NumberLong(1),
                "dlqMessageCount": NumberLong(0),
            }
        ],
    },
    {
        originalPipeline: [{$match: {"fullDocument.a": 1}}],
        modifiedPipeline: [],
        inputForOriginalPipeline: [{a: 1, b: 0}, {a: 0, b: 2}, {a: 0, b: 5}],
        inputAfterStopBeforeModify: [
            {a: 0, b: 0},
            {a: 1, b: 42},
        ],
        expectedOutput: [
            // Before the edit.
            {a: 1, b: 0},
            // After the edit.
            {a: 0, b: 0},
            {a: 1, b: 42},
        ],
        resultsQuery: [{$project: {a: "$fullDocument.a", b: "$fullDocument.b"}}],
        expectedOperatorStatsAfterModify: [
            {
                "name": "ChangeStreamConsumerOperator",
                "inputMessageCount": NumberLong(2),
                "outputMessageCount": NumberLong(2),
                "dlqMessageCount": NumberLong(0),
            },
            {
                "name": "MergeOperator",
                "inputMessageCount": NumberLong(2),
                "outputMessageCount": NumberLong(2),
                "dlqMessageCount": NumberLong(0),
            }
        ],
    },
    {
        originalPipeline: [
            {$match: {"fullDocument.a": 1}},
            {$replaceRoot: {newRoot: "$fullDocument"}},
            {$project: {_stream_meta: 0}}
        ],
        modifiedPipeline: [
            {$match: {"fullDocument.a": 1}},
            {$replaceRoot: {newRoot: "$fullDocument"}},
            {$project: {_stream_meta: 0}}
        ],
        // This will change the $merge collection.
        modifiedOutputCollection: uuidStr(),
        inputForOriginalPipeline: [
            {a: 1, b: 0},
        ],
        inputAfterStopBeforeModify: [
            {a: 1, b: 1},
        ],
        expectedOutput: [
            {a: 1, b: 0},
            {a: 1, b: 1},
        ]
    },
    {
        originalPipeline: [
            {$replaceRoot: {newRoot: "$fullDocument"}},
            {$validate: {validator: {$expr: {$eq: ["$a", 1]}}, validationAction: 'dlq'}},
        ],
        modifiedPipeline: [
            {$replaceRoot: {newRoot: "$fullDocument"}},
            {$validate: {validator: {$expr: {$eq: ["$a", 0]}}, validationAction: 'dlq'}},
        ],
        inputForOriginalPipeline: [
            {a: 1},
            {a: 0},
        ],
        inputAfterStopBeforeModify: [
            {a: 0},
            {a: 1},
        ],
        expectedOutput: [
            {a: 1},
            {a: 0},
        ],
        expectedDlqBeforeModify: [
            {a: 0},
        ],
        expectedDlqAfterModify: [
            {a: 0},
            {a: 1},
        ]
    },
    {
        originalPipeline: [
            {$replaceRoot: {newRoot: "$fullDocument"}},
            {$project: {_stream_meta: 0}},
            {$match: {a: 1}},
            {$project: {a: 1}},
        ],
        modifiedPipeline: [
            {$replaceRoot: {newRoot: "$fullDocument"}},
            {$project: {_stream_meta: 0}},
            {$match: {a: 2}},
        ],
        modifiedPipeline2: [
            {$replaceRoot: {newRoot: "$fullDocument"}},
            {$project: {_stream_meta: 0}},
            {$match: {a: 3}},
            {$addFields: {b: "foo"}},
        ],
        inputForOriginalPipeline: [
            {a: 0},
            {a: 1},
            {a: 2},
            {a: 3},
        ],
        inputAfterStopBeforeModify: [
            {a: 0},
            {a: 1},
            {a: 2},
            {a: 3},
        ],
        inputAfterStopBeforeModify2: [
            {a: 0},
            {a: 1},
            {a: 2},
            {a: 3},
        ],
        expectedOutput: [
            {a: 1},
            {a: 2},
            {a: 3, b: "foo"},
        ],
        expectedOperatorStatsAfterModify: [
            {
                "name": "ChangeStreamConsumerOperator",
                "inputMessageCount": NumberLong(4),
                "outputMessageCount": NumberLong(4),
                "dlqMessageCount": NumberLong(0),
            },
            {
                "name": "MatchOperator",
                "inputMessageCount": NumberLong(4),
                "outputMessageCount": NumberLong(1),
                "dlqMessageCount": NumberLong(0),
            },
            {
                "name": "ReplaceRootOperator",
                "inputMessageCount": NumberLong(1),
                "outputMessageCount": NumberLong(1),
                "dlqMessageCount": NumberLong(0),
            },
            {
                "name": "ProjectOperator",
                "inputMessageCount": NumberLong(1),
                "outputMessageCount": NumberLong(1),
                "dlqMessageCount": NumberLong(0),
            },
            {
                "name": "AddFieldsOperator",
                "inputMessageCount": NumberLong(1),
                "outputMessageCount": NumberLong(1),
                "dlqMessageCount": NumberLong(0),
            },
            {
                "name": "MergeOperator",
                "inputMessageCount": NumberLong(1),
                "outputMessageCount": NumberLong(1),
                "dlqMessageCount": NumberLong(0),
            }
        ],
    },
    {
        originalPipeline:
            [{$match: {"fullDocument.a": 1}}, {$replaceRoot: {newRoot: "$fullDocument"}}],
        modifiedPipeline: [{$foo: {"fullDocument.a": 1}}],
        validateShouldSucceed: false,
        expectedValidateError: "StreamProcessorInvalidOptions: Unsupported stage: $foo"
    },
    {
        originalPipeline: [
            {$match: {"fullDocument.a": 1}},
            {$replaceRoot: {newRoot: "$fullDocument"}},
            {
                $tumblingWindow: {
                    interval: {size: NumberInt(1), unit: "second"},
                    pipeline: [{$group: {_id: null, count: {$count: {}}}}]
                }
            }
        ],
        modifiedPipeline: [
            {$match: {"fullDocument.a": 1}},
            {$replaceRoot: {newRoot: "$fullDocument"}},
            // Remove the window stage.
        ],
        validateShouldSucceed: false,
        expectedValidateError:
            "resumeFromCheckpoint must be false to remove a window from a stream processor"
    },
    {
        originalPipeline: [
            {$match: {"fullDocument.a": 1}},
            {$replaceRoot: {newRoot: "$fullDocument"}},
            {
                $tumblingWindow: {
                    interval: {size: NumberInt(1), unit: "second"},
                    pipeline: [{$group: {_id: null, count: {$count: {}}}}]
                }
            }
        ],
        modifiedPipeline: [
            {$match: {"fullDocument.a": 1}},
            {$replaceRoot: {newRoot: "$fullDocument"}},
            {
                $tumblingWindow: {
                    interval: {size: NumberInt(1), unit: "second"},
                    pipeline: [
                        // Change the $group to a $sort.
                        {$sort: {a: 1}}
                    ]
                }
            }
        ],
    },
    {
        originalPipeline: [{$project: {a: "$fullDocument.a"}}, {$project: {_stream_meta: 0}}],
        modifyPipelineFunc: (test) => {
            return [
                {
                    $project: {
                        a: "$fullDocument.a"
                    }
                },
                // Add a lookup field.
                {
                    $lookup: {
                        from: {connectionName: test.dbConnectionName, db: test.dbName, coll: test.outputCollName},
                        localField: 'a',
                        foreignField: 'a',
                        as: 'out'
                    }
                },
                {
                    $project: {_stream_meta: 0}
                }
            ];
        },
        inputForOriginalPipeline: [
            {a: 1},
        ],
        inputAfterStopBeforeModify: [{a: 1}],
        expectedOutput: [
            {a: 1},
            {a: 1, out: [{a: 1}]},
        ],
    },
    {
        originalPipeline: [{$project: {a: "$fullDocument.a"}}, {$project: {_stream_meta: 0}}],
        // no-op edit
        modifiedPipeline: [{$project: {a: "$fullDocument.a"}}, {$project: {_stream_meta: 0}}],
        inputForOriginalPipeline: [
            {a: 1},
        ],
        inputAfterStopBeforeModify: [{a: 2}],
        expectedOutput: [
            {a: 1},
            {a: 2},
        ],
    },
    {
        originalPipeline: [{$project: {a: "$fullDocument.a"}}, {$project: {_stream_meta: 0}}],
        modifiedPipeline: [{$project: {a: "$fullDocument.a"}}, {$project: {_stream_meta: 0}}],
        // Change the source startAtOperationTime.
        modifySourceFunc: (test) => {
            test.targetSourceMergeDb["someCollection"].insertOne({a: 1});
            const operationTime = test.targetSourceMergeDb.hello().$clusterTime.clusterTime;
            return {
                connectionName: test.dbConnectionName,
                db: test.dbName,
                coll: test.outputCollName,
                config: {startAtOperationTime: operationTime}
            };
        },
        validateShouldSucceed: false,
        expectedValidateError:
            "resumeFromCheckpoint must be false to modify a stream processor's $source stage",
        inputForOriginalPipeline: [{a: 1}],
        inputAfterStopBeforeModify: [{a: 1}],
    },
    {
        originalPipeline: [{$project: {a: "$fullDocument.a"}}, {$project: {_stream_meta: 0}}],
        modifiedPipeline: [{$project: {a: "$fullDocument.a"}}, {$project: {_stream_meta: 0}}],
        // Change the source startAtOperationTime with resumeFromCheckpoint=false.
        modifySourceFunc: (test) => {
            test.targetSourceMergeDb["someCollection"].insertOne({a: 1});
            const operationTime = test.targetSourceMergeDb.hello().$clusterTime.clusterTime;
            return {
                connectionName: test.dbConnectionName,
                db: test.dbName,
                coll: test.outputCollName,
                config: {startAtOperationTime: operationTime}
            };
        },
        resumeFromCheckpoint: false,
        inputForOriginalPipeline: [{a: 1}],
        // This won't be observed by the modified processor.
        inputAfterStopBeforeModify: [{a: 1}],
        // This input message is from the original pipeline.
        expectedTotalInputMessages: 1,
        // This output is from the original pipeline.
        expectedOutput: [
            {a: 1},
        ],
    },
    {
        originalPipeline: [{
            $tumblingWindow: {
                interval: {unit: "second", size: NumberInt(1)},
                pipeline: [{$group: {_id: null, count: {$count: {}}}}],
            }
        }],
        modifiedPipeline: [{
            $tumblingWindow: {
                interval: {unit: "second", size: NumberInt(2)},
                pipeline: [{$group: {_id: null, count: {$count: {}}}}],
            }
        }],
        validateShouldSucceed: false,
        expectedValidateError:
            "resumeFromCheckpoint must be false to change a window stage's interval"
    },
    {
        originalPipeline: [{
            $tumblingWindow: {
                interval: {unit: "second", size: NumberInt(1)},
                pipeline: [{$group: {_id: null, count: {$count: {}}}}],
            }
        }],
        modifiedPipeline: [{
            $tumblingWindow: {
                interval: {unit: "second", size: NumberInt(2)},
                pipeline: [{$group: {_id: null, count: {$count: {}}}}],
            }
        }],
        resumeFromCheckpoint: false,
        validateShouldSucceed: true,
    },
    {
        originalPipeline: [{$project: {a: "$fullDocument.a"}}, {$project: {_stream_meta: 0}}],
        modifiedPipeline: [{$project: {a: "$fullDocument.a"}}, {$project: {_stream_meta: 0}}],
        modifyPipelineUseEarliestStartTimestamp: true,
        resumeFromCheckpoint: false,
        inputForOriginalPipeline: [{a: 1}],
        inputAfterStopBeforeModify: [{a: 1}],
        expectedTotalInputMessages: 4,
        expectedOutput: [{a: -1}, {a: 1}, {a: 1}],
    },
    {
        originalPipeline: [{
            $tumblingWindow: {
                interval: {unit: "second", size: NumberInt(1)},
                pipeline: [{$group: {_id: null, count: {$count: {}}}}],
            }
        }],
        modifiedPipeline: [{
            $tumblingWindow: {
                interval: {unit: "second", size: NumberInt(1)},
                pipeline: [{$group: {_id: null, count: {$count: {}}}}],
            }
        }],
        removeCheckpointsBeforeModify: true,
        resumeFromCheckpoint: false,
        validateShouldSucceed: true,
    },
    {
        originalPipeline: [{$project: {a: "$fullDocument.a"}}, {$project: {_stream_meta: 0}}],
        modifiedPipeline: [{$project: {a: "$fullDocument.a"}}, {$project: {_stream_meta: 0}}],
        inputForOriginalPipeline: [{a: 1}],
        inputAfterStopBeforeModify: [{a: 2}],
        expectedTotalInputMessages: 2,
        expectedOutput: [{a: 1}, {a: 2}],
    },
    {
        originalPipeline: [{$project: {a: "$fullDocument.a"}}, {$project: {_stream_meta: 0}}],
        modifiedPipeline: [{$project: {a: "$fullDocument.a"}}, {$project: {_stream_meta: 0}}],
        removeCheckpointsBeforeModify: true,
        inputForOriginalPipeline: [{a: 1}],
        inputAfterStopBeforeModify: [{a: 2}],
        expectedTotalInputMessages: 1,
        expectedOutput: [{a: 1}],
    },
    {
        originalPipeline: [{
            $tumblingWindow: {
                interval: {unit: "second", size: NumberInt(1)},
                pipeline: [{$match: {a: 1}}],
            }
        }],
        modifiedPipeline: [{
            $tumblingWindow: {
                interval: {unit: "second", size: NumberInt(1)},
                pipeline: [{$group: {_id: null, avg: {$avg: "$a"}}}],
            }
        }],
        validateShouldSucceed: false,
        expectedValidateError:
            "resumeFromCheckpoint must be false to modify a processor that has a window without a blocking stage"
    },
    {
        originalPipeline: [{
            $tumblingWindow: {
                interval: {unit: "second", size: NumberInt(1)},
                pipeline: [{$group: {_id: null, count: {$count: {}}}}],
            }
        }],
        modifiedPipeline: [{
            $tumblingWindow: {
                interval: {unit: "second", size: NumberInt(1)},
                pipeline: [{$group: {_id: null, avg: {$avg: "$a"}}}],
            }
        }],
        // 1GB oplog
        oplogSizeMB: 1000,
        // insert more than enough data to fall off the oplog
        inputForOriginalPipeline: Array.from({length: 1.5 * 1024 * 16},
                                             (_, i) => {
                                                 return {
                                                     i: i,
                                                     str: string100KB,
                                                     ts: ISODate("2024-01-01T00:00:00.000Z"),
                                                 };
                                             }),
        validateShouldSucceed: false,
        expectedValidateError:
            "Resume of change stream was not possible, as the resume point may no longer be in the oplog",
        validateFailureCode: ErrorCodes.StreamProcessorCannotResumeFromSource,
        useTimeField: true
    },
    {
        // start with the solar_sample connection
        sourceType: "sample",
        originalPipeline: [{$match: {a: {$exists: true}}}, {$project: {_stream_meta: 0}}],
        modifiedPipeline: [{$match: {a: {$exists: true}}}, {$project: {_stream_meta: 0}}],
        modifySourceFunc: (test) => {
            return {
                connectionName: test.dbConnectionName,
                db: test.dbName,
                coll: test.outputCollName,
            };
        },
        expectedOutput: [],
    },
];

// Note: for local dev, change testCases to testCases.slice(-1) if you just want to run the last
// test case.
for (const testCase of testCases) {
    jsTestLog(`Running: ${tojson({
        originalPipeline: testCase.originalPipeline,
        modifiedPipeline: testCase.modifiedPipeline
    })}`);
    testRunner(testCase);
}