/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */
import {} from "src/mongo/db/modules/enterprise/jstests/streams/utils.js";
import {
    testRunner
} from "src/mongo/db/modules/enterprise/jstests/streams/window_modify_test_utils.js";

const testCases = [
    {
        inputForOriginalPipeline: [
            {a: 1, b: 0, ts: ISODate("2024-01-01T00:00:00.000Z")},
            {a: 1, b: 1, ts: ISODate("2024-01-01T00:00:00.000Z")},
            {a: 0, b: 2, ts: ISODate("2024-01-01T00:00:02.000Z")}
        ],
        inputAfterStopBeforeModify: [{a: 0, b: 3, ts: ISODate("2024-01-01T00:00:30.000Z")}],
        inputAfterModifyBeforeRestart: [
            {a: 0, b: 4, ts: ISODate("2024-01-01T00:00:40.000Z")},
            {a: 1, b: 5, ts: ISODate("2024-01-01T00:00:40.000Z")},
            {a: 0, b: 6, ts: ISODate("2024-01-01T00:00:50.000Z")}
        ],
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
        expectedOutput: [
            {"a": 1, "b": 0, "ts": ISODate("2024-01-01T00:00:00Z")},
            {"a": 1, "b": 1, "ts": ISODate("2024-01-01T00:00:00Z")},
            {"a": 1, "b": 5, "ts": ISODate("2024-01-01T00:00:40Z")}
        ],
        afterModifyReplayCount: 3
    },
    {
        inputForOriginalPipeline: [
            {a: 1, b: 8, ts: ISODate("2024-01-01T00:00:01.000Z")},
            {a: 0, b: 24, ts: ISODate("2024-01-01T00:00:02.000Z")},
            {a: 1, b: 2, ts: ISODate("2024-01-01T00:00:03.000Z")},
        ],
        inputAfterCheckpoint1: [
            {a: 0, b: 9, ts: ISODate("2024-01-01T00:00:06.000Z")},
            {a: 1, b: 15, ts: ISODate("2024-01-01T00:00:07.000Z")}
        ],
        inputAfterCheckpoint2: [
            {a: 1, b: 12, ts: ISODate("2024-01-01T00:00:11.000Z")},
            {a: 1, b: 5, ts: ISODate("2024-01-01T00:00:12.000Z")}
        ],
        inputAfterStopBeforeModify: [
            {a: 0, b: 18, ts: ISODate("2024-01-01T00:00:16.000Z")},
            {a: 1, b: 3, ts: ISODate("2024-01-01T00:00:16.000Z")},
            {a: 1, b: 11, ts: ISODate("2024-01-01T00:00:17.000Z")}
        ],
        inputAfterModifyBeforeRestart: [
            {a: 0, b: 4, ts: ISODate("2024-01-01T00:00:40.000Z")},
            {a: 1, b: 13, ts: ISODate("2024-01-01T00:00:40.000Z")},
            {a: 0, b: 6, ts: ISODate("2024-01-01T00:02:00.000Z")},
        ],
        originalPipeline: [
            {$match: {"fullDocument.a": 1}},
            {$replaceRoot: {newRoot: "$fullDocument"}},
            {
                $hoppingWindow: {
                    interval: {size: NumberInt(20), unit: "second"},
                    allowedLateness: NumberInt(0),
                    hopSize: {size: NumberInt(5), unit: "second"},
                    pipeline: [{$group: {_id: null, count: {$count: {}}}}]
                }
            }
        ],
        modifiedPipeline: [
            {$match: {"fullDocument.a": 1}},
            {$replaceRoot: {newRoot: "$fullDocument"}},
            {
                $hoppingWindow: {
                    interval: {size: NumberInt(20), unit: "second"},
                    allowedLateness: NumberInt(0),
                    hopSize: {size: NumberInt(5), unit: "second"},
                    pipeline: [
                        // Change the $group to a $sort.
                        {$sort: {a: 1}}
                    ]
                }
            }
        ],
        expectedOutput: [
            {"count": 3},
            {"a": 1, "b": 8, "ts": ISODate("2024-01-01T00:00:01Z")},
            {"a": 1, "b": 2, "ts": ISODate("2024-01-01T00:00:03Z")},
            {"a": 1, "b": 15, "ts": ISODate("2024-01-01T00:00:07Z")},
            {"a": 1, "b": 12, "ts": ISODate("2024-01-01T00:00:11Z")},
            {"a": 1, "b": 5, "ts": ISODate("2024-01-01T00:00:12Z")},
            {"a": 1, "b": 3, "ts": ISODate("2024-01-01T00:00:16Z")},
            {"a": 1, "b": 11, "ts": ISODate("2024-01-01T00:00:17Z")},
            {"a": 1, "b": 13, "ts": ISODate("2024-01-01T00:00:40Z")}
        ],
        afterModifyReplayCount: 7
    },
    {
        originalPipeline: [
            {$replaceRoot: {newRoot: "$fullDocument"}},
            {$validate: {validator: {$expr: {$eq: ["$a", 1]}}, validationAction: 'dlq'}},
            {
                $tumblingWindow:
                    {interval: {size: NumberInt(1), unit: "second"}, pipeline: [{$sort: {a: 1}}]}
            },
        ],
        modifiedPipeline: [
            {$replaceRoot: {newRoot: "$fullDocument"}},
            {$validate: {validator: {$expr: {$eq: ["$a", 1]}}, validationAction: 'dlq'}},
            {
                $tumblingWindow:
                    {interval: {size: NumberInt(1), unit: "second"}, pipeline: [{$sort: {b: 1}}]}
            },
        ],
        inputForOriginalPipeline: [
            {a: 1, b: 1, ts: ISODate("2024-01-01T00:00:01.000Z")},
            {a: 0, b: 1, ts: ISODate("2024-01-01T00:00:01.000Z")}
        ],
        inputAfterStopBeforeModify: [
            {a: 0, b: 0, ts: ISODate("2024-01-01T00:00:03.000Z")},
            {a: 1, b: 0, ts: ISODate("2024-01-01T00:00:04.000Z")},
            {a: 1, b: 1, ts: ISODate("2024-01-01T00:00:14.000Z")}
        ],
        expectedOutput: [
            {a: 1, b: 1, ts: ISODate("2024-01-01T00:00:01.000Z")},
            {a: 1, b: 0, ts: ISODate("2024-01-01T00:00:04.000Z")}
        ],
        expectedDlqBeforeModify: [{a: 0, b: 1, ts: ISODate("2024-01-01T00:00:01.000Z")}],
        expectedDlqAfterModify: [
            {a: 0, b: 1, ts: ISODate("2024-01-01T00:00:01.000Z")},
            {a: 0, b: 1, ts: ISODate("2024-01-01T00:00:01.000Z")},
            {a: 0, b: 0, ts: ISODate("2024-01-01T00:00:03.000Z")}
        ],
        afterModifyReplayCount: 2
    },
    {
        inputForOriginalPipeline: [
            {a: 1, b: 8, ts: ISODate("2024-01-01T00:00:01.000Z")},
            {a: 1, b: 5, ts: ISODate("2024-01-01T00:00:06.000Z")},
            {a: 1, b: 3, ts: ISODate("2024-01-01T00:00:08.000Z")}
        ],
        inputAfterCheckpoint1: [
            // Documents will be replayed from here, after the modify-restore.
            {a: 0, b: 9, ts: ISODate("2024-01-01T00:00:26.000Z")},
            {a: 1, b: 15, ts: ISODate("2024-01-01T00:00:27.000Z")}
        ],
        inputAfterStopBeforeModify: [
            {a: 0, b: 18, ts: ISODate("2024-01-01T00:00:40.000Z")},
        ],
        originalPipeline: [
            {$match: {"fullDocument.a": 1}},
            {$replaceRoot: {newRoot: "$fullDocument"}},
            {
                $hoppingWindow: {
                    interval: {size: NumberInt(20), unit: "second"},
                    allowedLateness: NumberInt(0),
                    hopSize: {size: NumberInt(5), unit: "second"},
                    pipeline: [{$group: {_id: null, count: {$count: {}}}}]
                }
            }
        ],
        modifiedPipeline: [
            {$match: {"fullDocument.a": 1}},
            {$replaceRoot: {newRoot: "$fullDocument"}},
            {
                $hoppingWindow: {
                    interval: {size: NumberInt(20), unit: "second"},
                    allowedLateness: NumberInt(0),
                    hopSize: {size: NumberInt(5), unit: "second"},
                    pipeline: [
                        // Change the $group to a $sort.
                        {$sort: {a: 1}}
                    ]
                }
            }
        ],
        expectedOutput: [{"count": 2}, {a: 1, b: 15, ts: ISODate("2024-01-01T00:00:27.000Z")}],
        afterModifyReplayCount: 2
    },
    {
        inputForOriginalPipeline: [
            {a: 1, b: 8, ts: ISODate("2024-01-01T00:00:01.000Z")},
            {a: 1, b: 5, ts: ISODate("2024-01-01T00:00:06.000Z")},
            {a: 1, b: 3, ts: ISODate("2024-01-01T00:00:08.000Z")}
        ],
        inputAfterCheckpoint1: [
            {a: 0, b: 9, ts: ISODate("2024-01-01T00:00:26.000Z")},
            {a: 1, b: 15, ts: ISODate("2024-01-01T00:00:27.000Z")}
        ],
        inputAfterCheckpoint2: [
            // Documents will be replayed from here, after the modify-restore.
            {a: 1, b: 12, ts: ISODate("2024-01-01T00:01:01.000Z")},
            {a: 1, b: 7, ts: ISODate("2024-01-01T00:01:02.000Z")}
        ],
        inputAfterStopBeforeModify: [
            {a: 0, b: 11, ts: ISODate("2024-01-01T00:01:40.000Z")},
        ],
        inputAfterModifyBeforeRestart: [
            {a: 0, b: 4, ts: ISODate("2024-01-01T00:01:40.000Z")},
            {a: 1, b: 13, ts: ISODate("2024-01-01T00:01:40.000Z")},
            {a: 0, b: 6, ts: ISODate("2024-01-01T00:02:00.000Z")},
        ],
        originalPipeline: [
            {$match: {"fullDocument.a": 1}},
            {$replaceRoot: {newRoot: "$fullDocument"}},
            {
                $hoppingWindow: {
                    interval: {size: NumberInt(20), unit: "second"},
                    allowedLateness: NumberInt(0),
                    hopSize: {size: NumberInt(5), unit: "second"},
                    pipeline: [{$group: {_id: null, count: {$count: {}}}}]
                }
            }
        ],
        modifiedPipeline: [
            {$match: {"fullDocument.a": 1}},
            {$replaceRoot: {newRoot: "$fullDocument"}},
            {
                $hoppingWindow: {
                    interval: {size: NumberInt(20), unit: "second"},
                    allowedLateness: NumberInt(0),
                    hopSize: {size: NumberInt(5), unit: "second"},
                    pipeline: [
                        // Change the $group to a $sort.
                        {$sort: {a: 1}}
                    ]
                }
            }
        ],
        expectedOutput: [
            {"count": 1},
            {a: 1, b: 12, ts: ISODate("2024-01-01T00:01:01.000Z")},
            {a: 1, b: 7, ts: ISODate("2024-01-01T00:01:02.000Z")},
            {a: 1, b: 13, ts: ISODate("2024-01-01T00:01:40.000Z")}
        ],
        afterModifyReplayCount: 2
    },
    {
        inputForOriginalPipeline: [
            // window 1 open
            {a: 1, b: 8, ts: ISODate("2024-01-01T00:00:00.100Z")},
            // window 1 closes and is processed by the original pipeline
            // window 2 opens
            {a: 2, b: 1, ts: ISODate("2024-01-01T00:00:01.500Z")},
        ],
        inputAfterStopBeforeModify: [
            // Window 2 is closed and is processed by the modifiedPipeline
            // Window 3 is opened
            {a: 0, b: 4, ts: ISODate("2024-01-01T00:00:02.100Z")},
            {a: 3, b: 14, ts: ISODate("2024-01-01T00:00:02.300Z")},
        ],
        inputAfterModifyBeforeRestart: [
            // Window 3 is closed and is processed by modifiedPipeline
            // Window 4 is opened
            {a: 4, b: 11, ts: ISODate("2024-01-01T00:00:03.300Z")},
        ],
        afterModifyReplayCount: 2,
        originalPipeline: [
            {$match: {"fullDocument.a": {$gte: 1}}},
            {$replaceRoot: {newRoot: "$fullDocument"}},
            {
                $tumblingWindow: {
                    interval: {size: NumberInt(1), unit: "second"},
                    allowedLateness: NumberInt(0),
                    pipeline: [{$group: {_id: null, count: {$count: {}}}}]
                }
            }
        ],
        modifiedPipeline: [
            {$match: {"fullDocument.a": {$gte: 1}}},
            {$replaceRoot: {newRoot: "$fullDocument"}},
            {
                $tumblingWindow: {
                    interval: {size: NumberInt(1), unit: "second"},
                    allowedLateness: NumberInt(0),
                    pipeline: [
                        // Change the $group to a $sort.
                        {$sort: {a: 1}}
                    ]
                }
            }
        ],
        modifiedPipeline2: [
            {$match: {"fullDocument.a": {$gte: 1}}},
            {$match: {"fullDocument.b": {$gte: 5}}},
            {$replaceRoot: {newRoot: "$fullDocument"}},
            {
                $tumblingWindow: {
                    interval: {size: NumberInt(1), unit: "second"},
                    allowedLateness: NumberInt(0),
                    pipeline: [{$set: {"c": {$sum: ["$a", "$b"]}}}]
                }
            }
        ],
        inputAfterStopBeforeModify2: [
            {a: 5, b: 19, ts: ISODate("2024-01-01T00:00:03.700Z")},
            // Window 4 is closed and is processed by modifiedPipeline2
            // Window 5 is opened
            {a: 6, b: 4, ts: ISODate("2024-01-01T00:00:04.500Z")}
        ],
        inputAfterModifyBeforeRestart2: [
            {a: 7, b: 12, ts: ISODate("2024-01-01T00:00:04.600Z")},
            {a: 0, b: 2, ts: ISODate("2024-01-01T00:00:05.200Z")},
        ],
        expectedOutput: [
            {"count": 1},
            {a: 2, b: 1, ts: ISODate("2024-01-01T00:00:01.500Z")},
            {a: 3, b: 14, ts: ISODate("2024-01-01T00:00:02.300Z")},
            {a: 4, b: 11, c: 15, ts: ISODate("2024-01-01T00:00:03.300Z")},
            {a: 5, b: 19, c: 24, ts: ISODate("2024-01-01T00:00:03.700Z")},
            {a: 7, b: 12, c: 19, ts: ISODate("2024-01-01T00:00:04.600Z")}
        ],
        afterModifyReplayCount2: 1
    },
    {
        inputForOriginalPipeline: [
            {a: 1, b: 0, ts: ISODate("2024-01-01T00:00:00.000Z")},
            {a: 1, b: 1, ts: ISODate("2024-01-01T00:00:01.100Z")},
            {a: 0, b: 2, ts: ISODate("2024-01-01T00:00:01.600Z")},
            {a: 1, b: 3, ts: ISODate("2024-01-01T00:00:02.600Z")},
        ],
        inputAfterStopBeforeModify: [
            // Open Window 1
            {a: 1, b: 4, ts: ISODate("2024-01-01T00:00:03.100Z")},
            {a: 0, b: 5, ts: ISODate("2024-01-01T00:00:03.300Z")},
            {a: 1, b: 6, ts: ISODate("2024-01-01T00:00:03.500Z")},
            // Close window 1, open window 2 - count=2
            {a: 1, b: 7, ts: ISODate("2024-01-01T00:00:04.200Z")},
            {a: 1, b: 8, ts: ISODate("2024-01-01T00:00:04.600Z")},
            {a: 1, b: 9, ts: ISODate("2024-01-01T00:00:04.600Z")},
            {a: 1, b: 10, ts: ISODate("2024-01-01T00:00:04.700Z")},
            // Close window 2, open window 3 - count=4
            {a: 1, b: 11, ts: ISODate("2024-01-01T00:00:05.100Z")},
        ],
        inputAfterModifyBeforeRestart: [
            // Window 3
            {a: 1, b: 12, ts: ISODate("2024-01-01T00:00:05.200Z")},
            {a: 1, b: 14, ts: ISODate("2024-01-01T00:00:05.500Z")},
            // Close window 3, count=3
            {a: 0, b: 15, ts: ISODate("2024-01-01T00:00:06.200Z")},
            // Open window 4
            {a: 1, b: 16, ts: ISODate("2024-01-01T00:00:06.500Z")},
            // Close window 4, count=1
            {a: 0, b: 17, ts: ISODate("2024-01-01T00:00:07.200Z")},
        ],
        originalPipeline: [
            {$match: {"fullDocument.a": 1}},
            {$replaceRoot: {newRoot: "$fullDocument"}},
        ],
        modifiedPipeline: [
            {$match: {"fullDocument.a": 1}},
            {$replaceRoot: {newRoot: "$fullDocument"}},
            {
                $tumblingWindow: {
                    interval: {size: NumberInt(1), unit: "second"},
                    allowedLateness: {size: NumberInt(0), unit: "second"},
                    pipeline: [{$group: {_id: null, count: {$count: {}}}}]
                }
            },
            {$project: {_id: "$count", count: 1}}
        ],
        expectedOutput: [
            {a: 1, b: 0, ts: ISODate("2024-01-01T00:00:00.000Z")},
            {a: 1, b: 1, ts: ISODate("2024-01-01T00:00:01.100Z")},
            {a: 1, b: 3, ts: ISODate("2024-01-01T00:00:02.600Z")},
            {count: 2},
            {count: 4},
            {count: 3},
            {count: 1},
        ],
    },
    {
        originalPipeline:
            [{$match: {"fullDocument.a": 1}}, {$replaceRoot: {newRoot: "$fullDocument"}}],
        modifiedPipeline: [
            {$match: {"fullDocument.a": 1}},
            {$replaceRoot: {newRoot: "$fullDocument"}},
            {
                $tumblingWindow: {
                    interval: {unit: "second", size: NumberInt(1)},
                    pipeline: [{$group: {_id: null, count: {$count: {}}}}],
                }
            },
            {$project: {start: "$_stream_meta.window.start", count: 1}}
        ],
        inputForOriginalPipeline: [
            {a: 1, b: 0, ts: ISODate("2024-01-01T00:00:00.000Z")},
            {a: 0, b: 2, ts: ISODate("2024-01-01T00:00:01.000Z")},
            {a: 0, b: 5, ts: ISODate("2024-01-01T00:00:02.000Z")},
        ],
        inputAfterStopBeforeModify: [
            {a: 1, b: 0, ts: ISODate("2024-01-01T00:00:02.000Z")},
            {a: 1, b: 2, ts: ISODate("2024-01-01T00:00:02.000Z")},
            {a: 0, b: 2, ts: ISODate("2024-01-01T00:00:02.000Z")},
            // This will advance the watermark and close the 2-3 window.
            {a: 1, b: 5, ts: ISODate("2024-01-01T00:00:10.000Z")},
        ],
        expectedOutput: [
            // Before the edit.
            {a: 1, b: 0, ts: ISODate("2024-01-01T00:00:00.000Z")},
            // After the edit.
            {start: ISODate("2024-01-01T00:00:02.000Z"), count: 2}
        ],
    },
    {
        inputForOriginalPipeline: [
            {a: 0, b: 8, ts: ISODate("2024-01-01T00:00:01.500Z")},
            {a: 0, b: 5, ts: ISODate("2024-01-01T00:00:01.500Z")},
            {a: 1, b: 3, ts: ISODate("2024-01-01T00:00:01.800Z")}
        ],
        inputAfterCheckpoint1: [
            {a: 0, b: 9, ts: ISODate("2024-01-01T00:00:02.000Z")},
            {a: 1, b: 15, ts: ISODate("2024-01-01T00:00:02.000Z")}
        ],
        inputAfterCheckpoint2: [
            {a: 0, b: 12, ts: ISODate("2024-01-01T00:00:03.200Z")},
            {a: 1, b: 7, ts: ISODate("2024-01-01T00:00:03.200Z")}
        ],
        inputAfterStopBeforeModify: [
            {a: 1, b: 11, ts: ISODate("2024-01-01T00:00:04.500Z")},
            {a: 1, b: 18, ts: ISODate("2024-01-01T00:00:04.500Z")},
        ],
        inputAfterModifyBeforeRestart: [
            {a: 0, b: 4, ts: ISODate("2024-01-01T00:00:04.500Z")},
            {a: 1, b: 13, ts: ISODate("2024-01-01T00:00:05.200Z")},
        ],
        originalPipeline: [
            {$match: {"fullDocument.a": 1}},
            {$replaceRoot: {newRoot: "$fullDocument"}},
        ],
        modifiedPipeline: [
            {$match: {"fullDocument.a": 1}},
            {$replaceRoot: {newRoot: "$fullDocument"}},
            {
                $tumblingWindow: {
                    interval: {size: NumberInt(1), unit: "second"},
                    allowedLateness: {size: NumberInt(0), unit: "second"},
                    pipeline: [{$group: {_id: null, count: {$count: {}}}}]
                }
            },
        ],
        expectedOutput: [
            {a: 1, b: 3, ts: ISODate("2024-01-01T00:00:01.800Z")},
            {a: 1, b: 15, ts: ISODate("2024-01-01T00:00:02.000Z")},
            {a: 1, b: 7, ts: ISODate("2024-01-01T00:00:03.200Z")},
            {count: 2},
        ],
    }
];

for (const testCase of testCases) {
    jsTestLog(`Running: ${tojson(testCase)}`);
    testRunner(testCase);
}
