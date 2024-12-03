import {
    testRunner
} from "src/mongo/db/modules/enterprise/jstests/streams/window_modify_test_utils.js";

const testCases = [
    {
        inputForOriginalPipeline: [
            // window 0 to 1
            {a: 1, ts: ISODate("2024-01-01T00:00:00.000Z")},
            {a: 2, ts: ISODate("2024-01-01T00:00:00.000Z")},
            {a: 3, ts: ISODate("2024-01-01T00:00:00.000Z")},
            // close window 0 to 1, open 1 to 2
            {a: 4, ts: ISODate("2024-01-01T00:00:01.001Z")},
            {a: 5, ts: ISODate("2024-01-01T00:00:01.500Z")},
            {a: 6, ts: ISODate("2024-01-01T00:00:01.999Z")},
        ],
        inputAfterModifyBeforeRestart: [
            {a: 7, ts: ISODate("2024-01-01T00:00:02.000Z")},
            // close the 1 to 2 window on the new pipeline
            {a: 8, ts: ISODate("2024-01-01T00:00:02.001Z")},
        ],
        originalPipeline: [
            {$replaceRoot: {newRoot: "$fullDocument"}},
            {
                $tumblingWindow: {
                    interval: {size: NumberInt(1), unit: "second"},
                    allowedLateness: {size: NumberInt(0), unit: "second"},
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
                    allowedLateness: {size: NumberInt(0), unit: "second"},
                    pipeline: [
                        // Change the $group to a $sort.
                        {$sort: {a: 1}},
                        {$limit: 1}
                    ]
                }
            }
        ],
        expectedOutput: [
            {"count": 3},
            {a: 4, ts: ISODate("2024-01-01T00:00:01.001Z")},
        ],
        afterModifyReplayCount: 6
    },
];

for (const testCase of testCases) {
    jsTestLog(`Running case: ${tojson(testCase)}`);
    testRunner(testCase);
}
