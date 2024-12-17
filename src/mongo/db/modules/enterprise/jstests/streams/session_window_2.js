/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */

import {commonTest} from "src/mongo/db/modules/enterprise/jstests/streams/checkpoint_helper.js";

commonTest({
    input: [
        {customer: 1, ad: 1, value: 2, ts: ISODate("2023-01-01T00:59:57")},
        {customer: 2, ad: 1, value: 2, ts: ISODate("2023-01-01T01:00:02")},
        {customer: 2, ad: 3, value: 1, ts: ISODate("2023-01-01T01:00:06")},
        {customer: 1, ad: 1, value: 3, ts: ISODate("2023-01-01T01:00:12")},
        {customer: 1, ad: 2, value: 3, ts: ISODate("2023-01-01T01:00:15")},
        {customer: 2, ad: 3, value: 3, ts: ISODate("2023-01-01T01:00:01")},
        // opens a new session for customer 1, closing the to other sessions
        {customer: 1, ad: 2, value: 3, ts: ISODate("2023-01-02T05:00:00")},
    ],
    pipeline: [
        {
            $sessionWindow: {
                gap: {size: NumberInt(1), unit: "minute"},
                partitionBy: "$customer",
                pipeline:
                    [{$group: {_id: "$ad", sum: {$sum: "$value"}}}, {$sort: {sum: -1}}, {$limit: 1}]
            }
        },
        {
            $project: {
                customer: "$_stream_meta.window.partition",
                ad: "$_id",
                totalValue: "$sum",
                window: {
                    start: "$_stream_meta.window.start",
                    end: "$_stream_meta.window.end",
                },
                _id: 0
            }
        }
    ],
    expectedOutput: [
        {
            "customer": 1,
            "ad": 1,
            "totalValue": 5,
            "window":
                {"start": ISODate("2023-01-01T00:59:57Z"), "end": ISODate("2023-01-01T01:00:15Z")}
        },
        {
            "customer": 2,
            "ad": 3,
            "totalValue": 4,
            "window":
                {"start": ISODate("2023-01-01T01:00:01Z"), "end": ISODate("2023-01-01T01:00:06Z")}
        }
    ]
});
commonTest({
    input: [
        {customer: 1, ad: 1, value: 2, ts: ISODate("2023-01-01T00:59:57")},
        {customer: 2, ad: 1, value: 3, ts: ISODate("2023-01-01T01:00:57")},
        {customer: 2, ad: 1, value: 6, ts: ISODate("2023-01-01T01:01:56")},
        {justAWatermark: 1, ts: ISODate("2023-01-01T03:00:57.001")}
    ],
    pipeline: [
        {$match: {$expr: {$ne: ["$justAWatermark", 1]}}},
        {
            $sessionWindow: {
                gap: {size: NumberInt(1), unit: "minute"},
                partitionBy: "$customer",
                pipeline: [
                    {$group: {_id: "$customer", all: {$push: "$value"}, avg: {$avg: "$value"}}},
                ]
            }
        },
        {
            $project: {
                _id: 1,
                all: 1,
                avg: 1,
                window: {
                    start: "$_stream_meta.window.start",
                    end: "$_stream_meta.window.end",
                    partition: "$_stream_meta.window.partition",
                }
            }
        }
    ],
    expectedOutput: [
        {
            _id: 1,
            "all": [2],
            "avg": 2,
            "window": {
                "start": ISODate("2023-01-01T00:59:57"),
                "end": ISODate("2023-01-01T00:59:57"),
                "partition": 1
            }
        },
        {
            _id: 2,
            "all": [3, 6],
            "avg": 4.5,
            "window": {
                start: ISODate("2023-01-01T01:00:57"),
                end: ISODate("2023-01-01T01:01:56"),
                partition: 2
            }
        }
    ],
});
commonTest({
    input: [
        {customer: 1, ad: 1, value: 2, ts: ISODate("2023-01-01T00:59:57")},
        {customer: 1, ad: 1, value: 4, ts: ISODate("2023-01-01T00:59:57")},
        {customer: 2, ad: 1, value: 3, ts: ISODate("2023-01-01T01:00:57")},
        {customer: 2, ad: 3, value: 6, ts: ISODate("2023-01-01T01:01:56")},
        {customer: 2, ad: 3, value: 4, ts: ISODate("2023-01-01T01:01:56")},
        {justAWatermark: 1, ts: ISODate("2023-01-01T03:00:57.001")}
    ],
    pipeline: [
        {$match: {$expr: {$ne: ["$justAWatermark", 1]}}},
        {
            $sessionWindow: {
                gap: {size: NumberInt(1), unit: "minute"},
                partitionBy: "$customer",
                pipeline: [
                    {$group: {_id: "$ad", all: {$push: "$value"}, avg: {$avg: "$value"}}},
                ]
            }
        },
        {
            $project: {
                customer: "$_stream_meta.window.partition",
                ad: "$_id",
                all: 1,
                avg: 1,
            }
        },
        {$project: {_id: 0}}
    ],
    expectedOutput: [
        {customer: 1, ad: 1, all: [2, 4], avg: 3},
        {customer: 2, ad: 1, all: [3], avg: 3},
        {customer: 2, ad: 3, all: [6, 4], avg: 5},
    ],
});