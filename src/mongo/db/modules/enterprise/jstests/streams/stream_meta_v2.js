/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */
import {commonTest} from "src/mongo/db/modules/enterprise/jstests/streams/checkpoint_helper.js";

// This file contains tests for the {$meta: "stream.<optional path>"} expression.

// Test reading stream.source metadata.
commonTest({
    input: [
        {customer: 1, ad: 1, value: 2},
        {customer: 1, ad: 2, value: 4},
        {customer: 1, ad: 3, value: 6}
    ],
    pipeline: [
        {
            $project: {
                _id: 0,
            }
        },
        {$project: {customer: 1, ad: 1, value: 1, sourceMeta: {$meta: "stream.source"}}}
    ],
    useTimeField: false,
    expectedGeneratedOutput: [
        {
            customer: 1,
            ad: 1,
            value: 2,
            sourceMeta: {type: "generated"},
            _stream_meta: {source: {type: "generated"}}
        },
        {
            customer: 1,
            ad: 2,
            value: 4,
            sourceMeta: {type: "generated"},
            _stream_meta: {source: {type: "generated"}}
        },
        {
            customer: 1,
            ad: 3,
            value: 6,
            sourceMeta: {type: "generated"},
            _stream_meta: {source: {type: "generated"}}
        },
    ],
    expectedChangestreamOutput: [
        {
            customer: 1,
            ad: 1,
            value: 2,
            sourceMeta: {type: "atlas"},
            _stream_meta: {source: {type: "atlas"}}
        },
        {
            customer: 1,
            ad: 2,
            value: 4,
            sourceMeta: {type: "atlas"},
            _stream_meta: {source: {type: "atlas"}}
        },
        {
            customer: 1,
            ad: 3,
            value: 6,
            sourceMeta: {type: "atlas"},
            _stream_meta: {source: {type: "atlas"}}
        },
    ],
    expectedTestKafkaOutput: [
        {
            customer: 1,
            ad: 1,
            value: 2,
            sourceMeta:
                {type: "kafka", topic: "t1", partition: 0, offset: 0, key: null, headers: []},
            _stream_meta: {
                source:
                    {type: "kafka", topic: "t1", partition: 0, offset: 0, key: null, headers: []}
            }
        },
        {
            customer: 1,
            ad: 2,
            value: 4,
            sourceMeta:
                {type: "kafka", topic: "t1", partition: 0, offset: 1, key: null, headers: []},
            _stream_meta: {
                source:
                    {type: "kafka", topic: "t1", partition: 0, offset: 1, key: null, headers: []}
            }
        },
        {
            customer: 1,
            ad: 3,
            value: 6,
            sourceMeta:
                {type: "kafka", topic: "t1", partition: 0, offset: 2, key: null, headers: []},
            _stream_meta: {
                source:
                    {type: "kafka", topic: "t1", partition: 0, offset: 2, key: null, headers: []}
            }
        },
    ],
});

// Test reading stream window metadata.
const windowInput = [
    {customer: 1, ad: 1, value: 2, ts: ISODate("2023-01-01T00:59:57")},
    {customer: 1, ad: 1, value: 3, ts: ISODate("2023-01-01T00:59:57")},
    {customer: 2, ad: 1, value: 4, ts: ISODate("2023-01-01T00:59:57")},
    {customer: 1, ad: 1, value: 5, ts: ISODate("2023-01-01T00:59:57")},
    {customer: 2, ad: 1, value: 1, ts: ISODate("2023-01-01T00:59:57")},
    // watermark
    {customer: 2, ad: 1, value: 6, ts: ISODate("2023-01-01T01:59:57")},
];
commonTest({
    input: windowInput,
    pipeline: [
        {
            $tumblingWindow: {
                pipeline: [
                    {$group: {_id: "$customer", total: {$sum: "$value"}}},
                    {$sort: {total: -1}},
                    {$limit: 1}
                ],
                interval: {unit: "second", size: NumberInt(5)}
            }
        },
        {
            $project: {
                windowStart: {$meta: "stream.window.start"},
                windowEnd: {$meta: "stream.window.end"},
                total: 1,
                _id: 1
            }
        },
        {
            $project: {
                _stream_meta: 0,
            }
        },
    ],
    expectedOutput: [{
        _id: 1,
        total: 10,
        windowStart: ISODate("2023-01-01T00:59:55Z"),
        windowEnd: ISODate("2023-01-01T01:00:00Z"),
    }],
});
commonTest({
    input: windowInput,
    pipeline: [
        {
            $tumblingWindow: {
                pipeline: [{$sort: {value: 1}}, {$limit: 2}],
                interval: {unit: "second", size: NumberInt(5)}
            }
        },
        {
            $addFields: {
                windowStart: {$meta: "stream.window.start"},
                windowEnd: {$meta: "stream.window.end"},
                sourceMeta: {$meta: "stream.source"},
            }
        },
        {
            $project: {
                _stream_meta: 0,
            }
        },
    ],
    expectedGeneratedOutput: [
        {
            customer: 1,
            ad: 1,
            value: 2,
            ts: ISODate("2023-01-01T00:59:57"),
            _ts: ISODate("2023-01-01T00:59:57"),
            sourceMeta: {type: "generated"},
            windowStart: ISODate("2023-01-01T00:59:55Z"),
            windowEnd: ISODate("2023-01-01T01:00:00Z")
        },
        {
            customer: 2,
            ad: 1,
            value: 1,
            ts: ISODate("2023-01-01T00:59:57"),
            _ts: ISODate("2023-01-01T00:59:57"),
            sourceMeta: {type: "generated"},
            windowStart: ISODate("2023-01-01T00:59:55Z"),
            windowEnd: ISODate("2023-01-01T01:00:00Z")
        },
    ],
    expectedTestKafkaOutput: [
        {
            customer: 1,
            ad: 1,
            value: 2,
            ts: ISODate("2023-01-01T00:59:57"),
            _ts: ISODate("2023-01-01T00:59:57"),
            sourceMeta:
                {type: "kafka", topic: "t1", partition: 0, offset: 0, key: null, headers: []},
            windowStart: ISODate("2023-01-01T00:59:55Z"),
            windowEnd: ISODate("2023-01-01T01:00:00Z")
        },
        {
            customer: 2,
            ad: 1,
            value: 1,
            ts: ISODate("2023-01-01T00:59:57"),
            _ts: ISODate("2023-01-01T00:59:57"),
            sourceMeta:
                {type: "kafka", topic: "t1", partition: 0, offset: 4, key: null, headers: []},
            windowStart: ISODate("2023-01-01T00:59:55Z"),
            windowEnd: ISODate("2023-01-01T01:00:00Z")
        },
    ],
    expectedChangestreamOutput: [
        {
            customer: 1,
            ad: 1,
            value: 2,
            ts: ISODate("2023-01-01T00:59:57"),
            sourceMeta: {type: "atlas"},
            windowStart: ISODate("2023-01-01T00:59:55Z"),
            windowEnd: ISODate("2023-01-01T01:00:00Z")
        },
        {
            customer: 2,
            ad: 1,
            value: 1,
            ts: ISODate("2023-01-01T00:59:57"),
            sourceMeta: {type: "atlas"},
            windowStart: ISODate("2023-01-01T00:59:55Z"),
            windowEnd: ISODate("2023-01-01T01:00:00Z")
        },
    ],
});

// Test with oldStreamMeta disabled.
commonTest({
    input: [
        {customer: 1, ad: 1, value: 2},
    ],
    pipeline: [
        {
            $project: {
                _id: 0,
            }
        },
        {$project: {customer: 1, ad: 1, value: 1, sourceMeta: {$meta: "stream.source"}}}
    ],
    useTimeField: false,
    expectedGeneratedOutput: [{
        customer: 1,
        ad: 1,
        value: 2,
        sourceMeta: {type: "generated"},
    }],
    expectedChangestreamOutput: [{
        customer: 1,
        ad: 1,
        value: 2,
        sourceMeta: {type: "atlas"},
    }],
    expectedTestKafkaOutput: [
        {
            customer: 1,
            ad: 1,
            value: 2,
            sourceMeta:
                {type: "kafka", topic: "t1", partition: 0, offset: 0, key: null, headers: []},
        },
    ],
    featureFlags: {oldStreamMeta: false}
});

// TODO(SERVER-99097): Add a few more tests.