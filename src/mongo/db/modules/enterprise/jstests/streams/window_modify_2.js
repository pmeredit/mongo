import {
    testRunner
} from "src/mongo/db/modules/enterprise/jstests/streams/window_modify_test_utils.js";

const inputOutOfOrder = [
    // window 2 to 3
    {a: 4, ts: ISODate("2024-01-01T00:00:02.001Z")},
    {a: 5, ts: ISODate("2024-01-01T00:00:02.500Z")},
    // window 1 to 2
    {a: 4, ts: ISODate("2024-01-01T00:00:01.001Z")},
    {a: 5, ts: ISODate("2024-01-01T00:00:01.500Z")},
    {a: 6, ts: ISODate("2024-01-01T00:00:01.999Z")},
    // window 0 to 1
    {a: 1, ts: ISODate("2024-01-01T00:00:00.000Z")},
    {a: 2, ts: ISODate("2024-01-01T00:00:00.000Z")},
    {a: 3, ts: ISODate("2024-01-01T00:00:00.000Z")},
    {a: 4, ts: ISODate("2024-01-01T00:00:00.000Z")},
    {a: 0, ts: ISODate("2024-01-01T00:00:00.000Z")},
];
const inputOutOfOrderCloser = [
    // close the windows
    {a: 8, ts: ISODate("2024-01-02T00:00:02.001Z")},
];
const allAccumulators = {
    _id: "foo",
    addToSet: {$addToSet: "$a"},
    avg: {$avg: "$a"},
    max: {$max: "$a"},
    mergeObjects: {$mergeObjects: {a: "$a"}},
    min: {$min: "$a"},
    push: {$push: "$a"},
    stdDevPop: {$stdDevPop: "$a"},
    stdDevSamp: {$stdDevSamp: "$a"},
    sum: {$sum: "$a"},
    count: {$count: {}},
    bottom: {$bottom: {output: ["$a"], sortBy: {"a": 1}}},
    bottomN: {$bottomN: {n: 3, output: ["$a"], sortBy: {"a": 1}}},
    first: {$first: "$a"},
    firstN: {$firstN: {n: 3, input: "$a"}},
    last: {$last: "$a"},
    lastN: {$lastN: {n: 3, input: "$a"}},
    top: {$top: {output: ["$a"], sortBy: {"a": 1}}},
    topN: {$topN: {n: 3, output: ["$a"], sortBy: {"a": 1}}},
    maxN: {$maxN: {input: "$a", n: 2}},
};

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
    {
        inputForOriginalPipeline: inputOutOfOrder,
        inputAfterModifyBeforeRestart: inputOutOfOrderCloser,
        originalPipeline: [
            {$replaceRoot: {newRoot: "$fullDocument"}},
            {
                $tumblingWindow: {
                    interval: {size: NumberInt(1), unit: "second"},
                    allowedLateness: {size: NumberInt(90), unit: "second"},
                    pipeline: [{$group: {_id: "foo", count: {$count: {}}}}]
                }
            }
        ],
        modifiedPipeline: [
            {$match: {"fullDocument.a": {$gte: 1}}},
            {$replaceRoot: {newRoot: "$fullDocument"}},
            {
                $tumblingWindow: {
                    interval: {size: NumberInt(1), unit: "second"},
                    allowedLateness: {size: NumberInt(90), unit: "second"},
                    pipeline: [{$group: {_id: null, count: {$count: {}}}}]
                }
            },
            {$project: {_id: "$_stream_meta.window.start", count: 1}}
        ],
        expectedOutput: [
            {"count": 2},
            {"count": 3},
            {"count": 4},
        ],
        afterModifyReplayCount: 9
    },
    {
        inputForOriginalPipeline: [
            // 0 to 1 window
            {a: 4, ts: ISODate("2024-01-01T00:00:00.000Z")},
            {a: 5, ts: ISODate("2024-01-01T00:00:00.001Z")},
            // 1 to 2 window
            {a: 6, ts: ISODate("2024-01-01T00:00:01Z")},
            {a: 7, ts: ISODate("2024-01-01T00:00:01.001Z")},
        ],
        inputAfterModifyBeforeRestart: [
            {a: 1, ts: ISODate("2024-01-03T00:00:00Z")},
        ],
        originalPipeline: [
            {$replaceRoot: {newRoot: "$fullDocument"}},
            {
                $tumblingWindow: {
                    interval: {size: NumberInt(1), unit: "second"},
                    allowedLateness: {size: NumberInt(90), unit: "second"},
                    pipeline: [{$group: {_id: null, count: {$count: {}}}}]
                }
            },
        ],
        modifiedPipeline: [
            {$match: {"fullDocument.a": {$gte: 1}}},
            {$replaceRoot: {newRoot: "$fullDocument"}},
            {
                $tumblingWindow: {
                    interval: {size: NumberInt(1), unit: "second"},
                    allowedLateness: {size: NumberInt(90), unit: "second"},
                    pipeline: [{$group: allAccumulators}]
                }
            },
            {$set: {_id: "$_stream_meta.window.start"}}
        ],
        expectedOutput: [
            {
                "_id": ISODate("2024-01-01T00:00:00Z"),
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-01-01T00:00:00Z"),
                        "end": ISODate("2024-01-01T00:00:01Z")
                    }
                },
                "addToSet": [4, 5],
                "avg": 4.5,
                "bottom": [5],
                "bottomN": [[4], [5]],
                "count": 2,
                "first": 4,
                "firstN": [4, 5],
                "last": 5,
                "lastN": [4, 5],
                "max": 5,
                "maxN": [5, 4],
                "mergeObjects": {"a": 5},
                "min": 4,
                "push": [4, 5],
                "stdDevPop": 0.5,
                "stdDevSamp": 0.7071067811865476,
                "sum": 9,
                "top": [4],
                "topN": [[4], [5]]
            },
            {
                "_id": ISODate("2024-01-01T00:00:01Z"),
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-01-01T00:00:01Z"),
                        "end": ISODate("2024-01-01T00:00:02Z")
                    }
                },
                "addToSet": [6, 7],
                "avg": 6.5,
                "bottom": [7],
                "bottomN": [[6], [7]],
                "count": 2,
                "first": 6,
                "firstN": [6, 7],
                "last": 7,
                "lastN": [6, 7],
                "max": 7,
                "maxN": [7, 6],
                "mergeObjects": {"a": 7},
                "min": 6,
                "push": [6, 7],
                "stdDevPop": 0.5,
                "stdDevSamp": 0.7071067811865476,
                "sum": 13,
                "top": [6],
                "topN": [[6], [7]]
            }
        ],
        afterModifyReplayCount: 4
    },
    {
        inputForOriginalPipeline: [
            // windows (5/1 1:00, 5/2 1:00) to (5/2 0:00, 5/3 0:00)
            {a: 1, ts: ISODate("2024-05-02T00:00:00Z")},
            {a: 2, ts: ISODate("2024-05-02T00:00:00Z")},
            // windows (5/1 3:00, 5/2 3:00) to (5/2 2:00, 5/3 2:00)
            // closes the 5/1 1:00 to 5/2 1:00 window
            {a: 3, ts: ISODate("2024-05-02T02:00:00Z")},
        ],
        inputAfterModifyBeforeRestart: [
            // close everything
            {a: 10, ts: ISODate("2025-05-02T02:00:00Z")},
        ],
        originalPipeline: [
            {$replaceRoot: {newRoot: "$fullDocument"}},
            {
                $hoppingWindow: {
                    interval: {size: NumberInt(1), unit: "day"},
                    hopSize: {size: NumberInt(1), unit: "hour"},
                    allowedLateness: {size: NumberInt(0), unit: "second"},
                    pipeline: [{$group: {_id: null, count: {$count: {}}}}]
                }
            },
            {$set: {_id: "$_stream_meta.window.start"}}
        ],
        modifiedPipeline: [
            {$match: {"fullDocument.a": {$gte: 1}}},
            {$replaceRoot: {newRoot: "$fullDocument"}},
            {
                $hoppingWindow: {
                    interval: {size: NumberInt(1), unit: "day"},
                    hopSize: {size: NumberInt(1), unit: "hour"},
                    allowedLateness: {size: NumberInt(0), unit: "second"},
                    pipeline: [{$group: {_id: null, count: {$count: {}}, sum: {$sum: "$a"}}}]
                }
            },
            {$set: {_id: "$_stream_meta.window.start"}}
        ],
        expectedOutput: [
            {
                "_id": ISODate("2024-05-01T01:00:00Z"),
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T01:00:00Z"),
                        "end": ISODate("2024-05-02T01:00:00Z")
                    }
                },
                "count": 2
            },
            {
                "_id": ISODate("2024-05-01T02:00:00Z"),
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T02:00:00Z"),
                        "end": ISODate("2024-05-02T02:00:00Z")
                    }
                },
                "count": 2,
                "sum": 3
            },
            {
                "_id": ISODate("2024-05-01T03:00:00Z"),
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T03:00:00Z"),
                        "end": ISODate("2024-05-02T03:00:00Z")
                    }
                },
                "count": 3,
                "sum": 6
            },
            {
                "_id": ISODate("2024-05-01T04:00:00Z"),
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T04:00:00Z"),
                        "end": ISODate("2024-05-02T04:00:00Z")
                    }
                },
                "count": 3,
                "sum": 6
            },
            {
                "_id": ISODate("2024-05-01T05:00:00Z"),
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T05:00:00Z"),
                        "end": ISODate("2024-05-02T05:00:00Z")
                    }
                },
                "count": 3,
                "sum": 6
            },
            {
                "_id": ISODate("2024-05-01T06:00:00Z"),
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T06:00:00Z"),
                        "end": ISODate("2024-05-02T06:00:00Z")
                    }
                },
                "count": 3,
                "sum": 6
            },
            {
                "_id": ISODate("2024-05-01T07:00:00Z"),
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T07:00:00Z"),
                        "end": ISODate("2024-05-02T07:00:00Z")
                    }
                },
                "count": 3,
                "sum": 6
            },
            {
                "_id": ISODate("2024-05-01T08:00:00Z"),
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T08:00:00Z"),
                        "end": ISODate("2024-05-02T08:00:00Z")
                    }
                },
                "count": 3,
                "sum": 6
            },
            {
                "_id": ISODate("2024-05-01T09:00:00Z"),
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T09:00:00Z"),
                        "end": ISODate("2024-05-02T09:00:00Z")
                    }
                },
                "count": 3,
                "sum": 6
            },
            {
                "_id": ISODate("2024-05-01T10:00:00Z"),
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T10:00:00Z"),
                        "end": ISODate("2024-05-02T10:00:00Z")
                    }
                },
                "count": 3,
                "sum": 6
            },
            {
                "_id": ISODate("2024-05-01T11:00:00Z"),
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T11:00:00Z"),
                        "end": ISODate("2024-05-02T11:00:00Z")
                    }
                },
                "count": 3,
                "sum": 6
            },
            {
                "_id": ISODate("2024-05-01T12:00:00Z"),
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T12:00:00Z"),
                        "end": ISODate("2024-05-02T12:00:00Z")
                    }
                },
                "count": 3,
                "sum": 6
            },
            {
                "_id": ISODate("2024-05-01T13:00:00Z"),
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T13:00:00Z"),
                        "end": ISODate("2024-05-02T13:00:00Z")
                    }
                },
                "count": 3,
                "sum": 6
            },
            {
                "_id": ISODate("2024-05-01T14:00:00Z"),
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T14:00:00Z"),
                        "end": ISODate("2024-05-02T14:00:00Z")
                    }
                },
                "count": 3,
                "sum": 6
            },
            {
                "_id": ISODate("2024-05-01T15:00:00Z"),
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T15:00:00Z"),
                        "end": ISODate("2024-05-02T15:00:00Z")
                    }
                },
                "count": 3,
                "sum": 6
            },
            {
                "_id": ISODate("2024-05-01T16:00:00Z"),
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T16:00:00Z"),
                        "end": ISODate("2024-05-02T16:00:00Z")
                    }
                },
                "count": 3,
                "sum": 6
            },
            {
                "_id": ISODate("2024-05-01T17:00:00Z"),
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T17:00:00Z"),
                        "end": ISODate("2024-05-02T17:00:00Z")
                    }
                },
                "count": 3,
                "sum": 6
            },
            {
                "_id": ISODate("2024-05-01T18:00:00Z"),
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T18:00:00Z"),
                        "end": ISODate("2024-05-02T18:00:00Z")
                    }
                },
                "count": 3,
                "sum": 6
            },
            {
                "_id": ISODate("2024-05-01T19:00:00Z"),
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T19:00:00Z"),
                        "end": ISODate("2024-05-02T19:00:00Z")
                    }
                },
                "count": 3,
                "sum": 6
            },
            {
                "_id": ISODate("2024-05-01T20:00:00Z"),
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T20:00:00Z"),
                        "end": ISODate("2024-05-02T20:00:00Z")
                    }
                },
                "count": 3,
                "sum": 6
            },
            {
                "_id": ISODate("2024-05-01T21:00:00Z"),
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T21:00:00Z"),
                        "end": ISODate("2024-05-02T21:00:00Z")
                    }
                },
                "count": 3,
                "sum": 6
            },
            {
                "_id": ISODate("2024-05-01T22:00:00Z"),
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T22:00:00Z"),
                        "end": ISODate("2024-05-02T22:00:00Z")
                    }
                },
                "count": 3,
                "sum": 6
            },
            {
                "_id": ISODate("2024-05-01T23:00:00Z"),
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T23:00:00Z"),
                        "end": ISODate("2024-05-02T23:00:00Z")
                    }
                },
                "count": 3,
                "sum": 6
            },
            {
                "_id": ISODate("2024-05-02T00:00:00Z"),
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-02T00:00:00Z"),
                        "end": ISODate("2024-05-03T00:00:00Z")
                    }
                },
                "count": 3,
                "sum": 6
            },
            {
                "_id": ISODate("2024-05-02T01:00:00Z"),
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-02T01:00:00Z"),
                        "end": ISODate("2024-05-03T01:00:00Z")
                    }
                },
                "count": 1,
                "sum": 3
            },
            {
                "_id": ISODate("2024-05-02T02:00:00Z"),
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-02T02:00:00Z"),
                        "end": ISODate("2024-05-03T02:00:00Z")
                    }
                },
                "count": 1,
                "sum": 3
            }
        ],
        afterModifyReplayCount: 3
    },
    {
        inputForOriginalPipeline: [
            // windows (4/30 1:00, 5/1 1:00) to (5/1 0:00, 5/2 0:00)
            {customerId: "b", a: 0, ts: ISODate("2024-05-01T00:00:00Z")},
            // windows (5/1 1:00, 5/2 1:00) to (5/2 0:00, 5/3 0:00)
            {customerId: "foo", a: 1, ts: ISODate("2024-05-02T00:00:00Z")},
            {customerId: "foo", a: 9, ts: ISODate("2024-05-02T00:00:01Z")},
            {customerId: "bob", a: 2, ts: ISODate("2024-05-02T00:00:02Z")},
        ],
        inputAfterCheckpoint2: [
            // windows (5/1 3:00, 5/2 3:00) to (5/2 2:00, 5/3 2:00)
            // closes the 5/1 1:00 to 5/2 1:00 window
            {customerId: "baz", a: 3, ts: ISODate("2024-05-02T02:00:00Z")},
            {customerId: "bat", a: 4, ts: ISODate("2024-05-02T02:01:00Z")},
            {customerId: "bat", a: 5, ts: ISODate("2024-05-02T02:03:00Z")},
            {customerId: "bart", a: 5, ts: ISODate("2024-05-02T02:59:00Z")},
        ],
        inputAfterModifyBeforeRestart: [
            // close everything
            {customerId: "foo", a: 10, ts: ISODate("2025-05-02T02:00:00Z")},
        ],
        originalPipeline: [
            {$replaceRoot: {newRoot: "$fullDocument"}},
            {
                $hoppingWindow: {
                    interval: {size: NumberInt(1), unit: "day"},
                    hopSize: {size: NumberInt(1), unit: "hour"},
                    allowedLateness: {size: NumberInt(0), unit: "second"},
                    pipeline:
                        [{$group: {_id: "$customerId", count: {$count: {}}, avg: {$avg: "$a"}}}]
                }
            },
            {$set: {foo: "one"}},
            {$set: {_id: {start: "$_stream_meta.window.start", customerId: "$_id"}}}
        ],
        modifiedPipeline: [
            {$match: {"fullDocument.a": {$gte: 1}}},
            {$replaceRoot: {newRoot: "$fullDocument"}},
            {
                $hoppingWindow: {
                    interval: {size: NumberInt(1), unit: "day"},
                    hopSize: {size: NumberInt(1), unit: "hour"},
                    allowedLateness: {size: NumberInt(0), unit: "second"},
                    pipeline: [
                        {
                            $group: {
                                _id: "$customerId",
                                count: {$count: {}},
                                sum: {$sum: "$a"},
                                push: {$push: "$a"}
                            }
                        },
                        {$set: {foo: "two"}},
                    ]
                }
            },
            {$set: {_id: {start: "$_stream_meta.window.start", customerId: "$_id"}}}
        ],
        expectedOutput: [
            {
                "_id": {"start": ISODate("2024-04-30T01:00:00Z"), "customerId": "b"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-04-30T01:00:00Z"),
                        "end": ISODate("2024-05-01T01:00:00Z")
                    }
                },
                "avg": 0,
                "count": 1,
                "foo": "one"
            },
            {
                "_id": {"start": ISODate("2024-04-30T02:00:00Z"), "customerId": "b"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-04-30T02:00:00Z"),
                        "end": ISODate("2024-05-01T02:00:00Z")
                    }
                },
                "avg": 0,
                "count": 1,
                "foo": "one"
            },
            {
                "_id": {"start": ISODate("2024-04-30T03:00:00Z"), "customerId": "b"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-04-30T03:00:00Z"),
                        "end": ISODate("2024-05-01T03:00:00Z")
                    }
                },
                "avg": 0,
                "count": 1,
                "foo": "one"
            },
            {
                "_id": {"start": ISODate("2024-04-30T04:00:00Z"), "customerId": "b"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-04-30T04:00:00Z"),
                        "end": ISODate("2024-05-01T04:00:00Z")
                    }
                },
                "avg": 0,
                "count": 1,
                "foo": "one"
            },
            {
                "_id": {"start": ISODate("2024-04-30T05:00:00Z"), "customerId": "b"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-04-30T05:00:00Z"),
                        "end": ISODate("2024-05-01T05:00:00Z")
                    }
                },
                "avg": 0,
                "count": 1,
                "foo": "one"
            },
            {
                "_id": {"start": ISODate("2024-04-30T06:00:00Z"), "customerId": "b"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-04-30T06:00:00Z"),
                        "end": ISODate("2024-05-01T06:00:00Z")
                    }
                },
                "avg": 0,
                "count": 1,
                "foo": "one"
            },
            {
                "_id": {"start": ISODate("2024-04-30T07:00:00Z"), "customerId": "b"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-04-30T07:00:00Z"),
                        "end": ISODate("2024-05-01T07:00:00Z")
                    }
                },
                "avg": 0,
                "count": 1,
                "foo": "one"
            },
            {
                "_id": {"start": ISODate("2024-04-30T08:00:00Z"), "customerId": "b"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-04-30T08:00:00Z"),
                        "end": ISODate("2024-05-01T08:00:00Z")
                    }
                },
                "avg": 0,
                "count": 1,
                "foo": "one"
            },
            {
                "_id": {"start": ISODate("2024-04-30T09:00:00Z"), "customerId": "b"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-04-30T09:00:00Z"),
                        "end": ISODate("2024-05-01T09:00:00Z")
                    }
                },
                "avg": 0,
                "count": 1,
                "foo": "one"
            },
            {
                "_id": {"start": ISODate("2024-04-30T10:00:00Z"), "customerId": "b"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-04-30T10:00:00Z"),
                        "end": ISODate("2024-05-01T10:00:00Z")
                    }
                },
                "avg": 0,
                "count": 1,
                "foo": "one"
            },
            {
                "_id": {"start": ISODate("2024-04-30T11:00:00Z"), "customerId": "b"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-04-30T11:00:00Z"),
                        "end": ISODate("2024-05-01T11:00:00Z")
                    }
                },
                "avg": 0,
                "count": 1,
                "foo": "one"
            },
            {
                "_id": {"start": ISODate("2024-04-30T12:00:00Z"), "customerId": "b"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-04-30T12:00:00Z"),
                        "end": ISODate("2024-05-01T12:00:00Z")
                    }
                },
                "avg": 0,
                "count": 1,
                "foo": "one"
            },
            {
                "_id": {"start": ISODate("2024-04-30T13:00:00Z"), "customerId": "b"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-04-30T13:00:00Z"),
                        "end": ISODate("2024-05-01T13:00:00Z")
                    }
                },
                "avg": 0,
                "count": 1,
                "foo": "one"
            },
            {
                "_id": {"start": ISODate("2024-04-30T14:00:00Z"), "customerId": "b"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-04-30T14:00:00Z"),
                        "end": ISODate("2024-05-01T14:00:00Z")
                    }
                },
                "avg": 0,
                "count": 1,
                "foo": "one"
            },
            {
                "_id": {"start": ISODate("2024-04-30T15:00:00Z"), "customerId": "b"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-04-30T15:00:00Z"),
                        "end": ISODate("2024-05-01T15:00:00Z")
                    }
                },
                "avg": 0,
                "count": 1,
                "foo": "one"
            },
            {
                "_id": {"start": ISODate("2024-04-30T16:00:00Z"), "customerId": "b"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-04-30T16:00:00Z"),
                        "end": ISODate("2024-05-01T16:00:00Z")
                    }
                },
                "avg": 0,
                "count": 1,
                "foo": "one"
            },
            {
                "_id": {"start": ISODate("2024-04-30T17:00:00Z"), "customerId": "b"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-04-30T17:00:00Z"),
                        "end": ISODate("2024-05-01T17:00:00Z")
                    }
                },
                "avg": 0,
                "count": 1,
                "foo": "one"
            },
            {
                "_id": {"start": ISODate("2024-04-30T18:00:00Z"), "customerId": "b"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-04-30T18:00:00Z"),
                        "end": ISODate("2024-05-01T18:00:00Z")
                    }
                },
                "avg": 0,
                "count": 1,
                "foo": "one"
            },
            {
                "_id": {"start": ISODate("2024-04-30T19:00:00Z"), "customerId": "b"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-04-30T19:00:00Z"),
                        "end": ISODate("2024-05-01T19:00:00Z")
                    }
                },
                "avg": 0,
                "count": 1,
                "foo": "one"
            },
            {
                "_id": {"start": ISODate("2024-04-30T20:00:00Z"), "customerId": "b"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-04-30T20:00:00Z"),
                        "end": ISODate("2024-05-01T20:00:00Z")
                    }
                },
                "avg": 0,
                "count": 1,
                "foo": "one"
            },
            {
                "_id": {"start": ISODate("2024-04-30T21:00:00Z"), "customerId": "b"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-04-30T21:00:00Z"),
                        "end": ISODate("2024-05-01T21:00:00Z")
                    }
                },
                "avg": 0,
                "count": 1,
                "foo": "one"
            },
            {
                "_id": {"start": ISODate("2024-04-30T22:00:00Z"), "customerId": "b"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-04-30T22:00:00Z"),
                        "end": ISODate("2024-05-01T22:00:00Z")
                    }
                },
                "avg": 0,
                "count": 1,
                "foo": "one"
            },
            {
                "_id": {"start": ISODate("2024-04-30T23:00:00Z"), "customerId": "b"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-04-30T23:00:00Z"),
                        "end": ISODate("2024-05-01T23:00:00Z")
                    }
                },
                "avg": 0,
                "count": 1,
                "foo": "one"
            },
            {
                "_id": {"start": ISODate("2024-05-01T00:00:00Z"), "customerId": "b"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T00:00:00Z"),
                        "end": ISODate("2024-05-02T00:00:00Z")
                    }
                },
                "avg": 0,
                "count": 1,
                "foo": "one"
            },
            {
                "_id": {"start": ISODate("2024-05-01T01:00:00Z"), "customerId": "foo"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T01:00:00Z"),
                        "end": ISODate("2024-05-02T01:00:00Z")
                    }
                },
                "avg": 5,
                "count": 2,
                "foo": "one"
            },
            {
                "_id": {"start": ISODate("2024-05-01T01:00:00Z"), "customerId": "bob"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T01:00:00Z"),
                        "end": ISODate("2024-05-02T01:00:00Z")
                    }
                },
                "avg": 2,
                "count": 1,
                "foo": "one"
            },
            {
                "_id": {"start": ISODate("2024-05-01T02:00:00Z"), "customerId": "foo"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T02:00:00Z"),
                        "end": ISODate("2024-05-02T02:00:00Z")
                    }
                },
                "avg": 5,
                "count": 2,
                "foo": "one"
            },
            {
                "_id": {"start": ISODate("2024-05-01T02:00:00Z"), "customerId": "bob"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T02:00:00Z"),
                        "end": ISODate("2024-05-02T02:00:00Z")
                    }
                },
                "avg": 2,
                "count": 1,
                "foo": "one"
            },
            {
                "_id": {"start": ISODate("2024-05-01T03:00:00Z"), "customerId": "baz"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T03:00:00Z"),
                        "end": ISODate("2024-05-02T03:00:00Z")
                    }
                },
                "count": 1,
                "foo": "two",
                "push": [3],
                "sum": 3
            },
            {
                "_id": {"start": ISODate("2024-05-01T03:00:00Z"), "customerId": "foo"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T03:00:00Z"),
                        "end": ISODate("2024-05-02T03:00:00Z")
                    }
                },
                "count": 2,
                "foo": "two",
                "push": [1, 9],
                "sum": 10
            },
            {
                "_id": {"start": ISODate("2024-05-01T03:00:00Z"), "customerId": "bob"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T03:00:00Z"),
                        "end": ISODate("2024-05-02T03:00:00Z")
                    }
                },
                "count": 1,
                "foo": "two",
                "push": [2],
                "sum": 2
            },
            {
                "_id": {"start": ISODate("2024-05-01T03:00:00Z"), "customerId": "bat"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T03:00:00Z"),
                        "end": ISODate("2024-05-02T03:00:00Z")
                    }
                },
                "count": 2,
                "foo": "two",
                "push": [4, 5],
                "sum": 9
            },
            {
                "_id": {"start": ISODate("2024-05-01T04:00:00Z"), "customerId": "baz"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T04:00:00Z"),
                        "end": ISODate("2024-05-02T04:00:00Z")
                    }
                },
                "count": 1,
                "foo": "two",
                "push": [3],
                "sum": 3
            },
            {
                "_id": {"start": ISODate("2024-05-01T04:00:00Z"), "customerId": "bob"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T04:00:00Z"),
                        "end": ISODate("2024-05-02T04:00:00Z")
                    }
                },
                "count": 1,
                "foo": "two",
                "push": [2],
                "sum": 2
            },
            {
                "_id": {"start": ISODate("2024-05-01T04:00:00Z"), "customerId": "foo"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T04:00:00Z"),
                        "end": ISODate("2024-05-02T04:00:00Z")
                    }
                },
                "count": 2,
                "foo": "two",
                "push": [1, 9],
                "sum": 10
            },
            {
                "_id": {"start": ISODate("2024-05-01T04:00:00Z"), "customerId": "bat"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T04:00:00Z"),
                        "end": ISODate("2024-05-02T04:00:00Z")
                    }
                },
                "count": 2,
                "foo": "two",
                "push": [4, 5],
                "sum": 9
            },
            {
                "_id": {"start": ISODate("2024-05-01T05:00:00Z"), "customerId": "bob"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T05:00:00Z"),
                        "end": ISODate("2024-05-02T05:00:00Z")
                    }
                },
                "count": 1,
                "foo": "two",
                "push": [2],
                "sum": 2
            },
            {
                "_id": {"start": ISODate("2024-05-01T05:00:00Z"), "customerId": "foo"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T05:00:00Z"),
                        "end": ISODate("2024-05-02T05:00:00Z")
                    }
                },
                "count": 2,
                "foo": "two",
                "push": [1, 9],
                "sum": 10
            },
            {
                "_id": {"start": ISODate("2024-05-01T05:00:00Z"), "customerId": "baz"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T05:00:00Z"),
                        "end": ISODate("2024-05-02T05:00:00Z")
                    }
                },
                "count": 1,
                "foo": "two",
                "push": [3],
                "sum": 3
            },
            {
                "_id": {"start": ISODate("2024-05-01T05:00:00Z"), "customerId": "bat"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T05:00:00Z"),
                        "end": ISODate("2024-05-02T05:00:00Z")
                    }
                },
                "count": 2,
                "foo": "two",
                "push": [4, 5],
                "sum": 9
            },
            {
                "_id": {"start": ISODate("2024-05-01T06:00:00Z"), "customerId": "baz"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T06:00:00Z"),
                        "end": ISODate("2024-05-02T06:00:00Z")
                    }
                },
                "count": 1,
                "foo": "two",
                "push": [3],
                "sum": 3
            },
            {
                "_id": {"start": ISODate("2024-05-01T06:00:00Z"), "customerId": "foo"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T06:00:00Z"),
                        "end": ISODate("2024-05-02T06:00:00Z")
                    }
                },
                "count": 2,
                "foo": "two",
                "push": [1, 9],
                "sum": 10
            },
            {
                "_id": {"start": ISODate("2024-05-01T06:00:00Z"), "customerId": "bob"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T06:00:00Z"),
                        "end": ISODate("2024-05-02T06:00:00Z")
                    }
                },
                "count": 1,
                "foo": "two",
                "push": [2],
                "sum": 2
            },
            {
                "_id": {"start": ISODate("2024-05-01T06:00:00Z"), "customerId": "bat"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T06:00:00Z"),
                        "end": ISODate("2024-05-02T06:00:00Z")
                    }
                },
                "count": 2,
                "foo": "two",
                "push": [4, 5],
                "sum": 9
            },
            {
                "_id": {"start": ISODate("2024-05-01T07:00:00Z"), "customerId": "baz"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T07:00:00Z"),
                        "end": ISODate("2024-05-02T07:00:00Z")
                    }
                },
                "count": 1,
                "foo": "two",
                "push": [3],
                "sum": 3
            },
            {
                "_id": {"start": ISODate("2024-05-01T07:00:00Z"), "customerId": "foo"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T07:00:00Z"),
                        "end": ISODate("2024-05-02T07:00:00Z")
                    }
                },
                "count": 2,
                "foo": "two",
                "push": [1, 9],
                "sum": 10
            },
            {
                "_id": {"start": ISODate("2024-05-01T07:00:00Z"), "customerId": "bob"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T07:00:00Z"),
                        "end": ISODate("2024-05-02T07:00:00Z")
                    }
                },
                "count": 1,
                "foo": "two",
                "push": [2],
                "sum": 2
            },
            {
                "_id": {"start": ISODate("2024-05-01T07:00:00Z"), "customerId": "bat"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T07:00:00Z"),
                        "end": ISODate("2024-05-02T07:00:00Z")
                    }
                },
                "count": 2,
                "foo": "two",
                "push": [4, 5],
                "sum": 9
            },
            {
                "_id": {"start": ISODate("2024-05-01T08:00:00Z"), "customerId": "baz"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T08:00:00Z"),
                        "end": ISODate("2024-05-02T08:00:00Z")
                    }
                },
                "count": 1,
                "foo": "two",
                "push": [3],
                "sum": 3
            },
            {
                "_id": {"start": ISODate("2024-05-01T08:00:00Z"), "customerId": "bob"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T08:00:00Z"),
                        "end": ISODate("2024-05-02T08:00:00Z")
                    }
                },
                "count": 1,
                "foo": "two",
                "push": [2],
                "sum": 2
            },
            {
                "_id": {"start": ISODate("2024-05-01T08:00:00Z"), "customerId": "foo"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T08:00:00Z"),
                        "end": ISODate("2024-05-02T08:00:00Z")
                    }
                },
                "count": 2,
                "foo": "two",
                "push": [1, 9],
                "sum": 10
            },
            {
                "_id": {"start": ISODate("2024-05-01T08:00:00Z"), "customerId": "bat"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T08:00:00Z"),
                        "end": ISODate("2024-05-02T08:00:00Z")
                    }
                },
                "count": 2,
                "foo": "two",
                "push": [4, 5],
                "sum": 9
            },
            {
                "_id": {"start": ISODate("2024-05-01T09:00:00Z"), "customerId": "baz"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T09:00:00Z"),
                        "end": ISODate("2024-05-02T09:00:00Z")
                    }
                },
                "count": 1,
                "foo": "two",
                "push": [3],
                "sum": 3
            },
            {
                "_id": {"start": ISODate("2024-05-01T09:00:00Z"), "customerId": "bob"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T09:00:00Z"),
                        "end": ISODate("2024-05-02T09:00:00Z")
                    }
                },
                "count": 1,
                "foo": "two",
                "push": [2],
                "sum": 2
            },
            {
                "_id": {"start": ISODate("2024-05-01T09:00:00Z"), "customerId": "foo"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T09:00:00Z"),
                        "end": ISODate("2024-05-02T09:00:00Z")
                    }
                },
                "count": 2,
                "foo": "two",
                "push": [1, 9],
                "sum": 10
            },
            {
                "_id": {"start": ISODate("2024-05-01T09:00:00Z"), "customerId": "bat"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T09:00:00Z"),
                        "end": ISODate("2024-05-02T09:00:00Z")
                    }
                },
                "count": 2,
                "foo": "two",
                "push": [4, 5],
                "sum": 9
            },
            {
                "_id": {"start": ISODate("2024-05-01T10:00:00Z"), "customerId": "baz"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T10:00:00Z"),
                        "end": ISODate("2024-05-02T10:00:00Z")
                    }
                },
                "count": 1,
                "foo": "two",
                "push": [3],
                "sum": 3
            },
            {
                "_id": {"start": ISODate("2024-05-01T10:00:00Z"), "customerId": "foo"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T10:00:00Z"),
                        "end": ISODate("2024-05-02T10:00:00Z")
                    }
                },
                "count": 2,
                "foo": "two",
                "push": [1, 9],
                "sum": 10
            },
            {
                "_id": {"start": ISODate("2024-05-01T10:00:00Z"), "customerId": "bob"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T10:00:00Z"),
                        "end": ISODate("2024-05-02T10:00:00Z")
                    }
                },
                "count": 1,
                "foo": "two",
                "push": [2],
                "sum": 2
            },
            {
                "_id": {"start": ISODate("2024-05-01T10:00:00Z"), "customerId": "bat"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T10:00:00Z"),
                        "end": ISODate("2024-05-02T10:00:00Z")
                    }
                },
                "count": 2,
                "foo": "two",
                "push": [4, 5],
                "sum": 9
            },
            {
                "_id": {"start": ISODate("2024-05-01T11:00:00Z"), "customerId": "baz"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T11:00:00Z"),
                        "end": ISODate("2024-05-02T11:00:00Z")
                    }
                },
                "count": 1,
                "foo": "two",
                "push": [3],
                "sum": 3
            },
            {
                "_id": {"start": ISODate("2024-05-01T11:00:00Z"), "customerId": "bob"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T11:00:00Z"),
                        "end": ISODate("2024-05-02T11:00:00Z")
                    }
                },
                "count": 1,
                "foo": "two",
                "push": [2],
                "sum": 2
            },
            {
                "_id": {"start": ISODate("2024-05-01T11:00:00Z"), "customerId": "foo"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T11:00:00Z"),
                        "end": ISODate("2024-05-02T11:00:00Z")
                    }
                },
                "count": 2,
                "foo": "two",
                "push": [1, 9],
                "sum": 10
            },
            {
                "_id": {"start": ISODate("2024-05-01T11:00:00Z"), "customerId": "bat"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T11:00:00Z"),
                        "end": ISODate("2024-05-02T11:00:00Z")
                    }
                },
                "count": 2,
                "foo": "two",
                "push": [4, 5],
                "sum": 9
            },
            {
                "_id": {"start": ISODate("2024-05-01T12:00:00Z"), "customerId": "baz"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T12:00:00Z"),
                        "end": ISODate("2024-05-02T12:00:00Z")
                    }
                },
                "count": 1,
                "foo": "two",
                "push": [3],
                "sum": 3
            },
            {
                "_id": {"start": ISODate("2024-05-01T12:00:00Z"), "customerId": "bob"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T12:00:00Z"),
                        "end": ISODate("2024-05-02T12:00:00Z")
                    }
                },
                "count": 1,
                "foo": "two",
                "push": [2],
                "sum": 2
            },
            {
                "_id": {"start": ISODate("2024-05-01T12:00:00Z"), "customerId": "foo"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T12:00:00Z"),
                        "end": ISODate("2024-05-02T12:00:00Z")
                    }
                },
                "count": 2,
                "foo": "two",
                "push": [1, 9],
                "sum": 10
            },
            {
                "_id": {"start": ISODate("2024-05-01T12:00:00Z"), "customerId": "bat"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T12:00:00Z"),
                        "end": ISODate("2024-05-02T12:00:00Z")
                    }
                },
                "count": 2,
                "foo": "two",
                "push": [4, 5],
                "sum": 9
            },
            {
                "_id": {"start": ISODate("2024-05-01T13:00:00Z"), "customerId": "foo"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T13:00:00Z"),
                        "end": ISODate("2024-05-02T13:00:00Z")
                    }
                },
                "count": 2,
                "foo": "two",
                "push": [1, 9],
                "sum": 10
            },
            {
                "_id": {"start": ISODate("2024-05-01T13:00:00Z"), "customerId": "bob"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T13:00:00Z"),
                        "end": ISODate("2024-05-02T13:00:00Z")
                    }
                },
                "count": 1,
                "foo": "two",
                "push": [2],
                "sum": 2
            },
            {
                "_id": {"start": ISODate("2024-05-01T13:00:00Z"), "customerId": "baz"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T13:00:00Z"),
                        "end": ISODate("2024-05-02T13:00:00Z")
                    }
                },
                "count": 1,
                "foo": "two",
                "push": [3],
                "sum": 3
            },
            {
                "_id": {"start": ISODate("2024-05-01T13:00:00Z"), "customerId": "bat"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T13:00:00Z"),
                        "end": ISODate("2024-05-02T13:00:00Z")
                    }
                },
                "count": 2,
                "foo": "two",
                "push": [4, 5],
                "sum": 9
            },
            {
                "_id": {"start": ISODate("2024-05-01T14:00:00Z"), "customerId": "bob"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T14:00:00Z"),
                        "end": ISODate("2024-05-02T14:00:00Z")
                    }
                },
                "count": 1,
                "foo": "two",
                "push": [2],
                "sum": 2
            },
            {
                "_id": {"start": ISODate("2024-05-01T14:00:00Z"), "customerId": "foo"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T14:00:00Z"),
                        "end": ISODate("2024-05-02T14:00:00Z")
                    }
                },
                "count": 2,
                "foo": "two",
                "push": [1, 9],
                "sum": 10
            },
            {
                "_id": {"start": ISODate("2024-05-01T14:00:00Z"), "customerId": "bat"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T14:00:00Z"),
                        "end": ISODate("2024-05-02T14:00:00Z")
                    }
                },
                "count": 2,
                "foo": "two",
                "push": [4, 5],
                "sum": 9
            },
            {
                "_id": {"start": ISODate("2024-05-01T14:00:00Z"), "customerId": "baz"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T14:00:00Z"),
                        "end": ISODate("2024-05-02T14:00:00Z")
                    }
                },
                "count": 1,
                "foo": "two",
                "push": [3],
                "sum": 3
            },
            {
                "_id": {"start": ISODate("2024-05-01T15:00:00Z"), "customerId": "baz"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T15:00:00Z"),
                        "end": ISODate("2024-05-02T15:00:00Z")
                    }
                },
                "count": 1,
                "foo": "two",
                "push": [3],
                "sum": 3
            },
            {
                "_id": {"start": ISODate("2024-05-01T15:00:00Z"), "customerId": "foo"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T15:00:00Z"),
                        "end": ISODate("2024-05-02T15:00:00Z")
                    }
                },
                "count": 2,
                "foo": "two",
                "push": [1, 9],
                "sum": 10
            },
            {
                "_id": {"start": ISODate("2024-05-01T15:00:00Z"), "customerId": "bob"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T15:00:00Z"),
                        "end": ISODate("2024-05-02T15:00:00Z")
                    }
                },
                "count": 1,
                "foo": "two",
                "push": [2],
                "sum": 2
            },
            {
                "_id": {"start": ISODate("2024-05-01T15:00:00Z"), "customerId": "bat"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T15:00:00Z"),
                        "end": ISODate("2024-05-02T15:00:00Z")
                    }
                },
                "count": 2,
                "foo": "two",
                "push": [4, 5],
                "sum": 9
            },
            {
                "_id": {"start": ISODate("2024-05-01T16:00:00Z"), "customerId": "foo"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T16:00:00Z"),
                        "end": ISODate("2024-05-02T16:00:00Z")
                    }
                },
                "count": 2,
                "foo": "two",
                "push": [1, 9],
                "sum": 10
            },
            {
                "_id": {"start": ISODate("2024-05-01T16:00:00Z"), "customerId": "bat"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T16:00:00Z"),
                        "end": ISODate("2024-05-02T16:00:00Z")
                    }
                },
                "count": 2,
                "foo": "two",
                "push": [4, 5],
                "sum": 9
            },
            {
                "_id": {"start": ISODate("2024-05-01T16:00:00Z"), "customerId": "baz"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T16:00:00Z"),
                        "end": ISODate("2024-05-02T16:00:00Z")
                    }
                },
                "count": 1,
                "foo": "two",
                "push": [3],
                "sum": 3
            },
            {
                "_id": {"start": ISODate("2024-05-01T16:00:00Z"), "customerId": "bob"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T16:00:00Z"),
                        "end": ISODate("2024-05-02T16:00:00Z")
                    }
                },
                "count": 1,
                "foo": "two",
                "push": [2],
                "sum": 2
            },
            {
                "_id": {"start": ISODate("2024-05-01T17:00:00Z"), "customerId": "baz"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T17:00:00Z"),
                        "end": ISODate("2024-05-02T17:00:00Z")
                    }
                },
                "count": 1,
                "foo": "two",
                "push": [3],
                "sum": 3
            },
            {
                "_id": {"start": ISODate("2024-05-01T17:00:00Z"), "customerId": "foo"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T17:00:00Z"),
                        "end": ISODate("2024-05-02T17:00:00Z")
                    }
                },
                "count": 2,
                "foo": "two",
                "push": [1, 9],
                "sum": 10
            },
            {
                "_id": {"start": ISODate("2024-05-01T17:00:00Z"), "customerId": "bob"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T17:00:00Z"),
                        "end": ISODate("2024-05-02T17:00:00Z")
                    }
                },
                "count": 1,
                "foo": "two",
                "push": [2],
                "sum": 2
            },
            {
                "_id": {"start": ISODate("2024-05-01T17:00:00Z"), "customerId": "bat"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T17:00:00Z"),
                        "end": ISODate("2024-05-02T17:00:00Z")
                    }
                },
                "count": 2,
                "foo": "two",
                "push": [4, 5],
                "sum": 9
            },
            {
                "_id": {"start": ISODate("2024-05-01T18:00:00Z"), "customerId": "baz"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T18:00:00Z"),
                        "end": ISODate("2024-05-02T18:00:00Z")
                    }
                },
                "count": 1,
                "foo": "two",
                "push": [3],
                "sum": 3
            },
            {
                "_id": {"start": ISODate("2024-05-01T18:00:00Z"), "customerId": "bob"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T18:00:00Z"),
                        "end": ISODate("2024-05-02T18:00:00Z")
                    }
                },
                "count": 1,
                "foo": "two",
                "push": [2],
                "sum": 2
            },
            {
                "_id": {"start": ISODate("2024-05-01T18:00:00Z"), "customerId": "foo"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T18:00:00Z"),
                        "end": ISODate("2024-05-02T18:00:00Z")
                    }
                },
                "count": 2,
                "foo": "two",
                "push": [1, 9],
                "sum": 10
            },
            {
                "_id": {"start": ISODate("2024-05-01T18:00:00Z"), "customerId": "bat"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T18:00:00Z"),
                        "end": ISODate("2024-05-02T18:00:00Z")
                    }
                },
                "count": 2,
                "foo": "two",
                "push": [4, 5],
                "sum": 9
            },
            {
                "_id": {"start": ISODate("2024-05-01T19:00:00Z"), "customerId": "foo"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T19:00:00Z"),
                        "end": ISODate("2024-05-02T19:00:00Z")
                    }
                },
                "count": 2,
                "foo": "two",
                "push": [1, 9],
                "sum": 10
            },
            {
                "_id": {"start": ISODate("2024-05-01T19:00:00Z"), "customerId": "bat"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T19:00:00Z"),
                        "end": ISODate("2024-05-02T19:00:00Z")
                    }
                },
                "count": 2,
                "foo": "two",
                "push": [4, 5],
                "sum": 9
            },
            {
                "_id": {"start": ISODate("2024-05-01T19:00:00Z"), "customerId": "bob"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T19:00:00Z"),
                        "end": ISODate("2024-05-02T19:00:00Z")
                    }
                },
                "count": 1,
                "foo": "two",
                "push": [2],
                "sum": 2
            },
            {
                "_id": {"start": ISODate("2024-05-01T19:00:00Z"), "customerId": "baz"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T19:00:00Z"),
                        "end": ISODate("2024-05-02T19:00:00Z")
                    }
                },
                "count": 1,
                "foo": "two",
                "push": [3],
                "sum": 3
            },
            {
                "_id": {"start": ISODate("2024-05-01T20:00:00Z"), "customerId": "bob"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T20:00:00Z"),
                        "end": ISODate("2024-05-02T20:00:00Z")
                    }
                },
                "count": 1,
                "foo": "two",
                "push": [2],
                "sum": 2
            },
            {
                "_id": {"start": ISODate("2024-05-01T20:00:00Z"), "customerId": "foo"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T20:00:00Z"),
                        "end": ISODate("2024-05-02T20:00:00Z")
                    }
                },
                "count": 2,
                "foo": "two",
                "push": [1, 9],
                "sum": 10
            },
            {
                "_id": {"start": ISODate("2024-05-01T20:00:00Z"), "customerId": "bat"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T20:00:00Z"),
                        "end": ISODate("2024-05-02T20:00:00Z")
                    }
                },
                "count": 2,
                "foo": "two",
                "push": [4, 5],
                "sum": 9
            },
            {
                "_id": {"start": ISODate("2024-05-01T20:00:00Z"), "customerId": "baz"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T20:00:00Z"),
                        "end": ISODate("2024-05-02T20:00:00Z")
                    }
                },
                "count": 1,
                "foo": "two",
                "push": [3],
                "sum": 3
            },
            {
                "_id": {"start": ISODate("2024-05-01T21:00:00Z"), "customerId": "foo"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T21:00:00Z"),
                        "end": ISODate("2024-05-02T21:00:00Z")
                    }
                },
                "count": 2,
                "foo": "two",
                "push": [1, 9],
                "sum": 10
            },
            {
                "_id": {"start": ISODate("2024-05-01T21:00:00Z"), "customerId": "bob"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T21:00:00Z"),
                        "end": ISODate("2024-05-02T21:00:00Z")
                    }
                },
                "count": 1,
                "foo": "two",
                "push": [2],
                "sum": 2
            },
            {
                "_id": {"start": ISODate("2024-05-01T21:00:00Z"), "customerId": "baz"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T21:00:00Z"),
                        "end": ISODate("2024-05-02T21:00:00Z")
                    }
                },
                "count": 1,
                "foo": "two",
                "push": [3],
                "sum": 3
            },
            {
                "_id": {"start": ISODate("2024-05-01T21:00:00Z"), "customerId": "bat"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T21:00:00Z"),
                        "end": ISODate("2024-05-02T21:00:00Z")
                    }
                },
                "count": 2,
                "foo": "two",
                "push": [4, 5],
                "sum": 9
            },
            {
                "_id": {"start": ISODate("2024-05-01T22:00:00Z"), "customerId": "baz"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T22:00:00Z"),
                        "end": ISODate("2024-05-02T22:00:00Z")
                    }
                },
                "count": 1,
                "foo": "two",
                "push": [3],
                "sum": 3
            },
            {
                "_id": {"start": ISODate("2024-05-01T22:00:00Z"), "customerId": "foo"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T22:00:00Z"),
                        "end": ISODate("2024-05-02T22:00:00Z")
                    }
                },
                "count": 2,
                "foo": "two",
                "push": [1, 9],
                "sum": 10
            },
            {
                "_id": {"start": ISODate("2024-05-01T22:00:00Z"), "customerId": "bob"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T22:00:00Z"),
                        "end": ISODate("2024-05-02T22:00:00Z")
                    }
                },
                "count": 1,
                "foo": "two",
                "push": [2],
                "sum": 2
            },
            {
                "_id": {"start": ISODate("2024-05-01T22:00:00Z"), "customerId": "bat"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T22:00:00Z"),
                        "end": ISODate("2024-05-02T22:00:00Z")
                    }
                },
                "count": 2,
                "foo": "two",
                "push": [4, 5],
                "sum": 9
            },
            {
                "_id": {"start": ISODate("2024-05-01T23:00:00Z"), "customerId": "baz"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T23:00:00Z"),
                        "end": ISODate("2024-05-02T23:00:00Z")
                    }
                },
                "count": 1,
                "foo": "two",
                "push": [3],
                "sum": 3
            },
            {
                "_id": {"start": ISODate("2024-05-01T23:00:00Z"), "customerId": "foo"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T23:00:00Z"),
                        "end": ISODate("2024-05-02T23:00:00Z")
                    }
                },
                "count": 2,
                "foo": "two",
                "push": [1, 9],
                "sum": 10
            },
            {
                "_id": {"start": ISODate("2024-05-01T23:00:00Z"), "customerId": "bob"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T23:00:00Z"),
                        "end": ISODate("2024-05-02T23:00:00Z")
                    }
                },
                "count": 1,
                "foo": "two",
                "push": [2],
                "sum": 2
            },
            {
                "_id": {"start": ISODate("2024-05-01T23:00:00Z"), "customerId": "bat"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-01T23:00:00Z"),
                        "end": ISODate("2024-05-02T23:00:00Z")
                    }
                },
                "count": 2,
                "foo": "two",
                "push": [4, 5],
                "sum": 9
            },
            {
                "_id": {"start": ISODate("2024-05-02T00:00:00Z"), "customerId": "baz"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-02T00:00:00Z"),
                        "end": ISODate("2024-05-03T00:00:00Z")
                    }
                },
                "count": 1,
                "foo": "two",
                "push": [3],
                "sum": 3
            },
            {
                "_id": {"start": ISODate("2024-05-02T00:00:00Z"), "customerId": "bob"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-02T00:00:00Z"),
                        "end": ISODate("2024-05-03T00:00:00Z")
                    }
                },
                "count": 1,
                "foo": "two",
                "push": [2],
                "sum": 2
            },
            {
                "_id": {"start": ISODate("2024-05-02T00:00:00Z"), "customerId": "foo"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-02T00:00:00Z"),
                        "end": ISODate("2024-05-03T00:00:00Z")
                    }
                },
                "count": 2,
                "foo": "two",
                "push": [1, 9],
                "sum": 10
            },
            {
                "_id": {"start": ISODate("2024-05-02T00:00:00Z"), "customerId": "bat"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-02T00:00:00Z"),
                        "end": ISODate("2024-05-03T00:00:00Z")
                    }
                },
                "count": 2,
                "foo": "two",
                "push": [4, 5],
                "sum": 9
            },
            {
                "_id": {"start": ISODate("2024-05-02T01:00:00Z"), "customerId": "baz"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-02T01:00:00Z"),
                        "end": ISODate("2024-05-03T01:00:00Z")
                    }
                },
                "count": 1,
                "foo": "two",
                "push": [3],
                "sum": 3
            },
            {
                "_id": {"start": ISODate("2024-05-02T01:00:00Z"), "customerId": "bat"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-02T01:00:00Z"),
                        "end": ISODate("2024-05-03T01:00:00Z")
                    }
                },
                "count": 2,
                "foo": "two",
                "push": [4, 5],
                "sum": 9
            },
            {
                "_id": {"start": ISODate("2024-05-02T02:00:00Z"), "customerId": "baz"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-02T02:00:00Z"),
                        "end": ISODate("2024-05-03T02:00:00Z")
                    }
                },
                "count": 1,
                "foo": "two",
                "push": [3],
                "sum": 3
            },
            {
                "_id": {"start": ISODate("2024-05-02T02:00:00Z"), "customerId": "bat"},
                "_stream_meta": {
                    "source": {"type": "atlas"},
                    "window": {
                        "start": ISODate("2024-05-02T02:00:00Z"),
                        "end": ISODate("2024-05-03T02:00:00Z")
                    }
                },
                "count": 2,
                "foo": "two",
                "push": [4, 5],
                "sum": 9
            }
        ],
        afterModifyReplayCount: 7
    },
];

for (const testCase of testCases) {
    jsTestLog(`Running case: ${tojson(testCase)}`);
    testRunner(testCase);
}
