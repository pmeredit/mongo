/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */
import {assertArrayEq} from "jstests/aggregation/extras/utils.js";
import {Streams} from "src/mongo/db/modules/enterprise/jstests/streams/fake_client.js";
import {
    listStreamProcessors,
    TEST_TENANT_ID
} from 'src/mongo/db/modules/enterprise/jstests/streams/utils.js';

function testStreamMeta({
    documents,
    pipeline,
    streamMetaOptionValue,
    expectedSinkResults,
    expectedDlqResults,
}) {
    const uri = 'mongodb://' + db.getMongo().host;
    const dbName = "db";
    const collName = "coll";
    const dlqCollName = "dlq";
    const connectionName = "db1";
    const connectionRegistry = [{name: connectionName, type: 'atlas', options: {uri: uri}}];
    const sp = new Streams(TEST_TENANT_ID, connectionRegistry);
    const coll = db.getSiblingDB(dbName)[collName];
    const dlqColl = db.getSiblingDB(dbName)[dlqCollName];
    coll.drop();
    dlqColl.drop();

    const processorName = "processor";
    const sourceOptions = {
        documents,
        timeField: {$toDate: "$timestamp"},
    };
    if (streamMetaOptionValue !== undefined) {
        sourceOptions.streamMetaFieldName = streamMetaOptionValue;
    }
    const adjustedPipeline = [
        {$source: sourceOptions},
        ...pipeline,
        {$merge: {into: {connectionName: connectionName, db: dbName, coll: collName}}}
    ];
    const processor = sp.createStreamProcessor(processorName, adjustedPipeline);

    const options = {
        dlq: {connectionName: connectionName, db: dbName, coll: dlqCollName},
        featureFlags: {},
    };
    assert.commandWorked(processor.start(options));
    assert.soon(() => {
        return coll.find().itcount() === expectedSinkResults.length &&
            dlqColl.find().itcount() === expectedDlqResults.length;
    });

    // Test sink results.
    const sinkResults = coll.find({}, {_id: 0, _ts: 0}).toArray();
    assertArrayEq({actual: sinkResults, expected: expectedSinkResults});

    // Test DLQ results.
    const dlqResults = dlqColl.find({}, {_id: 0, dlqTime: 0}).toArray();
    for (let dlqResult of expectedDlqResults) {
        dlqResult["processorName"] = processorName;
    }
    assertArrayEq({actual: dlqResults, expected: expectedDlqResults});

    assert.commandWorked(processor.stop());
}

// Test pipeline without stream metadata dependency with metadata field name set to 'undefined'.
testStreamMeta({
    documents: [
        {timestamp: "2024-01-01T00:00:00Z", a: 0},
        {timestamp: "2024-01-01T00:00:01Z", a: 1},
    ],
    pipeline: [
        {$addFields: {b: {$divide: [1, "$a"]}}},
    ],
    expectedSinkResults: [{
        _stream_meta: {
            source: {type: "generated"},
        },
        timestamp: "2024-01-01T00:00:01Z",
        a: 1,
        b: 1,
    }],
    expectedDlqResults: [{
        _stream_meta: {
            source: {type: "generated"},
        },
        errInfo: {
            reason:
                "Failed to process input document in AddFieldsOperator with error: can't $divide by zero"
        },
        doc: {_ts: ISODate("2024-01-01T00:00:00Z"), timestamp: "2024-01-01T00:00:00Z", a: 0},
        operatorName: "AddFieldsOperator"
    }],
});

// Test pipeline without stream metadata dependency with metadata field name set to 'null'.
testStreamMeta({
    documents: [
        {timestamp: "2024-01-01T00:00:00Z", a: 0},
        {timestamp: "2024-01-01T00:00:01Z", a: 1},
    ],
    pipeline: [
        {$addFields: {b: {$divide: [1, "$a"]}}},
    ],
    streamMetaOptionValue: null,
    expectedSinkResults: [{
        _stream_meta: {
            source: {type: "generated"},
        },
        timestamp: "2024-01-01T00:00:01Z",
        a: 1,
        b: 1,
    }],
    expectedDlqResults: [{
        _stream_meta: {
            source: {type: "generated"},
        },
        errInfo: {
            reason:
                "Failed to process input document in AddFieldsOperator with error: can't $divide by zero"
        },
        doc: {_ts: ISODate("2024-01-01T00:00:00Z"), timestamp: "2024-01-01T00:00:00Z", a: 0},
        operatorName: "AddFieldsOperator"
    }],
});

// Test pipeline without stream metadata dependency with metadata field name set to a string.
testStreamMeta({
    documents: [
        {timestamp: "2024-01-01T00:00:00Z", a: 0},
        {timestamp: "2024-01-01T00:00:01Z", a: 1},
    ],
    pipeline: [
        {$addFields: {b: {$divide: [1, "$a"]}}},
    ],
    streamMetaOptionValue: "abc",
    expectedSinkResults: [{
        abc: {
            source: {type: "generated"},
        },
        timestamp: "2024-01-01T00:00:01Z",
        a: 1,
        b: 1,
    }],
    expectedDlqResults: [{
        abc: {
            source: {type: "generated"},
        },
        errInfo: {
            reason:
                "Failed to process input document in AddFieldsOperator with error: can't $divide by zero"
        },
        doc: {_ts: ISODate("2024-01-01T00:00:00Z"), timestamp: "2024-01-01T00:00:00Z", a: 0},
        operatorName: "AddFieldsOperator"
    }],
});

// Test pipeline without stream metadata dependency with metadata field name set to an empty string.
testStreamMeta({
    documents: [
        {timestamp: "2024-01-01T00:00:00Z", a: 0},
        {timestamp: "2024-01-01T00:00:01Z", a: 1},
    ],
    pipeline: [
        {$addFields: {b: {$divide: [1, "$a"]}}},
    ],
    streamMetaOptionValue: "",
    expectedSinkResults: [{
        timestamp: "2024-01-01T00:00:01Z",
        a: 1,
        b: 1,
    }],
    expectedDlqResults: [{
        errInfo: {
            reason:
                "Failed to process input document in AddFieldsOperator with error: can't $divide by zero"
        },
        doc: {_ts: ISODate("2024-01-01T00:00:00Z"), timestamp: "2024-01-01T00:00:00Z", a: 0},
        operatorName: "AddFieldsOperator"
    }],
});

// Test pipeline with match on stream metadata.
testStreamMeta({
    documents: [
        {timestamp: "2024-01-01T00:00:00Z"},
        {timestamp: "2024-01-01T00:00:01Z"},
    ],
    pipeline: [
        {$match: {"_stream_meta.source.type": {$ne: "generated"}}},
    ],
    expectedSinkResults: [],
    expectedDlqResults: [],
});

// Test pipeline with projection reading stream metadata.
testStreamMeta({
    documents: [
        {timestamp: "2024-01-01T00:00:00Z"},
    ],
    pipeline: [
        {
            $addFields: {
                x: "$_stream_meta.source.type",
            }
        },
    ],
    expectedSinkResults: [{
        _stream_meta: {
            source: {type: "generated"},
        },
        timestamp: "2024-01-01T00:00:00Z",
        x: "generated",
    }],
    expectedDlqResults: [],
});

// Test pipeline with projection reading entire document.
testStreamMeta({
    documents: [
        {timestamp: "2024-01-01T00:00:00Z"},
    ],
    pipeline: [
        {
            $addFields: {
                x: "$$ROOT",
            }
        },
    ],
    expectedSinkResults: [{
        _stream_meta: {
            source: {type: "generated"},
        },
        timestamp: "2024-01-01T00:00:00Z",
        x: {
            _stream_meta: {
                source: {type: "generated"},
            },
            _ts: ISODate("2024-01-01T00:00:00Z"),
            timestamp: "2024-01-01T00:00:00Z",
        }
    }],
    expectedDlqResults: [],
});

// Test pipeline with projection writing stream metadata.
testStreamMeta({
    documents: [
        {timestamp: "2024-01-01T00:00:00Z"},
    ],
    pipeline: [
        {
            $addFields: {
                "_stream_meta.x": "$_stream_meta.source.type",
            }
        },
    ],
    expectedSinkResults: [{
        _stream_meta: {
            source: {type: "generated"},
            x: "generated",
        },
        timestamp: "2024-01-01T00:00:00Z",
    }],
    expectedDlqResults: [],
});

// Test pipeline with projection removing stream metadata.
testStreamMeta({
    documents: [
        {timestamp: "2024-01-01T00:00:00Z"},
    ],
    pipeline: [
        {$project: {_stream_meta: 0}},
    ],
    expectedSinkResults: [{
        timestamp: "2024-01-01T00:00:00Z",
    }],
    expectedDlqResults: [],
});

// Test pipeline with group in window stage reading stream metadata.
testStreamMeta({
    documents: [
        {timestamp: "2024-01-01T00:00:00Z", group: 0},
        {timestamp: "2024-01-01T00:00:01Z", group: 0},
        {timestamp: "2024-01-01T00:00:02Z", group: 0},
        {timestamp: "2024-01-01T00:00:03Z", group: 0},
    ],
    pipeline: [
        {
            $tumblingWindow: {
                interval: {size: NumberInt(2), unit: "second"},
                allowedLateness: NumberInt(0),
                pipeline: [
                    {
                        $group: {
                            _id: "$group",
                            avg: {$avg: {$toLong: "$_stream_meta.window.start"}},
                        }
                    },
                    {$addFields: {avg: {$toDate: "$avg"}}}
                ]
            }
        },
    ],
    expectedSinkResults: [{
        _stream_meta: {
            source: {type: "generated"},
            window: {
                start: ISODate("2024-01-01T00:00:00Z"),
                end: ISODate("2024-01-01T00:00:02Z"),
            }
        },
        avg: ISODate("2024-01-01T00:00:00Z"),
    }],
    expectedDlqResults: [],
});

// Test pipeline with projection in window stage reading stream metadata.
testStreamMeta({
    documents: [
        {timestamp: "2024-01-01T00:00:00Z"},
    ],
    pipeline: [
        {
            $tumblingWindow: {
                interval: {size: NumberInt(2), unit: "second"},
                allowedLateness: NumberInt(0),
                pipeline: [{$addFields: {x: "$_stream_meta.window.start"}}]
            }
        },
    ],
    expectedSinkResults: [{
        _stream_meta: {
            source: {type: "generated"},
            window: {
                start: ISODate("2024-01-01T00:00:00Z"),
                end: ISODate("2024-01-01T00:00:02Z"),
            }
        },
        timestamp: "2024-01-01T00:00:00Z",
        x: ISODate("2024-01-01T00:00:00Z"),
    }],
    expectedDlqResults: [],
});

// Test pipeline with projection and sort in window stage reading stream metadata.
testStreamMeta({
    documents: [
        {timestamp: "2024-01-01T00:00:00Z"},
        {timestamp: "2024-01-01T00:00:02Z"},
    ],
    pipeline: [
        {
            $tumblingWindow: {
                interval: {size: NumberInt(1), unit: "second"},
                allowedLateness: NumberInt(0),
                pipeline: [{$addFields: {x: "$_stream_meta.window.start"}}, {$sort: {x: 1}}]
            }
        },
    ],
    expectedSinkResults: [{
        _stream_meta: {
            source: {type: "generated"},
            window: {
                start: ISODate("2024-01-01T00:00:00Z"),
                end: ISODate("2024-01-01T00:00:01Z"),
            }
        },
        timestamp: "2024-01-01T00:00:00Z",
        x: ISODate("2024-01-01T00:00:00Z"),
    }],
    expectedDlqResults: [],
});

// Test pipeline with projection after window stage having group reading stream metadata.
testStreamMeta({
    documents: [
        {timestamp: "2024-01-01T00:00:00Z", group: 0},
        {timestamp: "2024-01-01T00:00:01Z", group: 0},
        {timestamp: "2024-01-01T00:00:02Z", group: 0},
        {timestamp: "2024-01-01T00:00:03Z", group: 0},
    ],
    pipeline: [
        {
            $tumblingWindow: {
                interval: {size: NumberInt(2), unit: "second"},
                allowedLateness: NumberInt(0),
                pipeline: [{
                    $group: {
                        _id: "$group",
                        avg: {$avg: {$toLong: "$_stream_meta.window.start"}},
                    }
                }]
            }
        },
        {$addFields: {avg: {$toDate: "$avg"}}},
    ],
    expectedSinkResults: [{
        _stream_meta: {
            source: {type: "generated"},
            window: {
                start: ISODate("2024-01-01T00:00:00Z"),
                end: ISODate("2024-01-01T00:00:02Z"),
            }
        },
        avg: ISODate("2024-01-01T00:00:00Z"),
    }],
    expectedDlqResults: [],
});

// Test pipeline with projection after window stage having sort reading stream metadata.
testStreamMeta({
    documents: [
        {timestamp: "2024-01-01T00:00:00Z", s: 1},
        {timestamp: "2024-01-01T00:00:01Z", s: 2},
        {timestamp: "2024-01-01T00:00:02Z", s: 3},
        {timestamp: "2024-01-01T00:00:03Z", s: 4},
    ],
    pipeline: [
        {
            $tumblingWindow: {
                interval: {size: NumberInt(2), unit: "second"},
                allowedLateness: NumberInt(0),
                pipeline: [{$sort: {s: 1}}]
            }
        },
        {$addFields: {x: "$_stream_meta.window.start"}}
    ],
    expectedSinkResults: [
        {
            _stream_meta: {
                source: {type: "generated"},
                window: {
                    start: ISODate("2024-01-01T00:00:00Z"),
                    end: ISODate("2024-01-01T00:00:02Z"),
                }
            },
            s: 1,
            timestamp: "2024-01-01T00:00:00Z",
            x: ISODate("2024-01-01T00:00:00Z"),
        },
        {
            _stream_meta: {
                source: {type: "generated"},
                window: {
                    start: ISODate("2024-01-01T00:00:00Z"),
                    end: ISODate("2024-01-01T00:00:02Z"),
                }
            },
            s: 2,
            timestamp: "2024-01-01T00:00:01Z",
            x: ISODate("2024-01-01T00:00:00Z"),
        }
    ],
    expectedDlqResults: [],
});

// Test pipeline with projection removing stream metadata in the window pipeline.
testStreamMeta({
    documents: [
        {timestamp: "2024-01-01T00:00:00Z", group: 0, s: 1},
        {timestamp: "2024-01-01T00:00:01Z", group: 0, s: 2},
        {timestamp: "2024-01-01T00:00:02Z", group: 0, s: 3},
        {timestamp: "2024-01-01T00:00:03Z", group: 0, s: 4},
    ],
    pipeline: [
        {
            $tumblingWindow: {
                interval: {size: NumberInt(2), unit: "second"},
                allowedLateness: NumberInt(0),
                pipeline: [
                    {
                        $group: {
                            _id: "$group",
                            avg: {$avg: "$s"},
                        }
                    },
                    {$project: {_stream_meta: 0}},
                    {$sort: {s: 1}}
                ]
            }
        },
    ],
    expectedSinkResults: [
        {avg: 1.5},
    ],
    expectedDlqResults: [],
});

// Test pipeline does not overwrite user metadata object.
testStreamMeta({
    documents: [
        {timestamp: "2024-01-01T00:00:00Z", _stream_meta: {x: 0}},
    ],
    pipeline: [],
    expectedSinkResults: [{
        _stream_meta: {
            source: {type: "generated"},
            x: 0,
        },
        timestamp: "2024-01-01T00:00:00Z",
    }],
    expectedDlqResults: [],
});

// Test pipeline does not overwrite non-object user metadata.
testStreamMeta({
    documents: [
        {timestamp: "2024-01-01T00:00:00Z", _stream_meta: 0},
    ],
    pipeline: [],
    expectedSinkResults: [{
        _stream_meta: {
            source: {type: "generated"},
        },
        timestamp: "2024-01-01T00:00:00Z",
    }],
    expectedDlqResults: [],
});

// Test pipeline does not overwrite user metadata in the middle.
testStreamMeta({
    documents: [
        {timestamp: "2024-01-01T00:00:00Z"},
        {timestamp: "2024-01-01T00:00:02Z"},
    ],
    pipeline: [
        {$addFields: {"_stream_meta.x": 0}},
        {
            $tumblingWindow: {
                interval: {size: NumberInt(1), unit: "second"},
                allowedLateness: NumberInt(0),
                pipeline: [{$sort: {x: 1}}]
            }
        },
    ],
    expectedSinkResults: [{
        _stream_meta: {
            source: {type: "generated"},
            x: 0,
            window: {
                start: ISODate("2024-01-01T00:00:00Z"),
                end: ISODate("2024-01-01T00:00:01Z"),
            }
        },
        timestamp: "2024-01-01T00:00:00Z",
    }],
    expectedDlqResults: [],
});

assert.eq(listStreamProcessors()["streamProcessors"].length, 0);