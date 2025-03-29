/**
 * Tests for processing time windows
 *
 * @tags: [
 *  featureFlagStreams,
 * ]
 */
import {resultsEq} from "jstests/aggregation/extras/utils.js";
import {
    commonTest,
} from "src/mongo/db/modules/enterprise/jstests/streams/common_test.js";
import {Streams} from "src/mongo/db/modules/enterprise/jstests/streams/fake_client.js";
import {
    getStats,
    listStreamProcessors,
    sampleUntil,
    stopStreamProcessor,
    TEST_TENANT_ID,
} from 'src/mongo/db/modules/enterprise/jstests/streams/utils.js';

Random.setRandomSeed(20250131);

const inputColl = db.input_coll;
const dlqColl = db.dlq_coll;
const outputDB = "outputDB";
const outputCollName = "outputColl";
const outputColl = db.getSiblingDB(outputDB)[outputCollName];
outputColl.drop();
const uri = 'mongodb://' + db.getMongo().host;
const connName = "db1";

const connectionRegistry = [
    {name: connName, type: 'atlas', options: {uri: uri}},
    {name: 'kafka', type: 'kafka', options: {bootstrapServers: 'localhost:9092', isTestKafka: true}}
];
const sp = new Streams(TEST_TENANT_ID, connectionRegistry);
const changeStreamSourceSpec = [
    {
        $source: {
            connectionName: connName,
            db: "test",
            coll: "input_coll",
        }
    },
    {$match: {operationType: "insert"}}
];
const kafkaSourceSpec = [{
    $source: {
        connectionName: 'kafka',
        topic: 'topic',
        testOnlyPartitionCount: NumberInt(1),
    }
}];
const emitSpec = {
    $merge: {into: {connectionName: connName, db: "outputDB", coll: outputCollName}}
};
const fieldsToSkip =
    ["_id", "clusterTime", "wallTime", "_ts", "_stream_meta", "documentKey", "collectionUUID"];

function testCheckpointing() {
    commonTest({
        input: [{a: 1}, {a: 1}],
        pipeline: [
            {
                $tumblingWindow: {
                    boundary: "processingTime",
                    interval: {size: NumberInt(10), unit: "second"},
                    pipeline: [{$group: {_id: "$a", push: {$push: "$$ROOT"}}}]
                },
            },
            {$unwind: "$push"},
            {$project: {_id: 0}}
        ],
        expectedOutput: [{push: {a: 1}}, {push: {a: 1}}],
        featureFlags: {processingTimeWindows: true},
        fieldsToSkip: fieldsToSkip,
        useTimeField: false,
        useKafka: true
    });
    commonTest({
        input: [{a: 1}, {a: 1}],
        pipeline: [
            {
                $hoppingWindow: {
                    boundary: "processingTime",
                    interval: {size: NumberInt(1), unit: "second"},
                    hopSize: {size: NumberInt(1), unit: "second"},
                    pipeline: [{$group: {_id: "$a", push: {$push: "$$ROOT"}}}]
                }
            },
            {$unwind: "$push"},
            {$project: {_id: 0}}
        ],
        expectedOutput: [{push: {a: 1}}, {push: {a: 1}}],
        featureFlags: {processingTimeWindows: true},
        fieldsToSkip: fieldsToSkip,
        useTimeField: false,
        useKafka: true
    });
    commonTest({
        input: [{a: 1, b: 2}, {a: 2, b: 3}],
        pipeline: [{
            $sessionWindow: {
                boundary: "processingTime",
                gap: {size: NumberInt(5), unit: "second"},
                partitionBy: "$a",
                pipeline:
                    [{$sort: {b: 1}}, {$group: {_id: "$a", push: {$push: {a: "$a", b: "$b"}}}}]
            }
        }],
        expectedOutput: [{push: [{a: 1, b: 2}]}, {push: [{a: 2, b: 3}]}],
        featureFlags: {processingTimeWindows: true},
        fieldsToSkip: fieldsToSkip,
        useTimeField: false,
        useKafka: true
    });
}

function testIdleMessagesCloseWindows(sourceSpec, windowSpec, expectedOutput, isKafka) {
    inputColl.drop();
    outputColl.drop();
    const processorName = "idleMessageProcessor";
    sp.createStreamProcessor(processorName, [...sourceSpec, windowSpec, emitSpec]);
    const processor = sp[processorName];
    const startResult =
        processor.start({featureFlags: {processingTimeWindows: true}, shouldStartSample: true});
    const cursorId = startResult["sampleCursorId"];
    assert.commandWorked(startResult);
    if (isKafka) {
        processor.testInsert({a: 1});
    } else {
        inputColl.insertOne({a: 1});
    }
    assert.soon(() => {
        let stats = getStats(processorName);
        return stats["outputMessageCount"] == 1;
    });

    const firstWindowOutput = sampleUntil(cursorId, 1, processorName);
    assert(resultsEq(expectedOutput, firstWindowOutput, true, fieldsToSkip));
    stopStreamProcessor(processorName);
}

function testWindowsClosingAsExpected(
    sourceSpec, windowSpec, expectedOutput, windowIntervalMillis, isKafka) {
    inputColl.drop();
    outputColl.drop();
    const processorName = "windowsClosingAsExpectedProcessor";
    sp.createStreamProcessor(
        processorName,
        [...sourceSpec, windowSpec, {$addFields: {_stream_meta: {$meta: "stream"}}}, emitSpec]);
    const processor = sp[processorName];
    const startResult =
        processor.start({featureFlags: {processingTimeWindows: true}, shouldStartSample: true});
    const cursorId = startResult["sampleCursorId"];
    assert.commandWorked(startResult);

    const stats = getStats(processorName);
    assert(stats["outputMessageCount"] == 0);
    const startTime = new Date();
    if (isKafka) {
        processor.testInsert({a: 1});
    } else {
        inputColl.insertOne({a: 1});
    }

    // Make sure next document inserted closes the current window
    sleep(windowIntervalMillis);
    if (isKafka) {
        processor.testInsert({a: 1});
    } else {
        inputColl.insertOne({a: 1});
    }

    assert.soon(() => {
        const stats = getStats(processorName);
        return stats["outputMessageCount"] == 2;
    }, "Failed to close window");

    const outputDocFirstWindow = sampleUntil(cursorId, 2, processorName);

    assert(resultsEq(expectedOutput, [outputDocFirstWindow[0]], true, fieldsToSkip));
    const firstWindowStartTime =
        outputDocFirstWindow[0]["_stream_meta"]["window"]["start"].getTime();
    const firstWindowEndTime = outputDocFirstWindow[0]["_stream_meta"]["window"]["end"].getTime();
    assert(Math.abs(startTime.getTime() - firstWindowStartTime) <= 5000);
    assert(Math.abs(startTime.getTime() - firstWindowEndTime) <= 5000);
    let outputDocSecondWindow = [outputDocFirstWindow[1]];
    assert(resultsEq(expectedOutput, [outputDocSecondWindow[0]], true, fieldsToSkip));

    const secondWindowStartTime =
        outputDocSecondWindow[0]["_stream_meta"]["window"]["start"].getTime();
    const secondWindowEndTime = outputDocSecondWindow[0]["_stream_meta"]["window"]["end"].getTime();
    assert(secondWindowStartTime > firstWindowStartTime);
    assert(secondWindowEndTime > firstWindowEndTime);

    stopStreamProcessor(processorName);
}

function testDLQedMessages(sourceSpec,
                           windowSpec,
                           numExpectedDLQMessages,
                           numExpectedOutputDocuments,
                           windowIntervalMillis,
                           expectedOutput,
                           isKafka) {
    inputColl.drop();
    dlqColl.drop();
    outputColl.drop();
    const processorName = "dlqProcessor";
    sp.createStreamProcessor(processorName, [...sourceSpec, windowSpec, emitSpec]);
    const processor = sp[processorName];
    const startResult = processor.start({
        dlq: {connectionName: connName, db: "test", coll: dlqColl.getName()},
        featureFlags: {processingTimeWindows: true},
        shouldStartSample: true
    });
    const cursorId = startResult["sampleCursorId"];
    assert.commandWorked(startResult);
    if (isKafka) {
        processor.testInsert({a: 3});
    } else {
        inputColl.insert({a: 3});
    }

    // Make sure the window closes
    sleep(windowIntervalMillis);
    if (isKafka) {
        processor.testInsert({a: 45});
    } else {
        inputColl.insert({a: 45});
    }
    assert.soon(() => {
        let stats = getStats(processorName);
        return stats["outputMessageCount"] == 2;
    });
    const output = sampleUntil(cursorId, 2, processorName);
    let firstWindowOutput = [output[0]];
    assert(resultsEq(expectedOutput, firstWindowOutput, true, fieldsToSkip));

    let secondWindowOutput = [output[1]];
    assert(resultsEq(expectedOutput, secondWindowOutput, true, fieldsToSkip));

    assert.soon(() => {
        const stats = getStats(processorName);
        return stats["dlqMessageCount"] == numExpectedDLQMessages &&
            stats["outputMessageCount"] == numExpectedOutputDocuments;
    });
    stopStreamProcessor(processorName);
}
testIdleMessagesCloseWindows(changeStreamSourceSpec,
                             {
                                 $tumblingWindow: {
                                     boundary: "processingTime",
                                     interval: {size: NumberInt(1), unit: "second"},
                                     pipeline: [{$group: {_id: "$a", count: {$sum: 1}}}]
                                 }
                             },
                             [{
                                 "count": 1,
                             }],
                             false);
testIdleMessagesCloseWindows(changeStreamSourceSpec,
                             {
                                 $tumblingWindow: {
                                     boundary: "processingTime",
                                     interval: {size: NumberInt(1), unit: "second"},
                                     pipeline: [{$sort: {_id: 1}}]
                                 }
                             },
                             [{
                                 "operationType": "insert",
                                 "fullDocument": {"a": 1},
                                 "ns": {"db": "test", "coll": "input_coll"},
                             }],
                             false);
testIdleMessagesCloseWindows(changeStreamSourceSpec,
                             {
                                 $hoppingWindow: {
                                     boundary: "processingTime",
                                     interval: {size: NumberInt(1), unit: "second"},
                                     hopSize: {size: NumberInt(1), unit: "second"},
                                     pipeline: [{$sort: {_id: 1}}]
                                 }
                             },
                             [{
                                 "operationType": "insert",
                                 "fullDocument": {"a": 1},
                                 "ns": {"db": "test", "coll": "input_coll"},
                             }],
                             false);
testIdleMessagesCloseWindows(changeStreamSourceSpec,
                             {
                                 $sessionWindow: {
                                     boundary: "processingTime",
                                     gap: {size: NumberInt(5), unit: "second"},
                                     partitionBy: "$a",
                                     pipeline: [{$sort: {a: 1}}]
                                 }
                             },
                             [{
                                 "operationType": "insert",
                                 "fullDocument": {"a": 1},
                                 "ns": {"db": "test", "coll": "input_coll"},
                             }],
                             false);
testIdleMessagesCloseWindows(changeStreamSourceSpec,
                             {
                                 $sessionWindow: {
                                     boundary: "processingTime",
                                     gap: {size: NumberInt(1), unit: "second"},
                                     partitionBy: "$a",
                                     pipeline: [{$group: {_id: "$a", count: {$sum: 1}}}]
                                 }
                             },
                             [{
                                 "count": 1,
                             }],
                             false);
testWindowsClosingAsExpected(changeStreamSourceSpec,
                             {
                                 $tumblingWindow: {
                                     boundary: "processingTime",
                                     interval: {size: NumberInt(1), unit: "second"},
                                     pipeline: [{$sort: {_id: 1}}]
                                 }
                             },
                             [{
                                 "operationType": "insert",
                                 "fullDocument": {"a": 1},
                                 "ns": {"db": "test", "coll": "input_coll"},
                             }],
                             2000,
                             false);
testWindowsClosingAsExpected(changeStreamSourceSpec,
                             {
                                 $hoppingWindow: {
                                     boundary: "processingTime",
                                     interval: {size: NumberInt(1), unit: "second"},
                                     hopSize: {size: NumberInt(1), unit: "second"},
                                     pipeline: [{$sort: {_id: 1}}]
                                 }
                             },
                             [{
                                 "operationType": "insert",
                                 "fullDocument": {"a": 1},
                                 "ns": {"db": "test", "coll": "input_coll"},
                             }],
                             2000,
                             false);
testWindowsClosingAsExpected(changeStreamSourceSpec,
                             {
                                 $sessionWindow: {
                                     boundary: "processingTime",
                                     gap: {size: NumberInt(5), unit: "second"},
                                     partitionBy: "$a",
                                     pipeline: [{$sort: {_id: 1}}]
                                 }
                             },
                             [{
                                 "operationType": "insert",
                                 "fullDocument": {"a": 1},
                                 "ns": {"db": "test", "coll": "input_coll"},
                             }],
                             11000,
                             false);
testDLQedMessages(changeStreamSourceSpec,
                  {
                      $tumblingWindow: {
                          boundary: "processingTime",
                          interval: {size: NumberInt(5), unit: "second"},
                          pipeline: [{$group: {_id: "$a", count: {$sum: 1}}}]
                      }

                  },
                  0,
                  2,
                  5000,
                  [{
                      "count": 1,
                  }],
                  false);
testDLQedMessages(changeStreamSourceSpec,
                  {
                      $hoppingWindow: {
                          boundary: "processingTime",
                          interval: {size: NumberInt(5), unit: "second"},
                          hopSize: {size: NumberInt(5), unit: "second"},
                          pipeline: [{$group: {_id: "$a", count: {$sum: 1}}}]
                      }

                  },
                  0,
                  2,
                  5000,
                  [{
                      "count": 1,
                  }],
                  false);
testDLQedMessages(changeStreamSourceSpec,
                  {
                      $sessionWindow: {
                          boundary: "processingTime",
                          gap: {size: NumberInt(5), unit: "second"},
                          partitionBy: "$a",
                          pipeline: [{$group: {_id: "$a", count: {$sum: 1}}}]
                      }

                  },
                  0,
                  2,
                  11000,
                  [{
                      "count": 1,
                  }],
                  false);
testWindowsClosingAsExpected(kafkaSourceSpec,
                             {
                                 $tumblingWindow: {
                                     boundary: "processingTime",
                                     interval: {size: NumberInt(1), unit: "second"},
                                     pipeline: [{$sort: {_id: 1}}]
                                 }
                             },
                             [{"a": 1}],
                             2000,
                             true);
testWindowsClosingAsExpected(kafkaSourceSpec,
                             {
                                 $hoppingWindow: {
                                     boundary: "processingTime",
                                     interval: {size: NumberInt(1), unit: "second"},
                                     hopSize: {size: NumberInt(1), unit: "second"},
                                     pipeline: [{$sort: {_id: 1}}]
                                 }
                             },
                             [{"a": 1}],
                             2000,
                             true);
testWindowsClosingAsExpected(kafkaSourceSpec,
                             {
                                 $sessionWindow: {
                                     boundary: "processingTime",
                                     gap: {size: NumberInt(1), unit: "second"},
                                     partitionBy: "$a",
                                     pipeline: [{$sort: {_id: 1}}]
                                 }
                             },
                             [{"a": 1}],
                             2000,
                             true);
testDLQedMessages(kafkaSourceSpec,
                  {
                      $hoppingWindow: {
                          boundary: "processingTime",
                          interval: {size: NumberInt(5), unit: "second"},
                          hopSize: {size: NumberInt(5), unit: "second"},
                          pipeline: [{$group: {_id: "$a", count: {$sum: 1}}}]
                      }

                  },
                  0,
                  2,
                  5000,
                  [{
                      "count": 1,
                  }],
                  true);
testDLQedMessages(kafkaSourceSpec,
                  {
                      $tumblingWindow: {
                          boundary: "processingTime",
                          interval: {size: NumberInt(5), unit: "second"},
                          pipeline: [{$group: {_id: "$a", count: {$sum: 1}}}]
                      }

                  },
                  0,
                  2,
                  5000,
                  [{
                      "count": 1,
                  }],
                  true);
testDLQedMessages(kafkaSourceSpec,
                  {
                      $sessionWindow: {
                          boundary: "processingTime",
                          gap: {size: NumberInt(5), unit: "second"},
                          partitionBy: "$a",
                          pipeline: [{$group: {_id: "$a", count: {$sum: 1}}}]
                      }

                  },
                  0,
                  2,
                  5000,
                  [{
                      "count": 1,
                  }],
                  true);
testIdleMessagesCloseWindows(kafkaSourceSpec,
                             {
                                 $tumblingWindow: {
                                     boundary: "processingTime",
                                     interval: {size: NumberInt(1), unit: "second"},
                                     pipeline: [{$group: {_id: "$a", count: {$sum: 1}}}]
                                 }
                             },
                             [{
                                 "count": 1,
                             }],
                             true);
testIdleMessagesCloseWindows(kafkaSourceSpec,
                             {
                                 $tumblingWindow: {
                                     boundary: "processingTime",
                                     interval: {size: NumberInt(1), unit: "second"},
                                     pipeline: [{$sort: {_id: 1}}]
                                 }
                             },
                             [{"a": 1}],
                             true);
testIdleMessagesCloseWindows(kafkaSourceSpec,
                             {
                                 $hoppingWindow: {
                                     boundary: "processingTime",
                                     interval: {size: NumberInt(1), unit: "second"},
                                     hopSize: {size: NumberInt(1), unit: "second"},
                                     pipeline: [{$sort: {_id: 1}}]
                                 }
                             },
                             [{"a": 1}],
                             true);
testIdleMessagesCloseWindows(kafkaSourceSpec,
                             {
                                 $sessionWindow: {
                                     boundary: "processingTime",
                                     gap: {size: NumberInt(1), unit: "second"},
                                     partitionBy: "$a",
                                     pipeline: [{$sort: {_id: 1}}]
                                 }
                             },
                             [{"a": 1}],
                             true);
testCheckpointing();
assert.eq(listStreamProcessors()["streamProcessors"].length, 0);
