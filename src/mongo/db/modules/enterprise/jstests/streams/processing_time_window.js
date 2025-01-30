/**
 * Tests for processing time windows
 *
 * @tags: [
 *  featureFlagStreams,
 * ]
 */
import {resultsEq} from "jstests/aggregation/extras/utils.js";
import {Streams} from "src/mongo/db/modules/enterprise/jstests/streams/fake_client.js";
import {
    getStats,
    listStreamProcessors,
    sampleUntil,
    stopStreamProcessor,
    TEST_TENANT_ID,
} from 'src/mongo/db/modules/enterprise/jstests/streams/utils.js';

const inputColl = db.input_coll;
const dlqColl = db.dlq_coll;
const outputDB = "outputDB";
const outputCollName = "outputColl";
const outputColl = db.getSiblingDB(outputDB)[outputCollName];
outputColl.drop();
const uri = 'mongodb://' + db.getMongo().host;
const connName = "db1";

const connectionRegistry = [{name: connName, type: 'atlas', options: {uri: uri}}];
const sp = new Streams(TEST_TENANT_ID, connectionRegistry);
const changeStreamSourceSpec = [
    {
        $source: {
            connectionName: connName,
            db: "test",
            coll: "input_coll",
            'timeField': {$toDate: '$fullDocument.ts'}
        }
    },
    {$match: {operationType: "insert"}}
];
const emitSpec = {
    $merge: {into: {connectionName: connName, db: "outputDB", coll: outputCollName}}
};
const fieldsToSkip = ["_id", "clusterTime", "wallTime", "_ts", "_stream_meta", "documentKey"];

function testIdleMessagesCloseWindows(sourceSpec, windowSpec, expectedOutput) {
    inputColl.drop();
    outputColl.drop();
    const processorName = "idleMessageProcessor";
    sp.createStreamProcessor(processorName, [...sourceSpec, windowSpec, emitSpec]);
    const processor = sp[processorName];
    const startResult =
        processor.start({featureFlags: {processingTimeWindows: true}, shouldStartSample: true});
    const cursorId = startResult["sampleCursorId"];
    assert.commandWorked(startResult);
    inputColl.insertOne({a: 1});
    assert.soon(() => {
        let stats = getStats(processorName);
        return stats["outputMessageCount"] == 1;
    });

    const firstWindowOutput = sampleUntil(cursorId, 1, processorName);
    assert(resultsEq(expectedOutput, firstWindowOutput, true, fieldsToSkip));
    stopStreamProcessor(processorName);
}

function testWindowsClosingAsExpected(sourceSpec, windowSpec, windowIntervalMillis) {
    inputColl.drop();
    outputColl.drop();
    const processorName = "windowsClosingAsExpectedProcessor";
    sp.createStreamProcessor(processorName, [...sourceSpec, windowSpec, emitSpec]);
    const processor = sp[processorName];
    const startResult =
        processor.start({featureFlags: {processingTimeWindows: true}, shouldStartSample: true});
    const cursorId = startResult["sampleCursorId"];
    assert.commandWorked(startResult);

    const stats = getStats(processorName);
    assert(stats["outputMessageCount"] == 0);
    const startTime = new Date();
    inputColl.insertOne({a: 1});

    // Make sure next document inserted closes the current window
    sleep(windowIntervalMillis);

    assert.soon(() => {
        const outputMessageCount = getStats(processorName)["outputMessageCount"];
        if (outputMessageCount < 2) {
            inputColl.insertOne({a: 1});
        }
        return outputMessageCount >= 2;
    }, "Failed to close window");

    const fieldsToSkip = ["_id", "clusterTime", "wallTime", "_ts", "_stream_meta", "documentKey"];

    const outputDocFirstWindow = sampleUntil(cursorId, 1, processorName);
    const expectedOutput = [{
        "operationType": "insert",
        "fullDocument": {"a": 1},
        "ns": {"db": "test", "coll": "input_coll"},
    }];
    assert(resultsEq(expectedOutput, [outputDocFirstWindow[0]], true, fieldsToSkip));
    const firstWindowStartTime =
        outputDocFirstWindow[0]["_stream_meta"]["window"]["start"].getTime();
    const firstWindowEndTime = outputDocFirstWindow[0]["_stream_meta"]["window"]["end"].getTime();
    assert(Math.abs(startTime.getTime() - firstWindowStartTime) <= 5000);
    assert(Math.abs(startTime.getTime() - firstWindowEndTime) <= 5000);
    const verboseStats = getStats(processorName);
    assert(Math.abs(startTime.getTime() - new Date(verboseStats["watermark"]).getTime()) <= 5000);

    const outputDocSecondWindow = sampleUntil(cursorId, 1, processorName);
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
                           expectedOutput) {
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
    inputColl.insert({a: 3});

    // Make sure the window closes
    sleep(windowIntervalMillis);
    inputColl.insert({a: 45});
    const firstWindowOutput = sampleUntil(cursorId, 1, processorName);
    assert(resultsEq(expectedOutput, firstWindowOutput, true, fieldsToSkip));

    const secondWindowOutput = sampleUntil(cursorId, 1, processorName);
    assert(resultsEq(expectedOutput, secondWindowOutput, true, fieldsToSkip));

    assert.soon(() => {
        const stats = getStats(processorName);
        return stats["dlqMessageCount"] == numExpectedDLQMessages &&
            stats["outputMessageCount"] == numExpectedOutputDocuments;
    });
    stopStreamProcessor(processorName);
}

// TODO(SERVER-99832): Add test for processing time windows closing as expected with checkpoints

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
                             }]);
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
                             }]);
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
                             }]);
testWindowsClosingAsExpected(changeStreamSourceSpec,
                             {
                                 $tumblingWindow: {
                                     boundary: "processingTime",
                                     interval: {size: NumberInt(1), unit: "second"},
                                     pipeline: [{$sort: {_id: 1}}]
                                 }
                             },
                             1000);
testWindowsClosingAsExpected(changeStreamSourceSpec,
                             {
                                 $hoppingWindow: {
                                     boundary: "processingTime",
                                     interval: {size: NumberInt(1), unit: "second"},
                                     hopSize: {size: NumberInt(1), unit: "second"},
                                     pipeline: [{$sort: {_id: 1}}]
                                 }
                             },
                             1000);
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
                  }]);
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
                  }]);
assert.eq(listStreamProcessors()["streamProcessors"].length, 0);
