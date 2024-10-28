/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */

import {TestHelper} from "src/mongo/db/modules/enterprise/jstests/streams/checkpoint_helper.js";
import {listStreamProcessors} from "src/mongo/db/modules/enterprise/jstests/streams/utils.js";

function getOperatorStats(operatorName, statsOutput) {
    let opStats = statsOutput["operatorStats"];
    for (let i = 0; i < opStats.length; i++) {
        if (opStats[i]["name"] == operatorName) {
            return opStats[i];
        }
    }

    return {};
}

function getMetric(metricName, metrics) {
    let gauges = metrics["gauges"];

    for (let i = 0; i < gauges.length; i++) {
        let gauge = gauges[i];
        if (gauge.name == metricName) {
            return gauge;
        }
    }
    return 0;
}

function queuedSinkOperatorTest(featureFlags = {}) {
    const pipeline = [
        {$replaceRoot: {newRoot: "$fullDocument"}},
    ];
    let maxQueueSize = 1000;
    featureFlags.maxSinkQueueSize = NumberLong(maxQueueSize);

    const batch1 = Array.from({length: 2000}, (_, i) => ({_id: i, ts: i}));
    let test = new TestHelper(batch1,
                              pipeline,
                              999999999 /* interval */,
                              "changestream" /* sourceType */,
                              true /*useNewCheckpointing*/,
                              featureFlags);
    test.run();
    test.stop();
    test.run(false /* first time */);
    // Stop the cunsumer thread from processing message in the queue.
    assert.commandWorked(db.adminCommand(
        {'configureFailPoint': 'queuedSinkStopProcessingData', 'mode': 'alwaysOn'}));

    const batch2 = Array.from({length: 2000}, (_, i) => ({_id: batch1.length + i, ts: i}));
    test.inputColl.insert(batch2);

    let docCount = 5000;
    const batch3 =
        Array.from({length: docCount}, (_, i) => ({_id: batch1.length + batch2.length + i, ts: i}));
    test.inputColl.insert(batch3);

    let totalDocs = batch1.length + batch2.length + batch3.length;

    // Wait for all the messages to be read and the message queue at full capacity.
    let metrics = test.metrics();
    let queueSize = getMetric("sink_operator_queue_size", metrics).value;
    assert.soon(
        () => {
            metrics = test.metrics();
            queueSize = getMetric("sink_operator_queue_size", metrics).value;
            return queueSize >= maxQueueSize && test.inputColl.count() >= totalDocs;
        },
        `Queue size ${queueSize}, InputColl ${test.inputColl.count()}, Output ${
            test.outputColl.count()}`,
        300000,
        10000);

    // Take a forced checkpoint.
    test.checkpoint(true);
    assert.commandWorked(
        db.adminCommand({'configureFailPoint': 'queuedSinkStopProcessingData', 'mode': 'off'}));

    assert.soon(() => { return test.outputColl.count() >= totalDocs; },
                "Didn't get enough documents",
                300000,
                10000);

    jsTestLog("Stopping now");
    test.stop();

    let ids = test.getCheckpointIds();
    assert.eq(ids.length, 4, "expected 4 checkpoints");
}

queuedSinkOperatorTest();
// TODO(SERVER-92447): Remove this.
queuedSinkOperatorTest({useExecutionPlanFromCheckpoint: false});

assert.eq(listStreamProcessors()["streamProcessors"].length, 0);