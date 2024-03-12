/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */

import {TestHelper} from "src/mongo/db/modules/enterprise/jstests/streams/checkpoint_helper.js";

// Tests the logic in checkpoint coordinator around taking a new checkpoint only if something has
// changed. The current definition of "has something changed" is: Since the last checkpoint
// commit, 1) Has any operator in the DAG output docs or dlq'd docs? 2) If input is a changestream
// source, then do we have a new resume token since we last committed
function checkpointCoordinatorTakeCheckpointTest() {
    var numCustomers = 50;
    let baseTs = ISODate("2023-01-01T00:00:00.000Z");
    const pipeline = [
        {$replaceRoot: {newRoot: "$fullDocument"}},
        {
            $project: {
                value: {$range: [1, "$idx"]},
                ts: "$ts",
            }
        },
        {$unwind: "$value"},
        {
            $addFields: {
                "customerId": {$mod: ["$value", numCustomers]},
                "max": "$value",
            }
        },
        {
            $tumblingWindow: {
                interval: {size: NumberInt(15), unit: "second"},
                allowedLateness: {size: NumberInt(0), unit: "second"},
                idleTimeout: {size: NumberInt(10), unit: "second"},
                pipeline: [{
                    $group:
                        {_id: "$customerId", customerDocs: {$push: "$$ROOT"}, max: {$max: "$max"}}
                }]
            }
        },
        {$project: {customerId: "$_id", max: "$max"}},
        {$match: {"max": {$lt: 0}}},
    ];

    var cnt = 1000;
    const inputBeforeStop = [{idx: cnt, ts: baseTs}];

    let test = new TestHelper(inputBeforeStop,
                              pipeline,
                              999999999 /* interval */,
                              "changestream" /* sourceType */,
                              true /*useNewCheckpointing*/);

    test.run();

    assert.eq(test.stats()["operatorStats"][5]["name"], "GroupOperator");

    // Wait for all the messages to be read.
    assert.soon(() => { return test.stats()["inputMessageCount"] == inputBeforeStop.length; },
                "InputMessageCount mismatch",
                180000);
    assert.soon(() => { return test.stats()["operatorStats"][5]["inputMessageCount"] == cnt - 1; });
    assert.eq(0, test.stats()["operatorStats"][5]["outputMessageCount"]);
    assert.eq(0, test.getResults().length, "expected no output");

    assert.eq(test.stats()["operatorStats"][0]["name"], "ChangeStreamConsumerOperator");

    let numChkpts = test.getCheckpointIds().length;
    assert.gt(numChkpts, 0, "expected some checkpoints");

    // We expect a checkpoint to be taken
    test.checkpoint();
    assert.soon(() => { return test.getCheckpointIds().length == numChkpts + 1; });

    // Nothing has changed so normally a checkpoint() request will have no effect.
    // So set force and ensure that a new checkpoint was indeed taken
    test.checkpoint(true);
    assert.soon(() => {
        sleep(2000);
        return test.getCheckpointIds().length == numChkpts + 2;
    });

    // Send an input doc that is dlq'd by the ChangeStreamSourceOperator. It should still be
    // treated as a delta in the state change and subsequently a new checkpoint should be taken
    const inputToBeDlqed = [{idx: 2, ts: "blah"}];

    // Send more input, but such that window does not close.
    var beforeOutCnt = test.stats()["operatorStats"][0].outputMessageCount;
    var beforeDlqCnt = test.stats()["operatorStats"][0].dlqMessageCount;
    assert.commandWorked(test.inputColl.insertMany(inputToBeDlqed));
    assert.soon(
        () => { return test.stats()["operatorStats"][0].dlqMessageCount == beforeDlqCnt + 1; });
    assert.eq(test.stats()["operatorStats"][0].outputMessageCount, beforeOutCnt);

    // DLQ cnt has gone up, so we expect another checkpoint to be taken
    test.checkpoint();
    assert.soon(() => { return test.getCheckpointIds().length == numChkpts + 3; });

    // Send more input, but such that window does not close.
    beforeOutCnt = test.stats()["operatorStats"][0].outputMessageCount;
    assert.commandWorked(test.inputColl.insertMany(inputBeforeStop));
    assert.soon(
        () => { return test.stats()["operatorStats"][0].outputMessageCount == beforeOutCnt + 1; });

    // we expect another checkpoint to be taken
    test.checkpoint();
    assert.soon(() => { return test.getCheckpointIds().length == numChkpts + 4; });

    // Wait for idleTimeout to close window,
    jsTestLog("Waiting for idleTimeout induced window close");
    beforeOutCnt = test.stats()["operatorStats"][5].outputMessageCount;
    for (let i = 0; i < 6; i++) {
        sleep(5000);
        if (test.stats()["operatorStats"][5].outputMessageCount > beforeOutCnt) {
            break;
        }
        jsTestLog("Not yet..");
    }
    assert(test.stats()["operatorStats"][5].outputMessageCount > beforeOutCnt);

    // we expect another checkpoint to be taken since window closed
    test.checkpoint();
    assert.soon(() => { return test.getCheckpointIds().length == numChkpts + 5; });

    // No checkpoint expected to be taken now
    test.checkpoint();
    assert.soon(() => {
        sleep(2000);
        return test.getCheckpointIds().length == numChkpts + 5;
    });

    test.stop();
}

checkpointCoordinatorTakeCheckpointTest();
