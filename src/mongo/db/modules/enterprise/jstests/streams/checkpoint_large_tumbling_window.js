/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */

import {TestHelper} from "src/mongo/db/modules/enterprise/jstests/streams/checkpoint_helper.js";
import {listStreamProcessors} from "src/mongo/db/modules/enterprise/jstests/streams/utils.js";

function largeGroupTest(useRestoredExecutionPlan) {
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
                "idarray0": ["$_id", "$_id", "$_id", "$_$id", "$_id", "$_id"],
                "idarray1": ["$_id", "$_id", "$_id", "$_$id", "$_id", "$_id"],
                "idarray2": ["$_id", "$_id", "$_id", "$_$id", "$_id", "$_id"],
                "idarray3": ["$_id", "$_id", "$_id", "$_$id", "$_id", "$_id"],
            }
        },
        {
            $tumblingWindow: {
                interval: {size: NumberInt(3), unit: "hour"},
                allowedLateness: NumberInt(0),
                idleTimeout: NumberInt(0),
                pipeline: [{
                    $group:
                        {_id: "$customerId", customerDocs: {$push: "$$ROOT"}, max: {$max: "$max"}}
                }]
            }
        },
        {$project: {customerId: "$_id", max: "$max"}}
    ];

    var cnt = 2500000;
    const debugBuild = db.adminCommand("buildInfo").debug;
    if (debugBuild) {
        // Use fewer documents in debug builds so the test runs faster.
        cnt = cnt / 100;
    }
    const inputBeforeStop = [{idx: cnt, ts: baseTs}];

    let test = new TestHelper(inputBeforeStop,
                              pipeline,
                              999999999 /* interval */,
                              "changestream" /* sourceType */,
                              true /*useNewCheckpointing*/,
                              useRestoredExecutionPlan);

    test.run();

    assert.eq(test.stats()["operatorStats"][5]["name"], "GroupOperator");

    // Wait for all the messages to be read.
    assert.soon(() => {
        let stats = test.stats();
        jsTestLog(stats);
        return stats["inputMessageCount"] == inputBeforeStop.length;
    }, "InputMessageCount mismatch", 180000);
    assert.soon(() => { return test.stats()["operatorStats"][5]["inputMessageCount"] == cnt - 1; });
    assert.eq(0, test.stats()["operatorStats"][5]["outputMessageCount"]);
    assert.eq(0, test.getResults().length, "expected no output");

    jsTestLog("Stopping now");

    assert.eq(test.stats()["operatorStats"][0]["name"], "ChangeStreamConsumerOperator");

    test.stop();

    let ids = test.getCheckpointIds();
    assert.gt(ids.length, 0, "expected some checkpoints");

    // Run the streamProcessor, expecting to resume from a checkpoint.
    jsTestLog("about to rerun");
    test.run(false /* firstStart */);
    jsTestLog("finished test.run after restart");

    const inputAfterStop = [{idx: 2, ts: ISODate("2023-12-01T00:00:00.000Z")}];
    assert.commandWorked(test.inputColl.insertMany(inputAfterStop));

    assert.soon(() => { return test.stats()["operatorStats"][5]["inputMessageCount"] == cnt; });
    assert.soon(() => { return test.stats()["operatorStats"][5]["outputMessageCount"] == 50; });
    assert.soon(() => { return test.outputColl.find({}).count() == numCustomers; });
    test.outputColl.find().toArray().map(
        (doc) => assert((doc.max - doc.customerId) % numCustomers == 0));
    test.stop();
}

largeGroupTest(true);
// TODO(SERVER-92447): Remove this.
largeGroupTest(false);

assert.eq(listStreamProcessors()["streamProcessors"].length, 0);