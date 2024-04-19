/**
 * @tags: [
 *  featureFlagStreams,
 * ]
 */

import {TestHelper} from "src/mongo/db/modules/enterprise/jstests/streams/checkpoint_helper.js";
import {listStreamProcessors} from "src/mongo/db/modules/enterprise/jstests/streams/utils.js";

function largeGroupTest() {
    function generateInput(cnt) {
        let input = [];
        var msPerDocument = 1;
        let baseTs = ISODate("2023-01-01T00:00:00.000Z");
        for (let i = 0; i < cnt; i++) {
            let ts = new Date(baseTs.getTime() + msPerDocument * i);
            input.push({
                ts: ts,
                value: i,
                msg: "x".repeat(20000),
            });
        }
        return input;
    }

    var numCustomers = 50;
    const pipeline = [
        {$replaceRoot: {newRoot: "$fullDocument"}},
        {
            $addFields: {
                "customerId": {$mod: ["$value", numCustomers]},
                "max": "$value",
            }
        },
        {
            $hoppingWindow: {
                interval: {size: NumberInt(3), unit: "hour"},
                hopSize: {size: NumberInt(1), unit: "minute"},
                allowedLateness: {size: NumberInt(0), unit: "second"},
                pipeline: [{
                    $group:
                        {_id: "$customerId", customerDocs: {$push: "$$ROOT"}, max: {$max: "$max"}}
                }]
            }
        },
        {$project: {customerId: "$_id", max: "$max"}}
    ];

    var numInputDocs = 20 * numCustomers;
    const inputBeforeStop = generateInput(numInputDocs);
    let test = new TestHelper(inputBeforeStop,
                              pipeline,
                              999999999 /* interval */,
                              "changestream" /* sourceType */,
                              true /*useNewCheckpointing*/);

    test.run();
    // Wait for all the messages to be read.
    assert.soon(() => { return test.stats()["inputMessageCount"] == inputBeforeStop.length; });
    assert.eq(0, test.getResults().length, "expected no output");

    jsTestLog("About to stop");
    test.stop();

    let ids = test.getCheckpointIds();
    assert.gt(ids.length, 0, "expected some checkpoints");

    // Run the streamProcessor, expecting to resume from a checkpoint.
    jsTestLog("about to rerun");
    test.run(false /* firstStart */);
    jsTestLog("finished test.run after restart");

    const inputAfterStop = [{ts: ISODate("2023-12-01T02:00:00.001Z")}];
    assert.commandWorked(test.inputColl.insertMany(inputAfterStop));
    // Each of the 180 windows will contribute 50 unique docs (1 per customer). In the
    // output collection, these will then further get collapsed into 50 docs in all, 1
    // per customer
    assert.soon(() => { return test.outputColl.find({}).count() == numCustomers; });
    test.outputColl.find().toArray().map(
        (doc) => assert((doc.max - doc.customerId) % numCustomers == 0));
    jsTestLog("about to final-stop");
    test.stop();
}

largeGroupTest();

assert.eq(listStreamProcessors()["streamProcessors"].length, 0);