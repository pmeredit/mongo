/**
 * @tags: [
 *  featureFlagStreams,
 *  tsan_incompatible,
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

/**
 * The purpose of this test is to validate checkpoint save and restore for a
 * single large group accumulator state. The pipeline contains a tumbling window
 * which accumulates a large state (128 MB) for a single group id.
 * The test does the following,
 *   - Acuumulates a large state and stops the stream processor,
 *     which triggers a checkpoint
 *   - Resumes the stream processor from a stored checkpoint and
 *     ensures that the resume was successful
 *   - Close the tumbling window and validates the results
 */
function largeGroupAccumulatorTest(useRestoredExecutionPlan) {
    const pipeline = [
        {$replaceRoot: {newRoot: "$fullDocument"}},
        {
            $tumblingWindow: {
                interval: {size: NumberInt(30), unit: "minute"},
                allowedLateness: {size: NumberInt(0), unit: "second"},
                pipeline: [
                    { $project: { docSize: 1, seed: 1, ts: 1, value: { $range: [ 0, "$docCount" ] } } },
                    { $unwind: "$value" },
                    { $project: { seed: 1, ts: 1, bigValue: { $range: [0, "$docSize"] }}},
                    { 
                        $project: 
                        { 
                            bigStr: 
                            { 
                                $reduce: { 
                                    input: "$bigValue",
                                    initialValue: "",
                                    in: {
                                        "$concat": [ "$$value", "$seed" ]
                                    }
                                }
                            }, 
                            ts: 1,
                        }
                    },
                    {
                        $group: {
                            _id: "$_id",
                            bigArr: { $push: "$bigStr" },
                            ts: { $max: "$ts" }
                        }
                    },
                    {
                        $unwind: "$bigArr"
                    },
                    {
                        $project: {
                            _id: "$_id",
                            ts: "$ts"
                        }
                    }
                ]
            }
        }
    ];

    // Increase the memory limit from 100MB to 1GB for the group accumulator.
    let oldLimit = db.adminCommand(
        {getParameter: 1, internalQueryMaxPushBytes: 1})["internalQueryMaxPushBytes"];
    let newLimit = 1024 * 1024 * 1024;  // 1GB
    assert.commandWorked(db.adminCommand({setParameter: 1, internalQueryMaxPushBytes: newLimit}));

    // TODO: As part of the fix for "SERVER-88314: [Streams] Support large single group accumulator
    // checkpoint", increase the single accumulator state size from 128MB to 1GB.
    let docCount = 128;
    let docSize = 128;
    let seedSize = 8 * 1024;  // 8K
    const seed = Array(seedSize).toString();
    const inputBeforeStop = [{
        _id: 1,
        ts: ISODate("2024-03-01T01:00:00.000Z"),
        docCount: docCount,
        docSize: docSize,
        seed: seed
    }];

    let test = new TestHelper(inputBeforeStop,
                              pipeline,
                              999999999 /* interval */,
                              "changestream" /* sourceType */,
                              true /*useNewCheckpointing*/,
                              useRestoredExecutionPlan);

    test.run();

    // Wait for all the messages to be read.
    assert.soon(() => { return test.stats()["inputMessageCount"] == inputBeforeStop.length; },
                "InputMessageCount mismatch",
                180000);
    let groupOperatorStats = getOperatorStats("GroupOperator", test.stats());
    assert.eq("GroupOperator", groupOperatorStats["name"]);
    assert.soon(() => {
        groupOperatorStats = getOperatorStats("GroupOperator", test.stats());
        return groupOperatorStats["inputMessageCount"] == docCount;
    });

    assert.eq(0, groupOperatorStats["outputMessageCount"]);
    assert.eq(0, test.getResults().length, "expected no output");

    jsTestLog("Stopping now");
    test.stop();

    let ids = test.getCheckpointIds();
    assert.gt(ids.length, 0, "expected some checkpoints");

    // Run the streamProcessor, expecting to resume from a checkpoint.
    test.run(false /* firstStart */);

    const inputAfterStop =
        [{_id: 2, ts: ISODate("2024-03-01T02:00:00.000Z"), docCount: 1, docSize: 1, seed: "abc"}];
    assert.commandWorked(test.inputColl.insertMany(inputAfterStop));

    assert.soon(() => {
        groupOperatorStats = getOperatorStats("GroupOperator", test.stats());
        return groupOperatorStats["inputMessageCount"] == docCount + 1;
    });
    assert.soon(() => {
        groupOperatorStats = getOperatorStats("GroupOperator", test.stats());
        return groupOperatorStats["outputMessageCount"] == 1;
    });
    assert.soon(() => { return test.outputColl.find({}).count() == 1; });
    assert.eq(getOperatorStats("MergeOperator", test.stats())["inputMessageCount"], docCount);
    test.stop();
    assert.commandWorked(db.adminCommand({setParameter: 1, internalQueryMaxPushBytes: oldLimit}));
}

largeGroupAccumulatorTest(true);
// TODO(SERVER-92447): Remove this.
largeGroupAccumulatorTest(false);

assert.eq(listStreamProcessors()["streamProcessors"].length, 0);