import {
    resultsEq,
} from "jstests/aggregation/extras/utils.js";
import {
    TestHelper,
    uuidStr
} from "src/mongo/db/modules/enterprise/jstests/streams/checkpoint_helper.js";

export function testRunner({
    originalPipeline,
    modifiedPipeline,
    inputForOriginalPipeline = [],
    inputAfterCheckpoint1 = [],
    inputAfterCheckpoint2 = [],
    inputAfterStopBeforeModify = [],
    inputAfterModifyBeforeRestart = [],
    resultsQuery = [],
    expectedOutput = [],
    expectedDlqBeforeModify = [],
    expectedDlqAfterModify = [],
    validateShouldSucceed = true,
    expectedValidateError = "",
    resumeFromCheckpoint = true,
    afterModifyReplayCount = 0,
    modifiedPipeline2,
    inputAfterStopBeforeModify2 = [],
    inputAfterModifyBeforeRestart2 = [],
    afterModifyReplayCount2 = 0
}) {
    const waitTimeMs = 60000;
    // Run the stream processor with the originalPipeline.
    let test = new TestHelper(
        inputForOriginalPipeline,
        originalPipeline,
        null,           /* interval */
        "changestream", /* sourceType */
        true,           /* useNewCheckpointing */
        true,           /* useRestoredExecutionPlan */
    );

    test.run();
    let totalInputMessages = inputForOriginalPipeline.length;
    // Wait for all the messages to be read.
    assert.soon(() => { return test.stats()["inputMessageCount"] == totalInputMessages; },
                tojson(test.stats()),
                waitTimeMs);
    assert.soon(() => { return test.dlqColl.count() == expectedDlqBeforeModify.length; },
                "waiting for dlqMessageCount",
                waitTimeMs);
    assert(resultsEq(expectedDlqBeforeModify,
                     test.dlqColl.aggregate([{$replaceRoot: {newRoot: "$doc"}}]).toArray(),
                     true /* verbose */,
                     ["_id", "_stream_meta"]));
    if (inputAfterCheckpoint1) {
        test.checkpoint(true /* forced */);
        test.inputColl.insertMany(inputAfterCheckpoint1);
        totalInputMessages += inputAfterCheckpoint1.length;
        assert.soon(() => { return test.stats()["inputMessageCount"] == totalInputMessages; });
    }

    if (inputAfterCheckpoint2) {
        test.checkpoint(true /* forced */);
        test.inputColl.insertMany(inputAfterCheckpoint2);
        totalInputMessages += inputAfterCheckpoint2.length;
        assert.soon(() => { return test.stats()["inputMessageCount"] == totalInputMessages; });
    }

    // Stop the stream processor, writing a final checkpoint.
    test.stop();
    if (inputAfterStopBeforeModify) {
        totalInputMessages += inputAfterStopBeforeModify.length;
        test.inputColl.insertMany(inputAfterStopBeforeModify);
        assert.soon(() => { return totalInputMessages == test.inputColl.count(); });
    }

    // Resume the stream processor on the new pipeline.
    jsTestLog(`Starting modified processor ${tojson(modifiedPipeline)}`);

    // Validate the modify request and, if validateShouldSucceed=true, start the modified processor.
    let validateResult = test.modifyAndStart({
        newPipeline: modifiedPipeline,
        validateShouldSucceed: validateShouldSucceed,
        resumeFromCheckpointAfterModify: resumeFromCheckpoint
    });
    if (!validateShouldSucceed) {
        assert.commandFailedWithCode(validateResult, ErrorCodes.StreamProcessorInvalidOptions);
        assert.eq(validateResult.errmsg, expectedValidateError);
        return;
    }
    totalInputMessages += afterModifyReplayCount;
    // Wait for all the input messages to be processed.
    assert.soon(
        () => { return test.stats()["inputMessageCount"] == totalInputMessages; },
        `expected inputMessageCount of ${totalInputMessages}, found ${tojson(test.stats())}`,
        waitTimeMs);

    if (inputAfterModifyBeforeRestart) {
        test.stop();
        test.inputColl.insertMany(inputAfterModifyBeforeRestart);
        test.run(false);
        totalInputMessages += inputAfterModifyBeforeRestart.length;
        assert.soon(() => { return totalInputMessages == test.stats()["inputMessageCount"]; });
    }

    if (modifiedPipeline2) {
        test.stop();
        test.inputColl.insertMany(inputAfterStopBeforeModify2);
        jsTestLog(`Starting modified processor ${tojson(modifiedPipeline2)}`);
        validateResult =
            test.modifyAndStart({newPipeline: modifiedPipeline2, validateShouldSucceed: true});
        totalInputMessages += afterModifyReplayCount2;
        totalInputMessages += inputAfterStopBeforeModify2.length;
        assert.soon(() => { return test.stats()["inputMessageCount"] == totalInputMessages; },
                    tojson(test.stats()),
                    waitTimeMs);
    }
    if (inputAfterModifyBeforeRestart2) {
        test.stop();
        test.inputColl.insertMany(inputAfterModifyBeforeRestart2);
        test.run(false);
        totalInputMessages += inputAfterModifyBeforeRestart2.length;
        assert.soon(() => { return totalInputMessages == test.stats()["inputMessageCount"]; });
    }
    // Validate the expected output and summary stats.
    assert.soon(() => { return test.outputColl.count() == expectedOutput.length; },
                `waiting for expected output: ${tojson(expectedOutput)}, found ${
                    tojson(test.outputColl.find({}).toArray())}`,
                waitTimeMs);
    const output = test.outputColl.aggregate(resultsQuery).toArray();
    assert(resultsEq(
        expectedOutput, output, true /* verbose */, ["_id", "_stream_meta"] /* fieldsToSkip */));
    assert(resultsEq(expectedDlqAfterModify,
                     test.dlqColl.aggregate([{$replaceRoot: {newRoot: "$doc"}}]).toArray(),
                     true /* verbose */,
                     ["_id", "_stream_meta"] /* fieldsToSkip */));

    test.stop();
}
