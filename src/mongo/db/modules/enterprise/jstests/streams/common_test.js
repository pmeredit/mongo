/**
 * The commonTest function allows you to run a pipeline and input using different sources, with
 * checkpoints, and validate expected output.
 */

import {
    resultsEq,
} from "jstests/aggregation/extras/utils.js";
import {
    CheckPointTestHelper,
    TestHelper
} from "src/mongo/db/modules/enterprise/jstests/streams/checkpoint_helper.js";
import {
    getDefaultSp,
} from "src/mongo/db/modules/enterprise/jstests/streams/fake_client.js";
import {waitWhenThereIsMoreData} from "src/mongo/db/modules/enterprise/jstests/streams/utils.js";

const timeoutSecs = 60;

// Used in commonTest to force the next document to be in a new batch after previous documents
// have all been processed.
export const batchBreakerField = "__batchBreaker";
export function makeBatchBreakerDoc() {
    let doc = {};
    doc[batchBreakerField] = 1;
    return doc;
}
// Makes comparisons easier in commonTest.
export function sanitizeDlqDoc(d) {
    delete d["_stream_meta"];
    delete d["processorName"];
    delete d["dlqTime"];
    delete d["doc"]["_ts"];
    delete d["doc"]["_stream_meta"];
    return d;
}

function runGeneratedSourcePipeline({
    input,
    pipeline,
    expectedOutput,
    expectedDlq,
    featureFlags,
    timeField,
    useTimeField,
    fieldsToSkip
}) {
    const sp = getDefaultSp();
    let generatedSource = {$source: {documents: input}};
    if (useTimeField) {
        generatedSource["$source"]["timeField"] = timeField;
    }
    const waitForCount = expectedOutput.length + expectedDlq.length;
    const output = sp.process(
        [
            generatedSource,
            ...pipeline,
        ],
        expectedOutput.length > 0 ? 5000000 : 1 /* maxLoops */,
        featureFlags,
        waitForCount);
    const dlqMessageField = "_dlqMessage";
    const dlqOutput = output.filter(d => d.hasOwnProperty(dlqMessageField))
                          .map(d => sanitizeDlqDoc(d[dlqMessageField]));
    const actualOutput = output.filter(d => !d.hasOwnProperty(dlqMessageField));
    assert(resultsEq(expectedOutput, actualOutput, true /* verbose */, fieldsToSkip));
    assert(resultsEq(expectedDlq, dlqOutput, true /* verbose */, fieldsToSkip));
}

// Run kafka pipeline with a checkpoint in the middle
function runKafkaPipeline({
    input,
    pipeline,
    expectedOutput,
    featureFlags,
    fieldsToSkip,
    useTimeField,
    extraMergeParams,
    expectedOutputMessageCount,
    beforeStopValidationFunc
}) {
    const test2 = new CheckPointTestHelper(input,
                                           pipeline,
                                           10000000,
                                           "kafka",
                                           true,
                                           null,
                                           null,
                                           useTimeField,
                                           featureFlags,
                                           extraMergeParams);
    // now split the input, stop the run in the middle, continue and verify the output is same
    jsTestLog(`input docs size=${input.length}`);
    test2.runWithInputSlice(0, input.length - 1);
    assert.soon(() => { return test2.stats().inputMessageCount == input.length - 1; },
                "Waiting for input message count",
                timeoutSecs * 1000);
    test2.stop();  // this will force the checkpoint

    // Run the streamProcessor.
    test2.run();
    assert.soon(() => {
        return test2.stats().inputMessageCount == input.length &&
            test2.stats().outputMessageCount ==
            (expectedOutputMessageCount ? expectedOutputMessageCount : expectedOutput.length);
    }, "Waiting for input message count", timeoutSecs * 1000);
    assert.soon(() => { return test2.outputColl.count() == expectedOutput.length; },
                "waiting for output messages in output collection",
                timeoutSecs * 1000);
    waitWhenThereIsMoreData(test2.outputColl);
    assert.soon(() => {
        let results = test2.getResults();
        return resultsEq(expectedOutput, results, true, fieldsToSkip);
    }, "waiting for results", timeoutSecs * 1000);
    if (beforeStopValidationFunc) {
        beforeStopValidationFunc(test2);
    }
    test2.stop();
}

// Run changestream pipeline with checkpoint in the middle
function runChangeStreamPipeline({
    input,
    pipeline,
    useTimeField,
    featureFlags,
    expectedOutput,
    expectedDlq,
    fieldsToSkip,
    extraMergeParams,
    expectedOutputMessageCount,
    beforeStopValidationFunc,
    adjustPipeline = true,
    waitForInsertsInStats = true,
    expectedChangeStreamStats
}) {
    let newPipeline = pipeline;
    if (adjustPipeline) {
        newPipeline = [
            {$match: {$or: [{operationType: "insert"}, {operationType: "replace"}]}},
            {$replaceRoot: {newRoot: "$fullDocument"}},
            ...pipeline,
        ];
    }
    const test = new TestHelper(input,
                                newPipeline,
                                undefined,     // interval
                                "atlas",       // sourcetype
                                undefined,     // useNewCheckpointing
                                undefined,     // useRestoredExecutionPlan
                                undefined,     // writeDir
                                undefined,     // restoreDir
                                undefined,     // dbForTest
                                undefined,     // targetSourceMergeDb
                                useTimeField,  // useTimeField
                                "atlas",       // sinkType
                                false,         // changestreamStalenessMonitoring
                                undefined,     // oplogSizeMB
                                true,          // kafkaIsTest
                                featureFlags,
                                extraMergeParams);
    test.startOptions.featureFlags = Object.assign(test.startOptions.featureFlags, featureFlags);

    jsTestLog(`Running with input length ${input.length}, first doc ${tojson(input[0])}`);

    const insertData = (docs) => {
        let numInserts = 0;
        for (const doc of docs) {
            if (doc.hasOwnProperty(batchBreakerField)) {
                if (waitForInsertsInStats) {
                    // Wait for all other docs to be processed.
                    assert.soon(() => { return test.stats().inputMessageCount == numInserts; },
                                `waiting for inputMessageCount of ${numInserts}`,
                                timeoutSecs * 1000);
                }
            } else {
                if (doc.hasOwnProperty("_id")) {
                    assert.commandWorked(
                        test.inputColl.replaceOne({_id: doc["_id"]}, doc, {upsert: true}));
                } else {
                    assert.commandWorked(test.inputColl.replaceOne(doc, doc, {upsert: true}));
                }
                numInserts += 1;
            }
        }

        if (waitForInsertsInStats) {
            assert.soon(() => { return test.stats().inputMessageCount == numInserts; },
                        `waiting for inputMessageCount of ${numInserts}`,
                        timeoutSecs * 1000);
        }
    };

    // Start the processor and write part of the input.
    test.startFromLatestCheckpoint();
    const firstInput = input.slice(0, input.length - 1);
    insertData(firstInput);
    // Stop the processor and write the rest of the input.
    test.stop();
    const lastDoc = input[input.length - 1];
    if (lastDoc.hasOwnProperty("_id")) {
        assert.commandWorked(
            test.inputColl.replaceOne({_id: lastDoc["_id"]}, lastDoc, {upsert: true}));
    } else {
        assert.commandWorked(test.inputColl.replaceOne(lastDoc, lastDoc, {upsert: true}));
    }
    // Start the processor from its last checkpoint.
    test.startFromLatestCheckpoint();
    // Validate the expected results are obtained.
    assert.soon(() => {
        return test.stats().outputMessageCount ==
            (expectedOutputMessageCount ? expectedOutputMessageCount : expectedOutput.length);
    }, "waiting for expected outputMessageCount", 1000 * timeoutSecs);
    assert.soon(() => {
        return resultsEq(
            test.outputColl.find({}).toArray(), expectedOutput, true /* verbose */, fieldsToSkip);
    }, "waiting for expected output", 1000 * timeoutSecs);
    // Wait for expected DLQ results.
    assert.soon(() => { return test.stats().dlqMessageCount == expectedDlq.length; },
                "waiting for expected DLQ stats",
                timeoutSecs * 1000);
    assert.soon(() => {
        return resultsEq(test.dlqColl.find({}).toArray().map(d => sanitizeDlqDoc(d)),
                         expectedDlq,
                         true /* verbose */,
                         fieldsToSkip);
    }, "waiting for expected DLQ output", 1000 * timeoutSecs);

    if (beforeStopValidationFunc) {
        beforeStopValidationFunc(test);
    }

    if (expectedChangeStreamStats) {
        const operatorStats = test.stats().operatorStats;
        assert.eq(expectedChangeStreamStats.length, operatorStats.length);
        for (let opIdx = 0; opIdx < operatorStats.length; opIdx += 1) {
            const expectedStats = expectedChangeStreamStats[opIdx];
            const actualStats = operatorStats[opIdx];
            for (var prop in Object.getOwnPropertyNames(expectedStats)) {
                assert.eq(expectedStats[prop], actualStats[prop]);
            }
        }
    }

    test.stop();
}

/*
 * Helper function to test that the pipeline fails as intended.
 */
export function commonFailureTest({
    input,
    pipeline,
    expectedErrorCode,
    useTimeField = true,
    featureFlags = {},
}) {
    // Test with a changestream source and a checkpoint in the middle.
    const newPipeline = [
        {$match: {operationType: "insert"}},
        {$replaceRoot: {newRoot: "$fullDocument"}},
        ...pipeline,
    ];
    const test = new TestHelper(input,
                                newPipeline,
                                undefined,  // interval
                                "atlas",    // sourcetype
                                undefined,  // useNewCheckpointing
                                undefined,  // useRestoredExecutionPlan
                                undefined,  // writeDir
                                undefined,  // restoreDir
                                undefined,  // dbForTest
                                undefined,  // targetSourceMergeDb
                                useTimeField);
    test.startOptions.featureFlags = Object.assign(test.startOptions.featureFlags, featureFlags);

    jsTestLog(`Running with input length ${input.length}, first doc ${input[tojson(0)]}`);

    test.run();
    assert.commandWorked(test.inputColl.insertMany(input));

    assert.soon(() => { return test.stats()["status"] == "error"; },
                "waiting for expected status",
                1000 * 10);
    if (expectedErrorCode) {
        assert.eq(test.list()[0].error.code, expectedErrorCode);
    }
    test.stop();
}

/*
 * Helper function to test where the sink stage is included in the pipeline.
 */
export function commonSinkTest({
    input,
    pipeline,
    expectedOutputMessageCount,
    expectedDlq,
    useTimeField = true,
    featureFlags = {},
}) {
    // Test with a changestream source and a checkpoint in the middle.
    const newPipeline = [
        {$match: {operationType: "insert"}},
        {$replaceRoot: {newRoot: "$fullDocument"}},
        ...pipeline,
    ];
    const test = new TestHelper(
        input,
        newPipeline,
        undefined,  // interval
        "atlas",    // sourcetype
        undefined,  // useNewCheckpointing
        undefined,  // useRestoredExecutionPlan
        undefined,  // writeDir
        undefined,  // restoreDir
        undefined,  // dbForTest
        undefined,  // targetSourceMergeDb
        useTimeField,
        "included",  // sinkType
    );
    test.startOptions.featureFlags = Object.assign(test.startOptions.featureFlags, featureFlags);

    jsTestLog(`Running with input length ${input.length}, first doc ${input[tojson(0)]}`);

    test.run();
    assert.commandWorked(test.inputColl.insertMany(input));

    var waitTimeMs = 1000 * 10;

    assert.soon(() => { return test.stats()["inputMessageCount"] == input.length; },
                tojson(test.stats()),
                waitTimeMs);

    const waitForCount = expectedOutputMessageCount + expectedDlq.length;
    assert.soon(() => {
        return test.stats()["outputMessageCount"] + test.stats()["dlqMessageCount"] == waitForCount;
    }, tojson(test.stats()), waitTimeMs);

    assert.eq(test.stats()["outputMessageCount"], expectedOutputMessageCount);
    assert.eq(test.stats()["dlqMessageCount"], expectedDlq.length);
    assert.soon(() => {
        return resultsEq(test.dlqColl.find({}).toArray().map(d => sanitizeDlqDoc(d)),
                         expectedDlq,
                         true /* verbose */,
                         ["_id"]);
    });
    test.stop();
}

/*
 * Helper function to test that the pipeline returns the expectedOutput for the given input.
 * First, the $source.documents test source is used.
 * Then, then a mock Kafka $source is used with a checkpoint in the middle.
 * Then, a change stream source is used, and there is a checkpoint in the middle.
 */
export function commonTest({
    input,
    pipeline,
    expectedOutput,
    timeField = "$ts",
    expectedGeneratedOutput,
    expectedChangestreamOutput,
    expectedTestKafkaOutput,
    useTimeField = true,
    featureFlags = {},
    expectedDlq = [],
    useKafka = true,
    fieldsToSkip = ["_id"],
    extraMergeParams,
    expectedOutputMessageCount,
    beforeStopValidationFunc,
    useGeneratedSourcePipeline = true,
    adjustPipeline = true,
    waitForInsertsInStats = true,
    expectedChangeStreamStats
}) {
    assert(expectedOutput ||
           (expectedGeneratedOutput && expectedChangestreamOutput && expectedTestKafkaOutput) ||
           expectedDlq);

    const documentsSourceInput = input.filter(d => !d.hasOwnProperty(batchBreakerField));
    if (useGeneratedSourcePipeline) {
        runGeneratedSourcePipeline({
            input: documentsSourceInput,
            pipeline: pipeline,
            expectedOutput: (expectedGeneratedOutput ? expectedGeneratedOutput : expectedOutput),
            expectedDlq: expectedDlq,
            featureFlags: featureFlags,
            timeField: timeField,
            useTimeField: useTimeField,
            fieldsToSkip: fieldsToSkip
        });
    }

    if (useKafka) {
        runKafkaPipeline({
            input: documentsSourceInput,
            pipeline: pipeline,
            expectedOutput: (expectedTestKafkaOutput ? expectedTestKafkaOutput : expectedOutput),
            featureFlags: featureFlags,
            fieldsToSkip: fieldsToSkip,
            useTimeField: useTimeField,
            expectedOutputMessageCount: expectedOutputMessageCount,
            extraMergeParams: extraMergeParams,
            beforeStopValidationFunc: beforeStopValidationFunc,
        });
    }

    runChangeStreamPipeline({
        input: documentsSourceInput,
        pipeline: pipeline,
        useTimeField: useTimeField,
        featureFlags: featureFlags,
        expectedOutput: (expectedChangestreamOutput ? expectedChangestreamOutput : expectedOutput),
        expectedDlq: expectedDlq,
        fieldsToSkip: fieldsToSkip,
        extraMergeParams: extraMergeParams,
        expectedOutputMessageCount: expectedOutputMessageCount,
        beforeStopValidationFunc: beforeStopValidationFunc,
        adjustPipeline: adjustPipeline,
        waitForInsertsInStats: waitForInsertsInStats,
        expectedChangeStreamStats: expectedChangeStreamStats
    });
}
