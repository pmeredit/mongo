/* @tags: [
 *  featureFlagStreams,
 * ]
 *
 * Test various streaming $merge scenarios with diffferent levels of parallelism.
 */

import {
    commonTest,
} from "src/mongo/db/modules/enterprise/jstests/streams/common_test.js";
import {
    getDefaultSp,
} from "src/mongo/db/modules/enterprise/jstests/streams/fake_client.js";
import {test as constants} from "src/mongo/db/modules/enterprise/jstests/streams/fake_client.js";

for (let parallelism of [0, 2, 4, 8]) {
    const test = ({
        input,
        expectedOutput,
        expectedGeneratedOutput,
        expectedOutputMessageCount,
        on,
        whenMatched,
        fieldsToIgnore = [],
        expectedDlq = [],
        useGeneratedSourcePipeline = true,
    }) => {
        var extras = {};
        if (parallelism > 0) {
            extras.parallelism = NumberInt(parallelism);
        }
        if (on) {
            extras.on = on;
        }
        if (whenMatched) {
            extras.whenMatched = whenMatched;
        }
        commonTest({
            input,
            pipeline: [{$project: {_ts: 0, _stream_meta: 0}}],
            expectedOutput,
            useTimeField: false,
            expectedGeneratedOutput,
            expectedOutputMessageCount: expectedOutputMessageCount,
            extraMergeParams: extras,
            useGeneratedSourcePipeline: useGeneratedSourcePipeline,
            fieldsToIgnore,
            expectedDlq: expectedDlq,
            beforeStopValidationFunc: (testHelper) => {
                const metrics = testHelper.gaugeMetrics();
                const queueByteSizeMetrics =
                    metrics.filter((m) => { return m.name == "sink_operator_queue_size"; });
                const num = parallelism > 0 ? parallelism : 1;
                assert.eq(num, queueByteSizeMetrics.length);
                for (let threadID = 0; threadID < num; ++threadID) {
                    assert(queueByteSizeMetrics.some((m) => {
                        const threadIDLabel = m.labels.find(l => l.key == "sink_thread_id");
                        return threadIDLabel.value == threadID;
                    }));
                }
            },
        });
    };

    // Test a basic case where _id is auto-generated.
    test({
        input: [
            {customer: 1, ad: 1, value: 2},
        ],
        expectedOutput: [
            {customer: 1, ad: 1, value: 2},
        ],
        fieldsToIgnore: ["_id"]
    });
    // Test a basic case where _id is set in the pipeline.
    test({
        input: [
            {_id: 0, a: 1},
            {_id: 1, a: 2},
            {_id: 0, a: 3},
        ],
        expectedOutput: [
            {_id: 1, a: 2},
            {_id: 0, a: 3},
        ],
        expectedGeneratedOutput: [
            {_id: 0, a: 1},
            {_id: 1, a: 2},
            {_id: 0, a: 3},
        ],
        expectedOutputMessageCount: 3,
    });

    // Test where all input has the same _id.
    let input = Array.from(Array(100).keys()).map(i => { return {_id: 0, i: i}; });
    test({
        input: input,
        expectedOutput: [{_id: 0, i: 99}],
        expectedGeneratedOutput: input,
        expectedOutputMessageCount: input.length,
    });

    // Test where the input is spread across 2 _ids.
    input = Array.from(Array(100).keys()).map(i => { return {_id: (i & 0x1), i: i}; });
    test({
        input: input,
        expectedOutput: [{_id: 0, i: 98}, {_id: 1, i: 99}],
        expectedGeneratedOutput: input,
        expectedOutputMessageCount: input.length,
    });
    // Same input with keepExisting.
    test({
        input: input,
        expectedOutput: [{_id: 0, i: 0}, {_id: 1, i: 1}],
        expectedGeneratedOutput: input,
        expectedOutputMessageCount: input.length,
        whenMatched: "keepExisting",
    });

    // Test with a custom $merge.on field.
    input = Array.from(Array(100).keys()).map(i => { return {i: i, a: 1, _id: 123}; });
    test({
        input: input,
        expectedOutput: [{i: 99, a: 1}],
        expectedGeneratedOutput: input,
        expectedOutputMessageCount: input.length,
        on: ["a"],
        fieldsToIgnore: ["_id"]
    });

    // Test with a custom $merge.on field.
    input = Array.from(Array(100).keys()).map(i => { return {i: i, a: 1, b: i % 5, _id: i % 5}; });
    test({
        input: input,
        expectedOutput: [
            {i: 95, a: 1, b: 0},
            {i: 96, a: 1, b: 1},
            {i: 97, a: 1, b: 2},
            {i: 98, a: 1, b: 3},
            {i: 99, a: 1, b: 4},
        ],
        expectedGeneratedOutput: input,
        expectedOutputMessageCount: input.length,
        on: ["a", "b"],
        fieldsToIgnore: ["_id"]
    });

    // Test with a custom $merge.on field with documents that have it.
    input = [{i: 0, _id: 1}];
    test({
        input: input,
        expectedOutput: [{i: 0, _id: 1, a: null, b: null}],
        on: ["a", "b"],
        useGeneratedSourcePipeline: false,
    });

    // Test with a custom $merge.on field with documents that don't have part of it.
    input = [{i: 0, _id: 1, a: 1}];
    test({
        input: input,
        expectedOutput: [{i: 0, _id: 1, a: 1, b: null}],
        on: ["a", "b"],
        useGeneratedSourcePipeline: false,
    });
}

const expectFailureTest = (pipeline, errmsg) => {
    const sp = getDefaultSp();
    const result = sp.createStreamProcessor("foo", pipeline)
                       .start(undefined /* options */, false /* assertWorked */);
    assert.commandFailedWithCode(result, ErrorCodes.StreamProcessorInvalidOptions);
    assert.eq(errmsg, result.errmsg);
};

const errmsg =
    "StreamProcessorInvalidOptions: $merge.into.db and $merge.into.coll must be literals if $merge.parallelism is greater than 1";
expectFailureTest(
    [
        {$source: {connectionName: constants.atlasConnection, db: "db", coll: "coll"}},
        {
            $merge: {
                into: {connectionName: constants.atlasConnection, db: "$db", coll: "coll"},
                parallelism: NumberInt(2)
            }
        }
    ],
    errmsg);
expectFailureTest(
    [
        {$source: {connectionName: constants.atlasConnection, db: "db", coll: "coll"}},
        {
            $merge: {
                into: {
                    connectionName: constants.atlasConnection,
                    db: "db",
                    coll: {$toString: "$foo"}
                },
                parallelism: NumberInt(4)
            }
        }
    ],
    errmsg);
expectFailureTest(
    [
        {$source: {connectionName: constants.atlasConnection, db: "db", coll: "coll"}},
        {
            $emit: {
                connectionName: constants.atlasConnection,
                db: "db",
                coll: "coll",
                timeseries: {timeField: 'ts', metaField: 'metaData'},
                parallelism: NumberInt(2)
            }
        }
    ],
    "IDLUnknownField: BSON field 'TimeseriesSinkOptions.parallelism' is an unknown field.");