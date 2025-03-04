/* @tags: [
 *  featureFlagStreams,
 * ]
 *
 * Test changestream $source scenarios using commonTest
 */

import {
    commonTest,
} from "src/mongo/db/modules/enterprise/jstests/streams/common_test.js";

// Test a basic case where a match follows a change stream $source.
commonTest({
    input: [
        {customer: 1, ad: 1, value: 1},
        {customer: 1, ad: 2, value: 2},
        {customer: 1, ad: 4, value: 4},
        {customer: 1, ad: 5, value: 5},
        {customer: 1, ad: 1, value: 6},
    ],
    expectedOutput: [
        {customer: 1, ad: 1, value: 1},
        {customer: 1, ad: 1, value: 6},
    ],
    pipeline: [
        {$match: {"fullDocument.ad": 1}},
        {$match: {$or: [{operationType: "insert"}, {operationType: "replace"}]}},
        {$replaceRoot: {newRoot: "$fullDocument"}},
        {$project: {_stream_meta: 0}},
    ],
    adjustPipeline: false,
    useGeneratedSourcePipeline: false,
    useKafka: false,
    featureFlags: {changestreamPredicatePushdown: true},
    waitForInsertsInStats: false,
    useTimeField: false,
    expectedChangeStreamStats: [
        {"name": "ChangeStreamConsumerOperator", inputMessageCount: 2},
        {"name": "ReplaceRootOperator", inputMessageCount: 2},
        {"name": "ProjectOperator", inputMessageCount: 2},
        {"name": "MergeOperator", inputMessageCount: 2},
    ]
});