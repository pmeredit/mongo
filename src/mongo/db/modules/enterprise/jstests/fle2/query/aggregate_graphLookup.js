/**
 * Test aggregations with $graphLookup on encrypted collections.
 *
 * @tags: [
 *   assumes_read_concern_unchanged,
 *   assumes_read_preference_unchanged,
 *   assumes_unsharded_collection,
 *   requires_fcv_70,
 * ]
 */

import {assertArrayEq} from "jstests/aggregation/extras/utils.js";
import {EncryptedClient, kSafeContentField} from "jstests/fle2/libs/encrypted_client_util.js";
import {
    fleAggTestData
} from "src/mongo/db/modules/enterprise/jstests/fle2/query/utils/agg_utils.js";

const {schema, docs} = fleAggTestData;

// Set up the encrypted collection.
const dbName = "aggregateGraphLookupDB";
const collName = "aggregateColl";
const dbTest = db.getSiblingDB(dbName);
dbTest.dropDatabase();
let client = new EncryptedClient(db.getMongo(), dbName);
assert.commandWorked(client.createEncryptionCollection(collName, schema));
let edb = client.getDB();

const coll = edb[collName];
for (const doc of docs) {
    assert.commandWorked(coll.einsert(doc));
}

// Run the pipeline on the provided collection, and assert that the results are equivalent to
// 'expected'. The pipeline is appended with a $project stage to project out safeContent data
// and other fields that are inconvenient to have in the output.
const runTest = (pipeline, collection, expected, extraInfo) => {
    const aggPipeline = pipeline.slice();
    aggPipeline.push(
        {$project: {[kSafeContentField]: 0, [`chain.${kSafeContentField}`]: 0, distance: 0}});
    const result = collection.aggregate(aggPipeline).toArray();
    assertArrayEq({actual: result, expected: expected, extraErrorMsg: tojson(extraInfo)});
};

const tests = [
    // $graphLookup with a filter on an encrypted field ($in).
    {
        pipeline: [
            {
                $graphLookup: {
                    from: collName,
                    as: "chain",
                    connectToField: "name",
                    connectFromField: "manager",
                    startWith: "$manager",
                    restrictSearchWithMatch: {ssn: {$in: ["123", "456"]}}
                }
            }
        ],
        expected: [
            Object.assign({chain: [docs[1]]}, docs[0]),
            Object.assign({chain: []}, docs[1]),
            Object.assign({chain: [docs[3], docs[0], docs[1]]}, docs[2]),
            Object.assign({chain: [docs[0], docs[1]]}, docs[3]),
        ]
    },
    // $graphLookup with a compount filter on an encrypted field ($ne) and a non-encrypted field.
    {
        pipeline: [
            {
                $graphLookup: {
                    from: collName,
                    as: "chain",
                    connectToField: "name",
                    connectFromField: "manager",
                    startWith: "$manager",
                    restrictSearchWithMatch: {ssn: {$ne: "789"}, _id: {$lt: 3}}
                }
            }
        ],
        expected: [
            Object.assign({chain: [docs[1]]}, docs[0]),
            Object.assign({chain: []}, docs[1]),
            Object.assign({chain: []}, docs[2]),
            Object.assign({chain: [docs[0], docs[1]]}, docs[3]),
        ]
    },
    //
    // Tests that check that if there are multiple stages in a pipeline, each stage referencing an
    // encrypted field is properly encrypted, and stages which do not reference encrypted fields
    // are not modified.
    //

    // $match on an encrypted field ($eq) and $graphLookup filtering on an encrypted field ($in).
    {
        pipeline: [
            {$match: {ssn: "789"}},
            {
                $graphLookup: {
                    from: collName,
                    as: "chain",
                    connectToField: "name",
                    connectFromField: "manager",
                    startWith: "$manager",
                    restrictSearchWithMatch: {ssn: {$in: ["123", "456"]}}
                }
            }
        ],
        expected: [Object.assign({chain: [docs[3], docs[0], docs[1]]}, docs[2])]
    },
    // $graphLookup with no filter.
    {
        pipeline: [{
            $graphLookup: {
                from: collName,
                as: "chain",
                connectToField: "name",
                connectFromField: "manager",
                startWith: "$manager",
                maxDepth: 0
            }
        }],
        expected: [
            Object.assign({chain: [docs[1]]}, docs[0]),
            Object.assign({chain: [docs[2]]}, docs[1]),
            Object.assign({chain: [docs[3]]}, docs[2]),
            Object.assign({chain: [docs[0]]}, docs[3]),
        ]
    },
    // $graphLookup which filters on two encrypted fields ($in, $ne).
    {
        pipeline: [
            {
                $graphLookup: {
                    from: collName,
                    as: "chain",
                    connectToField: "name",
                    connectFromField: "manager",
                    startWith: "$manager",
                    restrictSearchWithMatch: {ssn: {$in: ["123", "456"]}, "age": {$ne: NumberLong(35)}}
                }
            }
        ],
        expected: [
            Object.assign({chain: []}, docs[0]),
            Object.assign({chain: []}, docs[1]),
            Object.assign({chain: [docs[3], docs[0]]}, docs[2]),
            Object.assign({chain: [docs[0]]}, docs[3]),
        ]
    },
];

// Run all of the tests.
client.runEncryptionOperation(() => {
    for (const testData of tests) {
        const extraInfo = Object.assign({transaction: false}, testData);
        runTest(testData.pipeline, coll, testData.expected, extraInfo);
    }
});