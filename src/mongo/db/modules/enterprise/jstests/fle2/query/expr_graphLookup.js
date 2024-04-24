/**
 * Test rewrites of agg expressions inside $graphLookup filters over encrypted fields for FLE2.
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

// Set up the encrypted collection.
const dbName = "exprGraphLookup";
const collName = "aggregateColl";
const dbTest = db.getSiblingDB(dbName);
dbTest.dropDatabase();
let client = new EncryptedClient(db.getMongo(), dbName);
assert.commandWorked(client.createEncryptionCollection(collName, {
    encryptedFields: {
        fields: [
            {path: "ssn", bsonType: "string", queries: {queryType: "equality"}},
            {path: "age", bsonType: "long", queries: {queryType: "equality"}}
        ]
    }

}));
let edb = client.getDB();

// Documents that will be used in the following tests. The ssn and age fields have an encrypted
// equality index.
const docs = [
    {_id: 0, ssn: "123", name: "A", manager: "B", age: NumberLong(25), location: [0, 0]},
    {_id: 1, ssn: "456", name: "B", manager: "C", age: NumberLong(35), location: [0, 1]},
    {_id: 2, ssn: "789", name: "C", manager: "D", age: NumberLong(45), location: [0, 2]},
    {_id: 3, ssn: "123", name: "D", manager: "A", age: NumberLong(55), location: [0, 3]},
];

const coll = edb[collName];
for (const doc of docs) {
    assert.commandWorked(coll.einsert(doc));
}
assert.commandWorked(coll.createIndex({location: "2dsphere"}));

// Run the pipeline on the provided collection, and assert that the results are equivalent to
// 'expected'. The pipeline is appended with a $project stage to project out safeContent data
// and other fields that are inconvenient to have in the output.
const runAggTest = (pipeline, collection, expected, extraInfo) => {
    const aggPipeline = pipeline.slice();
    aggPipeline.push(
        {$project: {[kSafeContentField]: 0, [`chain.${kSafeContentField}`]: 0, distance: 0}});
    const result = collection.aggregate(aggPipeline).toArray();
    assertArrayEq({actual: result, expected: expected, extraErrorMsg: tojson(extraInfo)});
};

const aggTests = [
    // Similar to above, test that $graphLookup with restrictSearchWithMatch undergoes rewriting.
    {
        pipeline: [
            {
                $graphLookup: {
                    from: collName,
                    as: "chain",
                    connectToField: "name",
                    connectFromField: "manager",
                    startWith: "$manager",
                    restrictSearchWithMatch: {$expr: {$in: ["$ssn", ["123", "456"]]}}
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
    // Test that when no rewrites are needed on restrictSearchWithMatch, we get the correct results.
    {
        pipeline: [
            {
                $graphLookup: {
                    from: collName,
                    as: "chain",
                    connectToField: "name",
                    connectFromField: "manager",
                    startWith: "$manager",
                    restrictSearchWithMatch: {$expr: {$in: ["$name", ["A", "B", "D"]]}}
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
    // Test that $graphLookup undergoes rewrites for the startWith field.
    {
        pipeline: [
            {
                $graphLookup: {
                    from: collName,
                    as: "chain",
                    connectToField: "name",
                    connectFromField: "manager",
                    maxDepth: 0,
                    startWith: {
                        $cond: [
                            {$eq: ["$age", NumberLong(25)]},
                            "$name",
                            "$manager"
                        ]
                    },
                }
            }
        ],
        expected: [
            Object.assign({chain: [docs[0]]}, docs[0]),
            Object.assign({chain: [docs[2]]}, docs[1]),
            Object.assign({chain: [docs[3]]}, docs[2]),
            Object.assign({chain: [docs[0]]}, docs[3]),
        ]
    },
    // Test that when no rewrites are needed on the startWith field, we get the correct results.
    {
        pipeline: [
            {
                $graphLookup: {
                    from: collName,
                    as: "chain",
                    connectToField: "name",
                    connectFromField: "manager",
                    maxDepth: 0,
                    startWith: {
                        $cond: [
                            {$eq: ["$name", "A"]},
                            "$name",
                            "$manager"
                        ]
                    },
                }
            }
        ],
        expected: [
            Object.assign({chain: [docs[0]]}, docs[0]),
            Object.assign({chain: [docs[2]]}, docs[1]),
            Object.assign({chain: [docs[3]]}, docs[2]),
            Object.assign({chain: [docs[0]]}, docs[3]),
        ]
    },
    // Test that both startWith and restrictSearchWithMatch can be rewritten together.
    {
        pipeline: [
            {
                $graphLookup: {
                    from: collName,
                    as: "chain",
                    connectToField: "name",
                    connectFromField: "manager",
                    maxDepth: 0,
                    startWith: {
                        $cond: [
                            {$eq: ["$age", NumberLong(25)]},
                            "$name",
                            "$manager"
                        ]
                    },
                    restrictSearchWithMatch: {$expr: {$in: ["$ssn", ["123", "456"]]}}
                }
            }
        ],
        expected: [
            Object.assign({chain: [docs[0]]}, docs[0]),
            Object.assign({chain: []}, docs[1]),
            Object.assign({chain: [docs[3]]}, docs[2]),
            Object.assign({chain: [docs[0]]}, docs[3]),
        ]
    },
    // Test multi-stage pipelines.
    {
        // $match with a rewrite followed by $graphLookup with a rewrite.
        pipeline: [
            {$match: {$expr: {$eq: ["$ssn", "789"]}}},
            {
                $graphLookup: {
                    from: collName,
                    as: "chain",
                    connectToField: "name",
                    connectFromField: "manager",
                    startWith: "$manager",
                    restrictSearchWithMatch: {$expr: {$in: ["$ssn", ["123", "456"]]}}
                }
            }
        ],
        expected: [Object.assign({chain: [docs[3], docs[0], docs[1]]}, docs[2])]
    },
];

client.runEncryptionOperation(() => {
    for (const testData of aggTests) {
        runAggTest(testData.pipeline, coll, testData.expected, testData);
    }
});