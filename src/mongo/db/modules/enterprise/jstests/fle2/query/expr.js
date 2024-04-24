/**
 * Test rewrites of agg expressions over encrypted fields for FLE2.
 *
 * @tags: [
 *   assumes_read_concern_unchanged,
 *   assumes_read_preference_unchanged,
 *   requires_fcv_70,
 * ]
 */

import {assertArrayEq} from "jstests/aggregation/extras/utils.js";
import {EncryptedClient, kSafeContentField} from "jstests/fle2/libs/encrypted_client_util.js";
import {exprTestData} from "src/mongo/db/modules/enterprise/jstests/fle2/query/utils/expr_utils.js";

const {docs, matchFilters} = exprTestData;

// Set up the encrypted collection.
const dbName = "exprDB";
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
    aggPipeline.push({
        $project: {
            [kSafeContentField]: 0,
            [`chain.${kSafeContentField}`]: 0,
            distance: 0,
            updateField: 0,
            'chain.updateField': 0
        }
    });
    const result = collection.aggregate(aggPipeline).toArray();
    assertArrayEq({actual: result, expected: expected, extraErrorMsg: tojson(extraInfo)});
};

// Run a find with the provided filter on the provided collection, and assert that the results are
// equivalent to 'expected'. The find will project out safeContent data.
const runFindTest = (filter, collection, expected, extraInfo) => {
    const result = collection.find(filter, {[kSafeContentField]: 0, updateField: 0}).toArray();
    assertArrayEq({actual: result, expected: expected, extraErrorMsg: tojson(extraInfo)});
};

// Run each of the filters above within a find and an aggregate. The results should be consistent.
client.runEncryptionOperation(() => {
    for (const testData of matchFilters) {
        runAggTest([{$match: testData.filter}], coll, testData.expected, testData);
        runFindTest(testData.filter, coll, testData.expected, testData);
    }
});

const aggTests = [
    // Similar to the above, test that $geoNear undergoes rewrites for the query field.
    {
        pipeline: [{
            $geoNear: {
                near: {type: "Point", coordinates: [0, 0]},
                distanceField: "distance",
                key: "location",
                query: {
                    $expr:
                        {$and: [{$in: ["$ssn", ["789", "456"]]}, {$eq: ["$age", NumberLong(35)]}]}
                }
            }
        }],
        expected: [docs[1]]
    },
    // Test that when the query field is missing, we get the correct results.
    {
        pipeline: [{
            $geoNear: {
                near: {type: "Point", coordinates: [0, 0]},
                distanceField: "distance",
                key: "location",
            }
        }],
        expected: [docs[0], docs[1], docs[2], docs[3]]
    },
    // Test that when no rewrites are needed on the query field, we get the correct results.
    {
        pipeline: [{
            $geoNear: {
                near: {type: "Point", coordinates: [0, 0]},
                distanceField: "distance",
                key: "location",
                query: {$expr: {$eq: ["$name", "A"]}}
            }
        }],
        expected: [docs[0]]
    },
    {
        // $match with a rewrite followed by $group.
        pipeline: [
            {$match: {$expr: {$in: ["$ssn", ["123", "456"]]}}},
            {$group: {_id: "$manager", numReports: {$count: {}}}}
        ],
        expected: [{_id: "B", numReports: 1}, {_id: "C", numReports: 1}, {_id: "A", numReports: 1}]
    },
    {
        // $group then $match. With the stages swapped, the $match is referencing fields which no
        // longer exist in the input documents and should filter out every document.
        pipeline: [
            {$group: {_id: "$manager", numReports: {$count: {}}}},
            {$match: {$expr: {$in: ["$ssn", ["123", "456"]]}}}
        ],
        expected: []
    },
    {
        // Alternate between stages that require rewriting and stages that do not.
        pipeline: [
            {$unwind: {path: "$location"}},
            {$match: {$expr: {$in: ['$age', [NumberLong(25), NumberLong(35), NumberLong(55)]]}}},
            {$match: {_id: {$in: [0, 1]}}},
            {$match: {$expr: {$eq: ["$ssn", "123"]}}},
        ],
        expected: [
            {_id: 0, ssn: "123", name: "A", manager: "B", age: NumberLong(25), location: 0},
            {_id: 0, ssn: "123", name: "A", manager: "B", age: NumberLong(25), location: 0}
        ]
    },
    {
        // Alternate between stages that require rewriting and stages that do not. Same as above but
        // with a permuted $match order.
        pipeline: [
            {$unwind: {path: "$location"}},
            {$match: {_id: {$in: [0, 1]}}},
            {$match: {$expr: {$in: ['$age', [NumberLong(25), NumberLong(35), NumberLong(55)]]}}},
            {$match: {$expr: {$eq: ["$ssn", "123"]}}},
        ],
        expected: [
            {_id: 0, ssn: "123", name: "A", manager: "B", age: NumberLong(25), location: 0},
            {_id: 0, ssn: "123", name: "A", manager: "B", age: NumberLong(25), location: 0}
        ]
    },
];

client.runEncryptionOperation(() => {
    for (const testData of aggTests) {
        runAggTest(testData.pipeline, coll, testData.expected, testData);
    }
});

const illegalTests = [
    {
        // While we could support this query, the rewrite to compare an encrypted field to anything
        // other than a constant is complicated. For simplicity, we enforce that FLE 2 encrypted
        // fields can be compared only to constants.
        run: () =>
            coll.find({$expr: {$eq: ["$ssn", {$cond: [{$eq: ['$name', "A"]}, "123", "456"]}]}})
                .toArray()
    },
    {
        // Same as above, but testing $in with non-constant expressions.
        run: () =>
            coll.find({
                    $expr: {
                        $in:
                            ["$ssn",
                             ["000", {$cond: [{$eq: ['$name', "A"]}, "123", "456"]}, "999"]]
                    }
                })
                .toArray()
    },
    {
        // Cannot compare an encrypted field to an object.
        run: () => coll.find({$expr: {$eq: ["$ssn", {a: 2}]}}).toArray()
    },
    {
        // Cannot compare an encrypted field to another field.
        run: () => coll.find({$expr: {$eq: ["$ssn", "$name"]}}).toArray()
    },
    {
        // Cannot reference an encrypted field in a find projection.
        run: () => coll.find({}, {name: {$eq: ["$ssn", "123"]}}).toArray()
    },
    {
        // Cannot reference an encrypted field in a $project.
        run: () => coll.aggregate({$project: {name: {$eq: ["$ssn", "123"]}}}).toArray()
    },
    {
        // This command fails because the safeContent information is projected out by the time we
        // we reach the $match stage, which needs the array to properly filter.
        run: () => coll.aggregate([{$project: {ssn: 1}}, {$match: {$expr: {$eq: ["$ssn", "123"]}}}])
                       .toArray()
    },
    {
        // Sanity check: querying as above but without $expr should also fail.
        run: () => coll.aggregate([{$project: {ssn: 1}}, {$match: {ssn: "123"}}]).toArray()
    }
];

client.runEncryptionOperation(() => {
    for (const testData of illegalTests) {
        let failed = false;
        let res;
        try {
            res = testData.run();
        } catch (e) {
            res = tojson(e);
            failed = true;
        }

        assert(failed, {testData, commandRes: res});
    }
});