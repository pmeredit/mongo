/**
 * Test aggregations on encrypted collections.
 *
 * @tags: [
 *   __TEMPORARILY_DISABLED__,
 * ]
 */

load('jstests/aggregation/extras/utils.js');  // For assertArrayEq.
load("jstests/fle2/libs/encrypted_client_util.js");

(function() {

// Set up the encrypted collection.
const dbName = "aggregateDB";
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
    assert.commandWorked(coll.insert(doc));
}
assert.commandWorked(coll.createIndex({location: "2dsphere"}));

// Run the pipeline on the provided collection, and assert that the results are equivalent to
// 'expected'. The pipeline is appended with a $project stage to project out safeContent data
// and other fields that are inconvenient to have in the output.
const runTest = (pipeline, options, collection, expected, extraInfo) => {
    const aggPipeline = pipeline.slice();
    aggPipeline.push(
        {$project: {[kSafeContentField]: 0, [`chain.${kSafeContentField}`]: 0, distance: 0}});
    const result = collection.aggregate(aggPipeline, options).toArray();
    assertArrayEq({actual: result, expected: expected, extraErrorMsg: tojson(extraInfo)});
};

// Same as above but with batchSize=1 to check that the pipeline runs correctly across getMore's.
const runTestWithGetMores = (pipeline, collection, expected, extraInfo) => {
    const extraErrorMsg = Object.assign({}, extraInfo, {withGetMores: true});
    return runTest(pipeline, {cursor: {batchSize: 1}}, collection, expected, extraErrorMsg);
};

const tests = [
    //
    // Tests that check that single-stage aggregations which reference encrypted fields return the
    // correct results.
    //

    // $match with a filter on an encrypted field ($eq).
    {pipeline: [{$match: {ssn: "123"}}], expected: [docs[0], docs[3]]},
    // $match with a compound filter on an encrypted field ($eq) and a non-encrypted field.
    {pipeline: [{$match: {ssn: "123", _id: 0}}], expected: [docs[0]]},
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
    // $geoNear with a filter on an encrypted field ($in).
    {
        pipeline: [
            {
                $geoNear: {
                    near: {type: "Point", coordinates: [0, 0]},
                    distanceField: "distance",
                    key: "location",
                    query: {ssn: {$in: ["789", "456"]}}
                }
            }
        ],
        expected: [
            docs[1],
            docs[2]
        ]
    },
    // $geoNear with a compound filter on an encrypted field ($ne) and a non-encrypted field.
    {
        pipeline: [
            {
                $geoNear: {
                    near: {type: "Point", coordinates: [0, 0]},
                    distanceField: "distance",
                    key: "location",
                    query: {ssn: {$ne: "123"}, _id: 1}
                }
            }
        ],
        expected: [
            docs[1]
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
    // $match on a non-encrypted field.
    {pipeline: [{$match: {name: "A"}}], expected: [docs[0]]},
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
    // $match on an encrypted field followed by a group with no references to encrypted fields.
    {
        pipeline: [{$match: {ssn: {$in: ["123", "456"]}}}, {$group: {_id: "$manager", numReports: {$count: {}}}}],
        expected: [{_id: "B", numReports: 1}, {_id: "C", numReports: 1}, {_id: "A", numReports: 1}]
    },
    // $project including an encrypted field.
    {
        pipeline: [{$project: {_id: 0, ssn: 1}}],
        expected: [{ssn: "123"}, {ssn: "456"}, {ssn: "789"}, {ssn: "123"}]
    },

    //
    // Tests that check that when there are multiple encrypted fields referenced in the pipeline,
    // each field is handled appropriately.
    //

    // $match on two encrypted fields ($eq).
    {pipeline: [{$match: {ssn: "123", age: NumberLong(55)}}], expected: [docs[3]]},
    // $match on two encrypted fields ($or, $and, $in, $ne, $eq).
    {
        pipeline: [{
            $match: {
                $or: [
                    {$and: [{ssn: "123"}, {age: NumberLong(55)}]},
                    {$and: [{ssn: {$in: ["456", "789"]}}, {age: {$ne: NumberLong(25)}}]}
                ]
            }
        }],
        expected: [docs[3], docs[2], docs[1]]},
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
    // $geoNear which filters on two encrypted fields ($in, $eq).
    {
        pipeline: [
            {
                $geoNear: {
                    near: {type: "Point", coordinates: [0, 0]},
                    distanceField: "distance",
                    key: "location",
                    query: {ssn: {$in: ["789", "456"]}, age: NumberLong(35)}
                }
            }
        ],
        expected: [
            docs[1]
        ]
    },

];

// Run all of the tests.
for (const testData of tests) {
    const extraInfo = Object.assign({transaction: false}, testData);
    runTest(testData.pipeline, {}, coll, testData.expected, extraInfo);

    runTestWithGetMores(testData.pipeline, coll, testData.expected, extraInfo);
}

// Run the same tests, this time in a transaction.
const session = edb.getMongo().startSession({causalConsistency: false});
const sessionDB = session.getDatabase(dbName);
const sessionColl = sessionDB.getCollection(collName);

for (const testData of tests) {
    const extraInfo = Object.assign({transaction: true}, testData);
    session.startTransaction();
    runTest(testData.pipeline, {}, sessionColl, testData.expected, extraInfo);
    session.commitTransaction();

    session.startTransaction();
    runTestWithGetMores(testData.pipeline, sessionColl, testData.expected, extraInfo);
    session.commitTransaction();
}
}());
