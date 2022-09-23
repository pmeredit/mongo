/**
 * Test rewrites of agg expressions over encrypted fields for FLE2.
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

// Run an update with the provided filter on the provided collection, do a dummy modification, and
// assert the number of modified documents is at least one if expected is non-empty. Multi-document
// updates are not allowed with FLE 2, so we only use updateOne() here.
const runUpdateTest = (filter, collection, expected, extraInfo) => {
    let res = assert.commandWorked(collection.updateOne(filter, {$set: {"updateField": 1}}));
    assert.eq(res.modifiedCount, expected.length > 0 ? 1 : 0, extraInfo);
};

const matchFilters = [
    // Simple tests for single encrypted field comparisons using $eq, $ne, and $in.
    {filter: {$expr: {$eq: ['$ssn', "123"]}}, expected: [docs[0], docs[3]]},
    {filter: {$expr: {$ne: ['$ssn', "123"]}}, expected: [docs[1], docs[2]]},
    {filter: {$expr: {$in: ['$ssn', ["123"]]}}, expected: [docs[0], docs[3]]},
    {filter: {$expr: {$in: ['$ssn', ["123", "456"]]}}, expected: [docs[0], docs[1], docs[3]]},

    // Test the reverse order of $eq arguments. Note that the corresponding queries for $in are
    // not permitted within QA, e.g. {$expr: {$in: ["123", "$ssn"]}}.
    {filter: {$expr: {$eq: ["123", "$ssn"]}}, expected: [docs[0], docs[3]]},

    // Similar to the above, but only querying non-encrypted fields.
    {filter: {$expr: {$eq: ['$_id', 0]}}, expected: [docs[0]]},
    {filter: {$expr: {$eq: ['$name', "B"]}}, expected: [docs[1]]},
    {filter: {$expr: {$in: ['$name', ["C", "D"]]}}, expected: [docs[2], docs[3]]},
    {filter: {$expr: {$eq: ['$name', "$name"]}}, expected: [docs[0], docs[1], docs[2], docs[3]]},
    {
        filter: {$expr: {$and: [{$in: ['$name', ["C", "D"]]}, {$eq: ['$_id', 2]}]}},
        expected: [docs[2]]
    },

    // Similar to the above, but querying with (to-be-encrypted) constants which do not exist in
    // the collection.
    {filter: {$expr: {$eq: ['$ssn', "999"]}}, expected: []},
    {filter: {$expr: {$ne: ['$ssn', "999"]}}, expected: [docs[0], docs[1], docs[2], docs[3]]},
    {filter: {$expr: {$in: ['$ssn', ["123", "999"]]}}, expected: [docs[0], docs[3]]},
    {filter: {$expr: {$in: ['$ssn', ["999", "111"]]}}, expected: []},
    {filter: {$expr: {$in: ['$ssn', []]}}, expected: []},

    // Test more complicated $expr shapes while querying multiple encrypted values.
    {
        filter: {$and: [{$expr: {$eq: ['$ssn', "123"]}}, {$expr: {$eq: ['$age', NumberLong(55)]}}]},
        expected: [docs[3]]
    },
    {
        filter: {
            $expr: {
                $and: [
                    {$in: ['$ssn', ["123", "456"]]},
                    {$in: ['$age', [NumberLong(55), NumberLong(35), NumberLong(60)]]}
                ]
            }
        },
        expected: [docs[1], docs[3]]
    },
    {
        filter: {
            $expr:
                {$or: [{$eq: ['$ssn', "123"]}, {$in: ['$age', [NumberLong(35), NumberLong(60)]]}]}
        },
        expected: [docs[0], docs[1], docs[3]]
    },
    {
        filter: {$and: [{$expr: {$eq: ['$ssn', "123"]}}, {age: {$eq: NumberLong(55)}}]},
        expected: [docs[3]]
    },

    // Test queries including comparisons to both encrypted and unencrypted fields.
    {
        filter: {$and: [{$expr: {$eq: ['$ssn', "123"]}}, {$expr: {$eq: ['$_id', 0]}}]},
        expected: [docs[0]]
    },
    {filter: {$expr: {$and: [{$eq: ['$ssn', "123"]}, {$eq: ['$_id', 0]}]}}, expected: [docs[0]]},
    {
        filter: {$expr: {$and: [{$in: ['$ssn', ["123", "456"]]}, {$eq: ["$name", "B"]}]}},
        expected: [docs[1]]
    }
];

// Run each of the filters above within a find and an aggregate. The results should be consistent.
for (const testData of matchFilters) {
    runAggTest([{$match: testData.filter}], coll, testData.expected, testData);
    runFindTest(testData.filter, coll, testData.expected, testData);
    runUpdateTest(testData.filter, coll, testData.expected, testData);
}

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
    // Similar to the above, test that $geoNear undergoes rewrites for the query field.
    {
        pipeline: [
            {
                $geoNear: {
                    near: {type: "Point", coordinates: [0, 0]},
                    distanceField: "distance",
                    key: "location",
                    query: {$expr: {$and: [{$in: ["$ssn", ["789", "456"]]}, {$eq: ["$age", NumberLong(35)]}]}}
                }
            }
        ],
        expected: [
            docs[1]
        ]
    },
    // Test that when the query field is missing, we get the correct results.
    {
        pipeline: [
            {
                $geoNear: {
                    near: {type: "Point", coordinates: [0, 0]},
                    distanceField: "distance",
                    key: "location",
                }
            }
        ],
        expected: [
            docs[0], docs[1], docs[2], docs[3]
        ]
    },
    // Test that when no rewrites are needed on the query field, we get the correct results.
    {
        pipeline: [
            {
                $geoNear: {
                    near: {type: "Point", coordinates: [0, 0]},
                    distanceField: "distance",
                    key: "location",
                    query: {$expr: {$eq: ["$name", "A"]}}
                }
            }
        ],
        expected: [
            docs[0]
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

for (const testData of aggTests) {
    runAggTest(testData.pipeline, coll, testData.expected, testData);
}

const illegalTests = [
    {
        // Cannot reference an encrypted field in a pipeline update.
        run: () => coll.updateOne(
            {}, [{$set: {"updateField": {$cond: [{$eq: ["$ssn", "123"]}, "123", "not123"]}}}])
    },
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
}());
