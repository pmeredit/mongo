/**
 * Test of `$search` aggregation stage within $unionWith and $lookup stages.
 */
(function() {
"use strict";
load("src/mongo/db/modules/enterprise/jstests/search/lib/mongotmock.js");
load('jstests/libs/uuid_util.js');  // For getUUIDFromListCollections.

// Set up mongotmock and point the mongod to it.
const mongotmock = new MongotMock();
mongotmock.start();
const mongotConn = mongotmock.getConnection();

const conn = MongoRunner.runMongod({setParameter: {mongotHost: mongotConn.host}});
const dbName = jsTestName();
const db = conn.getDB(dbName);
const coll = db.search;
coll.drop();

const collBase = db.base;
collBase.drop();

const collBase2 = db.base2;
collBase2.drop();

const view1 = db.view1;
view1.drop();

assert.commandWorked(coll.insert({"_id": 1, "title": "cakes"}));
assert.commandWorked(coll.insert({"_id": 2, "title": "cookies and cakes"}));
assert.commandWorked(coll.insert({"_id": 3, "title": "vegetables"}));
assert.commandWorked(coll.insert({"_id": 4, "title": "oranges"}));
assert.commandWorked(coll.insert({"_id": 5, "title": "cakes and oranges"}));
assert.commandWorked(coll.insert({"_id": 6, "title": "cakes and apples"}));
assert.commandWorked(coll.insert({"_id": 7, "title": "apples"}));
assert.commandWorked(coll.insert({"_id": 8, "title": "cakes and kale"}));

assert.commandWorked(collBase.insert({"_id": 100, "localField": "cakes", "weird": false}));
assert.commandWorked(collBase.insert({"_id": 101, "localField": "cakes and kale", "weird": true}));

assert.commandWorked(collBase2.insert({"_id": 200, "ref_id": 1}));
assert.commandWorked(collBase2.insert({"_id": 201, "ref_id": 8}));

const collUUID = getUUIDFromListCollections(db, coll.getName());

var cursorCounter = 123;

function setupSearchQuery(term, times, batch, searchMetaValue) {
    const searchQuery = {query: term, path: "title"};
    const searchCmd = mongotCommandForQuery(searchQuery, coll.getName(), dbName, collUUID);

    // Give mongotmock some stuff to return.
    for (let i = 0; i < times; i++) {
        const cursorId = NumberLong(cursorCounter++);
        const history = [{
            expectedCommand: searchCmd,
            response: {
                cursor: {id: NumberLong(0), ns: coll.getFullName(), nextBatch: batch},
                vars: {SEARCH_META: {value: searchMetaValue}},
                ok: 1
            }
        }];

        assert.commandWorked(
            mongotConn.adminCommand({setMockResponses: 1, cursorId: cursorId, history: history}));
    }
    return searchQuery;
}

const lookupSearchQuery = setupSearchQuery("cakes",
                                           2,
                                           [
                                               {_id: 1, $searchScore: 0.9},
                                               {_id: 2, $searchScore: 0.8},
                                               {_id: 5, $searchScore: 0.7},
                                               {_id: 6, $searchScore: 0.6},
                                               {_id: 8, $searchScore: 0.5}
                                           ],
                                           1);

const makeLookupPipeline = (fromColl, stage) =>
        [
            {$project: {"_id": 0}},
            {
                $lookup: {
                    from: fromColl,
                    let: { local_title: "$localField" },
                    pipeline: [
                        stage,
                        {
                            $match: {
                                $expr: {
                                    $eq: ["$title", "$$local_title"]
                                }
                            }
                        },
                        {
                            $project: {
                                "_id": 0,
                                "ref_id": "$_id"
                            }
                        }
                    ],
                    as: "cake_data"
                }
            }
        ];

const makeLookupSearchPipeline = (fromColl) =>
    makeLookupPipeline(fromColl, {$search: lookupSearchQuery});

// Perform a $search query with $lookup.
const lookupCursor = collBase.aggregate(makeLookupSearchPipeline(coll.getName()));

const lookupExpected = [
    {"localField": "cakes", "weird": false, "cake_data": [{"ref_id": 1}]},
    {"localField": "cakes and kale", "weird": true, "cake_data": [{"ref_id": 8}]}
];
assert.sameMembers(lookupExpected, lookupCursor.toArray());

const unionSearchQuery = setupSearchQuery("cakes",
                                          1,
                                          [
                                              {_id: 1, $searchScore: 0.9},
                                              {_id: 2, $searchScore: 0.8},
                                              {_id: 5, $searchScore: 0.7},
                                              {_id: 6, $searchScore: 0.6},
                                              {_id: 8, $searchScore: 0.5}
                                          ],
                                          1);

const unionCursor = collBase.aggregate([
    {$project: {"localField": 1, "_id": 0}},
    {$unionWith: {coll: coll.getName(), pipeline: [{$search: unionSearchQuery}]}}
]);

const unionExpected = [
    {"localField": "cakes"},
    {"localField": "cakes and kale"},
    {"_id": 1, "title": "cakes"},
    {"_id": 2, "title": "cookies and cakes"},
    {"_id": 5, "title": "cakes and oranges"},
    {"_id": 6, "title": "cakes and apples"},
    {"_id": 8, "title": "cakes and kale"}
];
assert.sameMembers(unionExpected, unionCursor.toArray());

// Assert that if SEARCH_META is accessed in a query with multiple search type stages the query will
// fail. No need to setup a response on mongot, as we will fail before running the remote query.
assert.commandFailedWithCode(db.runCommand({
    aggregate: coll.getName(),
    pipeline: [
        {$search: {/*Not accessed*/}},
        {$addFields: {meta: "$$SEARCH_META"}},
        {
            $unionWith: {
                coll: coll.getName(),
                pipeline: [{$search: {/*Not accessed*/}}, {$addFields: {meta: "$$SEARCH_META"}}]
            }
        }
    ],
    cursor: {}
}),
                             6080010);

// Same test with $lookup.
assert.commandFailedWithCode(db.runCommand({
    aggregate: coll.getName(),
    pipeline: [
        {$search: {/*Not accessed*/}},
        {$addFields: {meta: "$$SEARCH_META"}},
        {
            $lookup: {
                from: coll.getName(),
                pipeline: [{$search: {/*Not accessed*/}}, {$addFields: {meta: "$$SEARCH_META"}}],
                as: "arr",
            }
        }
    ],
    cursor: {}
}),
                             6080010);

// $search stage in a sub-pipeline.
// Assert that if SEARCH_META is accessed in a query with a search stage in a sub-pipeline
// the query will fail. No need to setup a response on mongot, as we will fail before running the
// remote query.
assert.commandFailedWithCode(db.runCommand({
    aggregate: coll.getName(),
    pipeline: [
        {$match: {val: 1}},
        {
            $unionWith: {
                coll: coll.getName(),
                pipeline: [{$search: {/*Not accessed*/}}, {$addFields: {meta: "$$SEARCH_META"}}]
            }
        },
    ],
    cursor: {}
}),
                             6080010);

assert.commandFailedWithCode(db.runCommand({
    aggregate: coll.getName(),
    pipeline: [
        {$match: {val: 1}},
        {$unionWith: {coll: coll.getName(), pipeline: [{$search: {/*Not accessed*/}}]}},
        {$addFields: {meta: "$$SEARCH_META"}},
    ],
    cursor: {}
}),
                             6080010);

// $$SEARCH_META before $unionWith ($search in pipeline)
assert.commandFailedWithCode(db.runCommand({
    aggregate: coll.getName(),
    pipeline: [
        {$addFields: {meta: "$$SEARCH_META"}},
        {$unionWith: {coll: coll.getName(), pipeline: [{$search: {/*Not accessed*/}}]}},
    ],
    cursor: {}
}),
                             6080010);

// Same test with $lookup.
assert.commandFailedWithCode(db.runCommand({
    aggregate: coll.getName(),
    pipeline: [
        {$match: {val: 1}},
        {
            $lookup: {
                from: coll.getName(),
                pipeline: [{$search: {/*Not accessed*/}}],
                as: "arr",
            }
        },
        {$addFields: {meta: "$$SEARCH_META"}},
    ],
    cursor: {}
}),
                             6080010);

// Test with $lookup ($searchMeta in pipeline).
assert.commandFailedWithCode(db.runCommand({
            aggregate: coll.getName(),
            pipeline: [
                {$match: {val: 1}},
                {
                    $lookup: {
                        from: coll.getName(),
                        pipeline: [{$searchMeta: {/*Not accessed*/}}],
                        as: "arr",
                    }
                },
            ],
            cursor: {}
        }),
        6080010);
// Reset the history for the next test.
let cookiesQuery = setupSearchQuery("cookies",
                                    1,
                                    [
                                        {_id: 2, $searchScore: 0.6},
                                    ],
                                    2);
// This is used in 'unionSearchQuery' in the next test.
setupSearchQuery("cakes",
                 1,
                 [
                     {_id: 1, $searchScore: 0.9},
                     {_id: 2, $searchScore: 0.8},
                     {_id: 5, $searchScore: 0.7},
                     {_id: 6, $searchScore: 0.6},
                     {_id: 8, $searchScore: 0.5}
                 ],
                 1);
// Assert that if $$SEARCH_META is not accessed, multiple search-type stages are allowed.
const union2Result =
    coll.aggregate([
            {$search: cookiesQuery},
            {$unionWith: {coll: coll.getName(), pipeline: [{$search: unionSearchQuery}]}}
        ])
        .toArray();
const union2SearchExpected = [
    {
        "_id": 2,
        "title": "cookies and cakes",
    },
    {
        "_id": 1,
        "title": "cakes",
    },
    {
        "_id": 2,
        "title": "cookies and cakes",
    },
    {
        "_id": 5,
        "title": "cakes and oranges",
    },
    {
        "_id": 6,
        "title": "cakes and apples",
    },
    {
        "_id": 8,
        "title": "cakes and kale",
    }
];
assert.sameMembers(union2SearchExpected, union2Result);

assert.commandFailedWithCode(db.runCommand({
    aggregate: collBase.getName(),
    cursor: {},
    pipeline:
        [{$unionWith: {coll: coll.getName(), pipeline: [{$searchMeta: {/* Not Accessed */}}]}}]
}),
                             6080010);

assert.commandFailedWithCode(db.runCommand({
    aggregate: collBase.getName(),
    cursor: {},
    pipeline: [{
        $lookup:
            {from: coll.getName(), as: "arr", pipeline: [{$searchMeta: {/* Not Accessed */}}]}
    }]
}),
                             6080010);

// $searchMeta with $search will fail since $searchMeta accesses $$SEARCH_META.
assert.commandFailedWithCode(db.runCommand({
    aggregate: coll.getName(),
    pipeline: [
        {$search: {/* Not Accessed */}},
        {$unionWith: {coll: coll.getName(), pipeline: [{$searchMeta: {/* Not Accessed */}}]}}
    ],
    cursor: {}
}),
                             6080010);

// Multiple $searchMeta in the pipeline will fail.
assert.commandFailedWithCode(db.runCommand({
    aggregate: coll.getName(),
    pipeline: [
        {$searchMeta: {/* Not Accessed */}},
        {$unionWith: {coll: coll.getName(), pipeline: [{$searchMeta: {/* Not Accessed */}}]}}
    ],
    cursor: {}
}),
                             6080010);

// $search within a view use-case with $unionWith.
const viewSearchQuery = setupSearchQuery("cakes",
                                         1,
                                         [
                                             {_id: 1, $searchScore: 0.9},
                                             {_id: 2, $searchScore: 0.8},
                                             {_id: 5, $searchScore: 0.7},
                                             {_id: 6, $searchScore: 0.6},
                                             {_id: 8, $searchScore: 0.5}
                                         ],
                                         1);

assert.commandWorked(db.createView(view1.getName(), collBase.getName(), [
    {$project: {"localField": 1, "_id": 0}},
    {$unionWith: {coll: coll.getName(), pipeline: [{$search: viewSearchQuery}]}}
]));

const viewCursor = view1.aggregate([]);

const viewExpected = [
    {"localField": "cakes"},
    {"localField": "cakes and kale"},
    {"_id": 1, "title": "cakes"},
    {"_id": 2, "title": "cookies and cakes"},
    {"_id": 5, "title": "cakes and oranges"},
    {"_id": 6, "title": "cakes and apples"},
    {"_id": 8, "title": "cakes and kale"}
];
assert.sameMembers(viewExpected, viewCursor.toArray());

// $lookup of a view with $search coll.lookup(searchView, lookup-cond).
const viewSearchQuery2 = setupSearchQuery("cakes",
                                          2,
                                          [
                                              {_id: 1, $searchScore: 0.9},
                                              {_id: 2, $searchScore: 0.8},
                                              {_id: 5, $searchScore: 0.7},
                                              {_id: 6, $searchScore: 0.6},
                                              {_id: 8, $searchScore: 0.5}
                                          ],
                                          1);
view1.drop();
assert.commandWorked(db.createView(view1.getName(), coll.getName(), [{$search: viewSearchQuery2}]));

const lookupViewSearchCursor =
    collBase.aggregate(makeLookupPipeline(view1.getName(), {$project: {_id: 1, title: 1}}));

const lookupViewSearchExpected = [
    {"localField": "cakes", "weird": false, "cake_data": [{"ref_id": 1}]},
    {"localField": "cakes and kale", "weird": true, "cake_data": [{"ref_id": 8}]}
];
assert.sameMembers(lookupViewSearchExpected, lookupViewSearchCursor.toArray());

// $lookup (with $search) within $lookup.
const nestedLookupQuery = setupSearchQuery("cakes",
                                           2,
                                           [
                                               {_id: 1, $searchScore: 0.9},
                                               {_id: 2, $searchScore: 0.8},
                                               {_id: 5, $searchScore: 0.7},
                                               {_id: 6, $searchScore: 0.6},
                                               {_id: 8, $searchScore: 0.5}
                                           ],
                                           1);

const nestedInternalPipeline = makeLookupSearchPipeline(coll.getName());
nestedInternalPipeline.push({$unwind: "$cake_data"},
                            {$match: {$expr: {$eq: ["$cake_data.ref_id", "$$local_ref_id"]}}});
const nestedLookupPipeline = [
    {
        $lookup: {
            from: collBase.getName(),
            let : {local_ref_id: "$ref_id"},
            pipeline: nestedInternalPipeline,
            as: "refs"
        }
    },
    {$unwind: "$refs"}
];

const nested = collBase2.aggregate(nestedLookupPipeline);
const nestedLookupExpected = [
    {
        "_id": 200,
        "ref_id": 1,
        "refs": {"localField": "cakes", "weird": false, "cake_data": {"ref_id": 1}}
    },
    {
        "_id": 201,
        "ref_id": 8,
        "refs": {"localField": "cakes and kale", "weird": true, "cake_data": {"ref_id": 8}}
    }
];
assert.sameMembers(nestedLookupExpected, nested.toArray());

// $lookup against non-trivial view($search) fails.

view1.drop();
assert.commandWorked(db.createView(view1.getName(), coll.getName(), [
    {$project: {"_id": 1}},
]));

assert.commandFailedWithCode(db.runCommand({
    aggregate: collBase.getName(),
    pipeline: makeLookupSearchPipeline(view1.getName()),
    cursor: {}
}),
                             40602);

// $lookup against trivial view($search) works.
const lookupSearchViewQuery = setupSearchQuery("cakes",
                                               2,
                                               [
                                                   {_id: 1, $searchScore: 0.9},
                                                   {_id: 2, $searchScore: 0.8},
                                                   {_id: 5, $searchScore: 0.7},
                                                   {_id: 6, $searchScore: 0.6},
                                                   {_id: 8, $searchScore: 0.5}
                                               ],
                                               1);
view1.drop();
assert.commandWorked(db.createView(view1.getName(), coll.getName(), []));

assert.sameMembers(
    [
        {"localField": "cakes", "weird": false, "cake_data": [{"ref_id": 1}]},
        {"localField": "cakes and kale", "weird": true, "cake_data": [{"ref_id": 8}]}
    ],
    collBase.aggregate(makeLookupSearchPipeline(view1.getName())).toArray());

// $unionWith against non-trivial view($search) fails.
view1.drop();
assert.commandWorked(db.createView(view1.getName(), coll.getName(), [
    {$project: {"_id": 1}},
]));

assert.commandFailedWithCode(db.runCommand({
    aggregate: collBase.getName(),
    pipeline: [
        {$project: {"localField": 1, "_id": 0}},
        {$unionWith: {coll: view1.getName(), pipeline: [{$search: unionSearchQuery}]}}
    ],
    cursor: {}
}),
                             40602);

// $unionWith against trivial view($search) passes.
const unionSearchViewQuery = setupSearchQuery("cakes",
                                              1,
                                              [
                                                  {_id: 1, $searchScore: 0.9},
                                                  {_id: 2, $searchScore: 0.8},
                                                  {_id: 5, $searchScore: 0.7},
                                                  {_id: 6, $searchScore: 0.6},
                                                  {_id: 8, $searchScore: 0.5}
                                              ],
                                              1);
view1.drop();
assert.commandWorked(db.createView(view1.getName(), coll.getName(), []));

assert.sameMembers(
    [
        {"localField": "cakes"},
        {"localField": "cakes and kale"},
        {"_id": 1, "title": "cakes"},
        {"_id": 2, "title": "cookies and cakes"},
        {"_id": 5, "title": "cakes and oranges"},
        {"_id": 6, "title": "cakes and apples"},
        {"_id": 8, "title": "cakes and kale"}
    ],
    collBase
        .aggregate([
            {$project: {"localField": 1, "_id": 0}},
            {$unionWith: {coll: view1.getName(), pipeline: [{$search: unionSearchViewQuery}]}}
        ])
        .toArray());

// Verify localField/foreignField combination fails in $lookup with $search.
assert.commandFailedWithCode(db.runCommand({
    aggregate: collBase.getName(),
    pipeline: [
        {
            $lookup: {
                from: coll.getName(),
                localField: "localField",
                foreignField: "title",
                pipeline: [
                    {
                        $search: lookupSearchQuery
                    }
                ],
                as: "cake_data"
            }
        }
    ],
    cursor: {}
}),
                             40602);

MongoRunner.stopMongod(conn);
mongotmock.stop();
})();
