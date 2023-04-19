/**
 * Tests that if the user defines a limit, we send a search command to mongot with that information.
 * @tags: [featureFlagSearchBatchSizeLimit]
 */
(function() {
"use strict";
load("src/mongo/db/modules/enterprise/jstests/search/lib/mongotmock.js");
load('jstests/libs/uuid_util.js');  // For getUUIDFromListCollections.
load("src/mongo/db/modules/enterprise/jstests/search/lib/shardingtest_with_mongotmock.js");

const dbName = "test";
const collName = "sharded_batchsize";
const chunkBoundary = 8;

const docs = [
    {"_id": 1, "title": "cakes"},
    {"_id": 2, "title": "cookies and cakes"},
    {"_id": 3, "title": "vegetables"},
    {"_id": 4, "title": "oranges"},
    {"_id": 5, "title": "cakes and oranges"},
    {"_id": 6, "title": "cakes and apples"},
    {"_id": 7, "title": "apples"},
    {"_id": 8, "title": "cakes and xyz"},
    {"_id": 9, "title": "cakes and blueberries"},
    {"_id": 10, "title": "cakes and strawberries"},
    {"_id": 11, "title": "cakes and raspberries"},
    {"_id": 12, "title": "cakes and cakes"},
    {"_id": 13, "title": "cakes and spinach"},
    {"_id": 14, "title": "cakes and carrots"},
    {"_id": 15, "title": "cakes and more cakes"},
    {"_id": 16, "title": "cakes and even more cakes"},
];

const searchQuery = {
    query: "cakes",
    path: "title"
};

function searchCmd(uuid, extractedLimit) {
    return {
        search: collName,
        collectionUUID: uuid,
        query: searchQuery,
        $db: dbName,
        cursorOptions: {docsRequested: extractedLimit},
    };
}

// All the documents that would be returned by the search query above.
let relevantDocs = [];
let relevantSearchDocs = [];
let relevantSearchDocsShard0 = [];
let relevantSearchDocsShard1 = [];
let searchScore = 0.300;
for (let i = 0; i < docs.length; i++) {
    if (docs[i]["title"].includes(searchQuery.query)) {
        relevantDocs.push(docs[i]);

        // Standalone case.
        relevantSearchDocs.push({_id: docs[i]._id, $searchScore: searchScore});

        // Sharded environment case.
        if (docs[i]._id < chunkBoundary) {
            relevantSearchDocsShard0.push({_id: docs[i]._id, $searchScore: searchScore});
        } else {
            relevantSearchDocsShard1.push({_id: docs[i]._id, $searchScore: searchScore});
        }

        // The documents with lower _id will have a higher search score.
        searchScore = searchScore - 0.001;
    }
}

// Mongot may return slightly more documents than mongod requests as an optimization for the case
// when $idLookup filters out some of them.
function calcNumDocsMongotShouldReturn(extractedLimit) {
    return Math.max(Math.ceil(1.064 * extractedLimit), 10);
}

function buildHistoryStandalone(coll, collUUID, extractedLimit, mongotConn) {
    let mongotReturnedDocs = calcNumDocsMongotShouldReturn(extractedLimit);
    {
        const cursorId = NumberLong(123);
        const history = [
            {
                expectedCommand: searchCmd(collUUID, extractedLimit),
                response: {
                    cursor: {
                        id: NumberLong(0),
                        ns: coll.getFullName(),
                        nextBatch: relevantSearchDocs.slice(0, mongotReturnedDocs),
                    },
                    ok: 1
                }
            },
        ];
        assert.commandWorked(
            mongotConn.adminCommand({setMockResponses: 1, cursorId: cursorId, history: history}));
    }
}

function buildHistoryShardedEnv(coll, collUUID, extractedLimit, stWithMock) {
    let mongotReturnedDocs = calcNumDocsMongotShouldReturn(extractedLimit);
    {
        const cursorId = NumberLong(123);

        // Set history for shard 0.
        const history0 = [
            {
                expectedCommand: searchCmd(collUUID, extractedLimit),
                response: {
                    cursor: {
                        id: NumberLong(0),
                        ns: coll.getFullName(),
                        nextBatch: relevantSearchDocsShard0.slice(0, mongotReturnedDocs),
                    },
                    ok: 1
                }
            },
        ];
        const s0Mongot = stWithMock.getMockConnectedToHost(stWithMock.st.rs0.getPrimary());
        s0Mongot.setMockResponses(history0, cursorId);

        // Set history for shard 1.
        const history1 = [
            {
                expectedCommand: searchCmd(collUUID, extractedLimit),
                response: {
                    cursor: {
                        id: NumberLong(0),
                        ns: coll.getFullName(),
                        nextBatch: relevantSearchDocsShard1.slice(0, mongotReturnedDocs),
                    },
                    ok: 1
                }
            },
        ];
        const s1Mongot = stWithMock.getMockConnectedToHost(stWithMock.st.rs1.getPrimary());
        s1Mongot.setMockResponses(history1, cursorId);

        mockPlanShardedSearchResponse(
            collName, searchQuery, dbName, undefined /*sortSpec*/, stWithMock);
    }
}

function runAndAssert(
    pipeline, extractedLimit, expectedResults, coll, collUUID, standaloneConn, stConn) {
    // Only one of standaloneConn and stConn can be non-null.
    if (standaloneConn != null) {
        assert(stConn == null);
        buildHistoryStandalone(coll, collUUID, extractedLimit, standaloneConn);
    } else {
        assert(standaloneConn == null);
        buildHistoryShardedEnv(coll, collUUID, extractedLimit, stConn);
    }
    let cursor = coll.aggregate(pipeline);
    assert.eq(expectedResults, cursor.toArray());
}

// The extractable limit optimization cannot be done if there is a stage between $search and
// $limit that would change the number of documents, such as $match. Thus, there should be no
// 'docsRequested' field in the command sent to mongot in this case.
function expectNoDocsRequestedInCommand(coll, collUUID, mongotConn, stWithMock) {
    let pipeline = [{$search: searchQuery}, {$match: {title: {$regex: "more cakes"}}}, {$limit: 1}];
    let cursorId = NumberLong(123);

    // Only one of mongotConn and stWithMock can be non-null.
    if (mongotConn != null) {
        assert(stWithMock == null);
        const history = [
            {
                expectedCommand: {
                    search: coll.getName(),
                    collectionUUID: collUUID,
                    query: searchQuery,
                    $db: dbName,
                },
                response: {
                    cursor: {
                        id: NumberLong(0),
                        ns: coll.getFullName(),
                        nextBatch: [
                            {_id: 15, $searchScore: 0.789},
                            {_id: 16, $searchScore: 0.123},
                        ]
                    },
                    ok: 1
                }
            },
        ];
        assert.commandWorked(
            mongotConn.adminCommand({setMockResponses: 1, cursorId: cursorId, history: history}));
    } else {
        assert(mongotConn == null);
        // Set history for shard 0.
        const history0 = [
            {
                expectedCommand: {
                    search: coll.getName(),
                    collectionUUID: collUUID,
                    query: searchQuery,
                    $db: dbName,
                },
                response: {
                    cursor: {
                        id: NumberLong(0),
                        ns: coll.getFullName(),
                        nextBatch: [],
                    },
                    ok: 1
                }
            },
        ];
        const s0Mongot = stWithMock.getMockConnectedToHost(stWithMock.st.rs0.getPrimary());
        s0Mongot.setMockResponses(history0, cursorId);

        // Set history for shard 1.
        const history1 = [
            {
                expectedCommand: {
                    search: coll.getName(),
                    collectionUUID: collUUID,
                    query: searchQuery,
                    $db: dbName,
                },
                response: {
                    cursor: {
                        id: NumberLong(0),
                        ns: coll.getFullName(),
                        nextBatch: [
                            {_id: 15, $searchScore: 0.789},
                            {_id: 16, $searchScore: 0.123},
                        ]
                    },
                    ok: 1
                }
            },
        ];
        const s1Mongot = stWithMock.getMockConnectedToHost(stWithMock.st.rs1.getPrimary());
        s1Mongot.setMockResponses(history1, cursorId);

        mockPlanShardedSearchResponse(
            collName, searchQuery, dbName, undefined /*sortSpec*/, stWithMock);
    }
    let cursor = coll.aggregate(pipeline);
    const expected = [
        {"_id": 15, "title": "cakes and more cakes"},
    ];
    assert.eq(expected, cursor.toArray());
}

// Perform a $search query where $$SEARCH_META is referenced after a $limit stage.
function searchMetaAfterLimit(coll, collUUID, stWithMock) {
    let st = stWithMock.st;
    let limit = 3;
    let mongotReturnedDocs = calcNumDocsMongotShouldReturn(limit);

    let pipeline =
        [{$search: searchQuery}, {$limit: limit}, {$project: {_id: 1, meta: "$$SEARCH_META"}}];
    let expected = relevantDocs.slice(0, limit);
    // Modify the expected documents to reflect the $project stage in the pipeline.
    for (let i = 0; i < expected.length; i++) {
        expected[i] = {_id: expected[i]["_id"], meta: {value: 0}};
    }

    // Set history for shard 0.
    {
        const resultsID = NumberLong(11);
        const metaID = NumberLong(12);
        const historyResults = [
            {
                expectedCommand: searchCmd(collUUID, limit),
                response: {
                    ok: 1,
                    cursors: [
                        {
                            cursor: {
                                id: NumberLong(0),
                                type: "results",
                                ns: coll.getFullName(),
                                nextBatch: relevantSearchDocsShard0.slice(0, mongotReturnedDocs)
                            },
                            ok: 1
                        },
                        {
                            cursor: {
                                id: NumberLong(0),
                                ns: coll.getFullName(),
                                type: "meta",
                                nextBatch: [{value: 0}],
                            },
                            ok: 1
                        }
                    ]
                }
            },
        ];
        const mongot = stWithMock.getMockConnectedToHost(st.rs0.getPrimary());
        mongot.setMockResponses(historyResults, resultsID, metaID);
    }

    // Set history for shard 1.
    {
        const resultsID = NumberLong(21);
        const metaID = NumberLong(22);
        const historyResults = [
            {
                expectedCommand: searchCmd(collUUID, limit),
                response: {
                    ok: 1,
                    cursors: [
                        {
                            cursor: {
                                id: NumberLong(0),
                                type: "results",
                                ns: coll.getFullName(),
                                nextBatch: relevantSearchDocsShard1.slice(0, mongotReturnedDocs)

                            },
                            ok: 1
                        },
                        {
                            cursor: {
                                id: NumberLong(0),
                                ns: coll.getFullName(),
                                type: "meta",
                                nextBatch: [{value: 0}],
                            },
                            ok: 1
                        }
                    ]
                }
            },
        ];
        const mongot = stWithMock.getMockConnectedToHost(st.rs1.getPrimary());
        mongot.setMockResponses(historyResults, resultsID, metaID);
    }

    // Set history for mongos.
    {
        const mergingPipelineHistory = [{
            expectedCommand: {
                planShardedSearch: collName,
                query: searchQuery,
                $db: dbName,
                searchFeatures: {shardedSort: 1}
            },
            response: {
                ok: 1,
                protocolVersion: NumberInt(1),
                // This does not represent an actual merging pipeline. The merging pipeline is
                // arbitrary, it just must only generate one document.
                metaPipeline: [{$limit: 1}]
            }
        }];
        const mongot = stWithMock.getMockConnectedToHost(stWithMock.st.s);
        mongot.setMockResponses(mergingPipelineHistory, 1);
    }

    let cursor = coll.aggregate(pipeline);
    assert.eq(expected, cursor.toArray());
}

function runTest(coll, collUUID, standaloneConn, stConn) {
    function runSearchQueries(limitVal, otherLimitVal, skipVal) {
        // Perform a $search query with a limit stage.
        let pipeline = [{$search: searchQuery}, {$limit: limitVal}];
        // The extracted limit here comes from the limit value in the pipeline.
        let expected = relevantDocs.slice(0, limitVal);
        runAndAssert(pipeline, limitVal, expected, coll, collUUID, standaloneConn, stConn);

        // Perform a $search query with a $skip followed by $limit.
        pipeline = [{$search: searchQuery}, {$skip: skipVal}, {$limit: limitVal}];
        // The extracted limit here comes from the sum of the limit and skip values in the pipeline.
        expected = relevantDocs.slice(skipVal).slice(0, limitVal);
        runAndAssert(
            pipeline, limitVal + skipVal, expected, coll, collUUID, standaloneConn, stConn);

        // Perform a $search query with multiple limit stages.
        pipeline = [{$search: searchQuery}, {$limit: limitVal}, {$limit: otherLimitVal}];
        // The extracted limit here comes from the minimum of the two limit values in the pipeline.
        expected = relevantDocs.slice(0, Math.min(limitVal, otherLimitVal));
        runAndAssert(pipeline,
                     Math.min(limitVal, otherLimitVal),
                     expected,
                     coll,
                     collUUID,
                     standaloneConn,
                     stConn);

        // Perform a $search query with a limit and multiple skip stages.
        pipeline = [{$search: searchQuery}, {$skip: skipVal}, {$skip: skipVal}, {$limit: limitVal}];
        // The extracted limit here comes from the value of the limit plus the values of the two
        // skip stages in the pipeline.
        expected = relevantDocs.slice(skipVal + skipVal).slice(0, limitVal);
        runAndAssert(pipeline,
                     skipVal + skipVal + limitVal,
                     expected,
                     coll,
                     collUUID,
                     standaloneConn,
                     stConn);

        // Perform a $search query with multiple limit stages and multiple skip stages.
        pipeline = [
            {$search: searchQuery},
            {$skip: skipVal},
            {$skip: skipVal},
            {$limit: limitVal},
            {$limit: otherLimitVal}
        ];
        // The extracted limit here comes from the minimum of the two limit values plus the values
        // of the two skip stages in the pipeline.
        expected =
            relevantDocs.slice(skipVal + skipVal).slice(0, Math.min(limitVal, otherLimitVal));
        runAndAssert(pipeline,
                     skipVal + skipVal + Math.min(limitVal, otherLimitVal),
                     expected,
                     coll,
                     collUUID,
                     standaloneConn,
                     stConn);
    }

    // Run the search queries with limit and skip values such that mongod will extract a user limit
    // of less than 10, which means we will exercise the branch where mongot returns a minimum of 10
    // documents.
    runSearchQueries(3 /* limitVal */, 2 /* otherLimitVal */, 1 /* skipVal */);

    // Run the search queries with limit and skip values such that mongod will extract a user limit
    // of greater than 10, which means we will exercise the branch where mongot the extracted limit
    // multiplied by the oversubscription factor.
    runSearchQueries(11 /* limitVal */, 10 /* otherLimitVal */, 1 /* skipVal */);

    expectNoDocsRequestedInCommand(coll, collUUID, standaloneConn, stConn);

    // Test that the docsRequested field makes it to the shards in a sharded environment when
    // $$SEARCH_META is referenced in the query.
    if (stConn != null) {
        searchMetaAfterLimit(coll, collUUID, stConn);
    }
}

function setupAndRunTestStandalone() {
    const mongotmock = new MongotMock();
    mongotmock.start();
    const mongotConn = mongotmock.getConnection();
    const conn = MongoRunner.runMongod({setParameter: {mongotHost: mongotConn.host}});
    let db = conn.getDB(dbName);
    let coll = db.getCollection(collName);

    // Insert documents.
    assert.commandWorked(coll.insertMany(docs));

    let collUUID = getUUIDFromListCollections(db, collName);
    runTest(coll, collUUID, mongotConn, null /* stConn */);

    MongoRunner.stopMongod(conn);
    mongotmock.stop();
}

function setupAndRunTestShardedEnv() {
    const stWithMock = new ShardingTestWithMongotMock({
        name: "search_batchsize",
        shards: {
            rs0: {nodes: 2},
            rs1: {nodes: 2},
        },
        mongos: 1,
        other: {
            rsOptions: {setParameter: {enableTestCommands: 1}},
        }
    });
    stWithMock.start();
    let st = stWithMock.st;
    let mongos = st.s;
    let db = mongos.getDB(dbName);
    let coll = db.getCollection(collName);

    // Insert documents.
    assert.commandWorked(coll.insertMany(docs));

    // Shard the collection, split it at {_id: chunkBoundary}, and move the higher chunk to shard1.
    assert.commandWorked(mongos.getDB("admin").runCommand({enableSharding: dbName}));
    st.ensurePrimaryShard(dbName, st.shard0.name);
    st.shardColl(coll, {_id: 1}, {_id: chunkBoundary}, {_id: chunkBoundary + 1});

    let collUUID = getUUIDFromListCollections(st.rs0.getPrimary().getDB(dbName), collName);
    runTest(coll, collUUID, null /* standaloneConn */, stWithMock);

    stWithMock.stop();
}

// Test standalone.
setupAndRunTestStandalone();

// Test sharded cluster.
setupAndRunTestShardedEnv();
})();
