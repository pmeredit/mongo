/**
 * Verify that $search with 'returnStoredSource' returns both metadata and full documents.
 */
(function() {
"use strict";
load("src/mongo/db/modules/enterprise/jstests/search/lib/mongotmock.js");
load("jstests/libs/analyze_plan.js");  // For getAggPlanStages().
load('jstests/libs/uuid_util.js');     // For getUUIDFromListCollections.
load("src/mongo/db/modules/enterprise/jstests/search/lib/shardingtest_with_mongotmock.js");
load("jstests/aggregation/extras/utils.js");  // For arrayEq
load("jstests/libs/feature_flag_util.js");

const dbName = jsTestName();
const searchQuery = {
    query: "cakes",
    path: "title",
    returnStoredSource: true
};
(function testStandalone() {
    // Set up mongotmock and point the mongod to it.
    const mongotmock = new MongotMock();
    mongotmock.start();
    const mongotConn = mongotmock.getConnection();

    const conn = MongoRunner.runMongod({setParameter: {mongotHost: mongotConn.host}});

    let testDB = conn.getDB(dbName);

    const coll = testDB.searchCollector;
    coll.drop();
    assert.commandWorked(coll.insert([
        {"_id": 1, "title": "cakes"},
        {"_id": 2, "title": "cookies and cakes"},
        {"_id": 3, "title": "vegetables"}
    ]));

    const collUUID = getUUIDFromListCollections(testDB, coll.getName());
    const searchQuery = {query: "cakes", path: "title", returnStoredSource: true};

    const searchCmd = {
        search: coll.getName(),
        collectionUUID: collUUID,
        query: searchQuery,
        $db: dbName,
    };
    const cursorId = NumberLong(17);

    {
        const history = [{
            expectedCommand: searchCmd,
            response: {
                ok: 1,
                cursor: {
                    id: NumberLong(0),
                    ns: coll.getFullName(),
                    nextBatch: [
                        // Include a field not on mongod to make sure we are getting back mongot
                        // documents.
                        {
                            $searchScore: 0.654,
                            storedSource: {_id: 2, title: "cookies and cakes", tasty: true}
                        },
                        {$searchScore: 0.321, storedSource: {_id: 1, title: "cakes", tasty: true}},
                        {$searchScore: 0.123, storedSource: {title: "vegetables", tasty: false}},
                        // Ensure that if a returned document doesn't have a 'storedSource' field we
                        // can still return a corresponding document.
                        {_id: 4, $searchScore: .2},
                        // Ensure that if a returned document has an empty 'storedSource' field we
                        // can still return a corresponding document.
                        {$searchScore: 0.653, storedSource: {}}
                    ]
                },
                vars: {SEARCH_META: {value: 42}}
            }
        }];
        assert.commandWorked(
            mongotConn.adminCommand({setMockResponses: 1, cursorId, history: history}));

        let aggResults = coll.aggregate([
                                 {$search: searchQuery},
                                 {
                                     $project: {
                                         _id: 1,
                                         score: {$meta: "searchScore"},
                                         title: 1,
                                         tasty: 1,
                                         meta: "$$SEARCH_META"
                                     }
                                 }
                             ])
                             .toArray();
        let expected = [
            {_id: 2, score: 0.654, title: "cookies and cakes", tasty: true, meta: {value: 42}},
            {score: .653, meta: {value: 42}},
            {_id: 1, score: 0.321, title: "cakes", tasty: true, meta: {value: 42}},
            {score: 0.123, title: "vegetables", tasty: false, meta: {value: 42}},
            {_id: 4, score: .2, meta: {value: 42}},
        ];
        assert(arrayEq(expected, aggResults),
               "Expected:\n" + tojson(expected) + "\nGot:\n" + tojson(aggResults));
    }

    MongoRunner.stopMongod(conn);
    mongotmock.stop();
})();
// Repeat the test in a sharded environment.
(function testSharded() {
    const stWithMock = new ShardingTestWithMongotMock({
        name: "sharded_search",
        shards: {
            rs0: {nodes: 1},
            rs1: {nodes: 1},
        },
        mongos: 1,
    });
    stWithMock.start();
    const st = stWithMock.st;

    const mongos = st.s;
    const testDB = mongos.getDB(dbName);
    const coll = testDB.getCollection(jsTestName());
    const collNS = coll.getFullName();
    const useShardedFacets =
        FeatureFlagUtil.isEnabled(st.configRS.getPrimary().getDB(dbName), "SearchShardedFacets");

    // Documents that end up on shard0.
    assert.commandWorked(coll.insert([{_id: 1, shardKey: 0}, {_id: 2, shardKey: 0}]));
    // Documents that end up on shard1.
    assert.commandWorked(coll.insert([{_id: 11, shardKey: 100}, {_id: 12, shardKey: 100}]));

    // Shard the test collection, split it at {shardKey: 10}, and move the higher chunk to shard1.
    assert.commandWorked(coll.createIndex({shardKey: 1}));
    assert.commandWorked(testDB.adminCommand({enableSharding: dbName}));
    st.ensurePrimaryShard(dbName, st.shard0.name);
    st.shardColl(coll, {shardKey: 1}, {shardKey: 10}, {shardKey: 10 + 1});

    const shard0Conn = st.rs0.getPrimary();
    const shard1Conn = st.rs1.getPrimary();

    const collUUID0 = getUUIDFromListCollections(st.rs0.getPrimary().getDB(dbName), coll.getName());
    const collUUID1 = getUUIDFromListCollections(st.rs1.getPrimary().getDB(dbName), coll.getName());

    const responseOk = 1;

    const mongot0ResponseBatch = [
        {_id: 3, $searchScore: 29},
        {$searchScore: 10, storedSource: {_id: 2, old: true}},
        {$searchScore: 0.99, storedSource: {_id: 1, old: true}},
        {$searchScore: 0.654, storedSource: {}}
    ];
    const mongot1ResponseBatch = [
        {$searchScore: 111, storedSource: {_id: 11, old: false}},
        {_id: 13, $searchScore: 30},
        {$searchScore: 28, storedSource: {_id: 12, old: false}},
        {$searchScore: 0.456, storedSource: {}}
    ];
    let history0 = [];
    let history1 = [];
    let expectedDocs = [];
    const protocolVersion = NumberLong(42);
    if (useShardedFacets) {
        history0 = [{
            expectedCommand: {
                search: coll.getName(),
                collectionUUID: collUUID0,
                query: searchQuery,
                $db: dbName,
                intermediate: protocolVersion,
            },
            response: mongotMultiCursorResponseForBatch(mongot0ResponseBatch,
                                                        NumberLong(0),
                                                        [{val: 20}, {val: 30}],
                                                        NumberLong(0),
                                                        collNS,
                                                        responseOk),
        }];
        history1 = [{
            expectedCommand: {
                search: coll.getName(),
                collectionUUID: collUUID1,
                query: searchQuery,
                $db: dbName,
                intermediate: protocolVersion,
            },
            response: mongotMultiCursorResponseForBatch(mongot1ResponseBatch,
                                                        NumberLong(0),
                                                        [{val: 200}, {val: 300}],
                                                        NumberLong(0),
                                                        collNS,
                                                        responseOk),
        }];
        expectedDocs = [
            {_id: 11, old: false, score: 111, meta: 550},
            {_id: 13, score: 30, meta: 550},
            {_id: 3, score: 29, meta: 550},
            {_id: 12, old: false, score: 28, meta: 550},
            {_id: 2, old: true, score: 10, meta: 550},
            {_id: 1, old: true, score: .99, meta: 550},
            {score: .654, meta: 550},
            {score: .456, meta: 550},
        ];
        const mergingPipelineHistory = [{
            expectedCommand: {planShardedSearch: coll.getName(), query: searchQuery, $db: dbName},
            response: {
                ok: 1,
                protocolVersion: NumberInt(42),
                metaPipeline: [{$group: {_id: null, val: {$sum: "$val"}}}],
            }
        }];
        const mongot = stWithMock.getMockConnectedToHost(stWithMock.st.s);
        mongot.setMockResponses(mergingPipelineHistory, 1423);
    } else {
        history0 = [{
            expectedCommand: {
                search: coll.getName(),
                collectionUUID: collUUID0,
                query: searchQuery,
                $db: dbName
            },
            response:
                mongotResponseForBatch(mongot0ResponseBatch, NumberLong(0), collNS, responseOk),
        }];
        history1 = [{
            expectedCommand: {
                search: coll.getName(),
                collectionUUID: collUUID1,
                query: searchQuery,
                $db: dbName
            },
            response:
                mongotResponseForBatch(mongot1ResponseBatch, NumberLong(0), collNS, responseOk),
        }];
        expectedDocs = [
            {_id: 11, old: false, score: 111},
            {_id: 13, score: 30},
            {_id: 3, score: 29},
            {_id: 12, old: false, score: 28},
            {_id: 2, old: true, score: 10},
            {_id: 1, old: true, score: .99},
            {score: .654},
            {score: .456},
        ];
    }
    const s0Mongot = stWithMock.getMockConnectedToHost(shard0Conn);
    s0Mongot.setMockResponsesMetadataAgnostic(history0, NumberLong(123), useShardedFacets);

    const s1Mongot = stWithMock.getMockConnectedToHost(shard1Conn);
    s1Mongot.setMockResponsesMetadataAgnostic(history1, NumberLong(456), useShardedFacets);

    let pipeline = [
        {$search: searchQuery},
        {$project: {_id: 1, old: 1, score: {$meta: "searchScore"}}},
    ];
    if (useShardedFacets) {
        pipeline.push({$addFields: {meta: "$$SEARCH_META.val"}});
    }
    const aggResults = coll.aggregate(pipeline).toArray();
    // Make sure order is according to $searchScore.
    assert.eq(expectedDocs,
              aggResults,
              "Expected:\n" + tojson(expectedDocs) + "\nGot:\n" + tojson(aggResults));

    stWithMock.stop();
})();
})();
