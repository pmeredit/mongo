/**
 * Sharding tests for the `$searchBeta` aggregation pipeline stage.
 */
(function() {
    "use strict";

    load('jstests/libs/uuid_util.js');                 // For getUUIDFromListCollections.
    load("jstests/libs/collection_drop_recreate.js");  // For assertCreateCollection.
    load("src/mongo/db/modules/enterprise/jstests/search_beta/lib/mongotmock.js");
    load("src/mongo/db/modules/enterprise/jstests/search_beta/lib/shardingtest_with_mongotmock.js");

    const dbName = "test";
    const collName = "internal_search_beta_mongot_remote";

    const stWithMock = new ShardingTestWithMongotMock({
        name: "sharded_search_beta",
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
    const st = stWithMock.st;

    const mongos = st.s;
    const testDB = mongos.getDB(dbName);
    const testColl = testDB.getCollection(collName);
    const collNS = testColl.getFullName();

    assert.commandWorked(testColl.insert({_id: 1, x: "ow"}));
    assert.commandWorked(testColl.insert({_id: 2, x: "now", y: "lorem"}));
    assert.commandWorked(testColl.insert({_id: 3, x: "brown", y: "ipsum"}));
    assert.commandWorked(testColl.insert({_id: 4, x: "cow", y: "lorem ipsum"}));
    assert.commandWorked(testColl.insert({_id: 11, x: "brown", y: "ipsum"}));
    assert.commandWorked(testColl.insert({_id: 12, x: "cow", y: "lorem ipsum"}));
    assert.commandWorked(testColl.insert({_id: 13, x: "brown", y: "ipsum"}));
    assert.commandWorked(testColl.insert({_id: 14, x: "cow", y: "lorem ipsum"}));

    // Shard the test collection, split it at {_id: 10}, and move the higher chunk to shard1.
    assert.commandWorked(mongos.getDB("admin").runCommand({enableSharding: dbName}));
    st.ensurePrimaryShard(dbName, st.shard0.name);
    st.shardColl(testColl, {_id: 1}, {_id: 10}, {_id: 10 + 1});

    const collUUID0 = getUUIDFromListCollections(st.rs0.getPrimary().getDB(dbName), collName);
    const collUUID1 = getUUIDFromListCollections(st.rs1.getPrimary().getDB(dbName), collName);

    const mongotQuery = {};
    const cursorId = NumberLong(123);
    const pipeline = [
        {$searchBeta: mongotQuery},
    ];

    function runTestOnPrimaries(testFn) {
        testDB.getMongo().setReadPref("primary");
        testFn(st.rs0.getPrimary(), st.rs1.getPrimary());
    }

    function runTestOnSecondaries(testFn) {
        testDB.getMongo().setReadPref("secondary");
        testFn(st.rs0.getSecondary(), st.rs1.getSecondary());
    }

    // Tests that $searchBeta returns documents in descending $meta: searchScore.
    function testBasicCase(shard0Conn, shard1Conn) {
        const responseOk = 1;

        const mongot0ResponseBatch = [
            {_id: 3, $searchScore: 100},
            {_id: 2, $searchScore: 10},
            {_id: 4, $searchScore: 1},
            {_id: 1, $searchScore: 0.99},
        ];
        const history0 = [{
            expectedCommand: mongotCommandForQuery(mongotQuery, collName, dbName, collUUID0),
            response:
                mongotResponseForBatch(mongot0ResponseBatch, NumberLong(0), collNS, responseOk),
        }];
        const s0Mongot = stWithMock.getMockConnectedToHost(shard0Conn);
        s0Mongot.setMockResponses(history0, cursorId);

        const mongot1ResponseBatch = [
            {_id: 11, $searchScore: 111},
            {_id: 13, $searchScore: 30},
            {_id: 12, $searchScore: 29},
            {_id: 14, $searchScore: 28},
        ];
        const history1 = [{
            expectedCommand: mongotCommandForQuery(mongotQuery, collName, dbName, collUUID1),
            response:
                mongotResponseForBatch(mongot1ResponseBatch, NumberLong(0), collNS, responseOk),
        }];
        const s1Mongot = stWithMock.getMockConnectedToHost(shard1Conn);
        s1Mongot.setMockResponses(history1, cursorId);

        const expectedDocs = [
            {_id: 11, x: "brown", y: "ipsum"},
            {_id: 3, x: "brown", y: "ipsum"},
            {_id: 13, x: "brown", y: "ipsum"},
            {_id: 12, x: "cow", y: "lorem ipsum"},
            {_id: 14, x: "cow", y: "lorem ipsum"},
            {_id: 2, x: "now", y: "lorem"},
            {_id: 4, x: "cow", y: "lorem ipsum"},
            {_id: 1, x: "ow"},
        ];

        assert.eq(testColl.aggregate(pipeline).toArray(), expectedDocs);
    }
    runTestOnPrimaries(testBasicCase);
    runTestOnSecondaries(testBasicCase);

    // Tests the case where there's an error from one mongot, which should get propagated to
    // mongod, then to mongos.
    function testErrorCase(shard0Conn, shard1Conn) {
        const responseOk = 1;

        const mongot0ResponseBatch = [
            {_id: 3, $searchScore: 100},
        ];
        const history0 = [
            {
              expectedCommand: mongotCommandForQuery(mongotQuery, collName, dbName, collUUID0),
              response: mongotResponseForBatch(mongot0ResponseBatch, cursorId, collNS, responseOk),
            },
            {
              expectedCommand: {getMore: cursorId, collection: collName},
              response: {
                  ok: 0,
                  errmsg: "mongot error",
                  code: ErrorCodes.InternalError,
                  codeName: "InternalError"
              }
            }
        ];
        const s0Mongot = stWithMock.getMockConnectedToHost(shard0Conn);
        s0Mongot.setMockResponses(history0, cursorId);

        const mongot1ResponseBatch = [
            {_id: 11, $searchScore: 111},
            {_id: 13, $searchScore: 30},
            {_id: 12, $searchScore: 29},
            {_id: 14, $searchScore: 28},
        ];
        const history1 = [{
            expectedCommand: mongotCommandForQuery(mongotQuery, collName, dbName, collUUID1),
            response:
                mongotResponseForBatch(mongot1ResponseBatch, NumberLong(0), collNS, responseOk),
        }];
        const s1Mongot = stWithMock.getMockConnectedToHost(shard1Conn);
        s1Mongot.setMockResponses(history1, cursorId);

        const err = assert.throws(() => testColl.aggregate(pipeline).toArray());
        assert.commandFailedWithCode(err, ErrorCodes.InternalError);
    }
    runTestOnPrimaries(testErrorCase);
    runTestOnSecondaries(testErrorCase);

    // Tests the case where the mongot associated with one shard returns a small one batch data
    // set, and the mongot associated with the other shard returns more data.
    function testUnevenResultDistributionCase(shard0Conn, shard1Conn) {
        const responseOk = 1;

        const mongot0ResponseBatch = [
            {_id: 3, $searchScore: 100},
        ];
        const history0 = [{
            expectedCommand: mongotCommandForQuery(mongotQuery, collName, dbName, collUUID0),
            response:
                mongotResponseForBatch(mongot0ResponseBatch, NumberLong(0), collNS, responseOk),
        }];
        const s0Mongot = stWithMock.getMockConnectedToHost(shard0Conn);
        s0Mongot.setMockResponses(history0, cursorId);

        const history1 = [
            {
              expectedCommand: mongotCommandForQuery(mongotQuery, collName, dbName, collUUID1),
              response: mongotResponseForBatch(
                  [{_id: 11, $searchScore: 111}, {_id: 13, $searchScore: 30}],
                  cursorId,
                  collNS,
                  responseOk),
            },

            {
              expectedCommand: {getMore: cursorId, collection: collName},
              response:
                  mongotResponseForBatch([{_id: 12, $searchScore: 29}, {_id: 14, $searchScore: 28}],
                                         NumberLong(0),
                                         collNS,
                                         responseOk)
            }
        ];
        const s1Mongot = stWithMock.getMockConnectedToHost(shard1Conn);
        s1Mongot.setMockResponses(history1, cursorId);

        const expectedDocs = [
            {_id: 11, x: "brown", y: "ipsum"},
            {_id: 3, x: "brown", y: "ipsum"},
            {_id: 13, x: "brown", y: "ipsum"},
            {_id: 12, x: "cow", y: "lorem ipsum"},
            {_id: 14, x: "cow", y: "lorem ipsum"},
        ];

        assert.eq(testColl.aggregate(pipeline).toArray(), expectedDocs);
    }
    runTestOnPrimaries(testUnevenResultDistributionCase);
    runTestOnSecondaries(testUnevenResultDistributionCase);

    // Tests the case where a mongot does not actually return documents in the right order.
    function testMisbehavingMongot(shard0Conn, shard1Conn) {
        const responseOk = 1;

        // mongot 0 returns results in the wrong order!
        const mongot0ResponseBatch = [
            {_id: 1, $searchScore: 0.99},
            {_id: 4, $searchScore: 1},
            {_id: 2, $searchScore: 10},
            {_id: 3, $searchScore: 100},
        ];
        const history0 = [{
            expectedCommand: mongotCommandForQuery(mongotQuery, collName, dbName, collUUID0),
            response:
                mongotResponseForBatch(mongot0ResponseBatch, NumberLong(0), collNS, responseOk),
        }];
        const s0Mongot = stWithMock.getMockConnectedToHost(shard0Conn);
        s0Mongot.setMockResponses(history0, cursorId);

        const mongot1ResponseBatch = [
            {_id: 11, $searchScore: 111},
            {_id: 13, $searchScore: 30},
            {_id: 12, $searchScore: 29},
            {_id: 14, $searchScore: 28},
        ];
        const history1 = [{
            expectedCommand: mongotCommandForQuery(mongotQuery, collName, dbName, collUUID1),
            response:
                mongotResponseForBatch(mongot1ResponseBatch, NumberLong(0), collNS, responseOk),
        }];
        const s1Mongot = stWithMock.getMockConnectedToHost(shard1Conn);
        s1Mongot.setMockResponses(history1, cursorId);

        const expectedDocs = [
            {_id: 11, x: "brown", y: "ipsum"},
            {_id: 3, x: "brown", y: "ipsum"},
            {_id: 13, x: "brown", y: "ipsum"},
            {_id: 12, x: "cow", y: "lorem ipsum"},
            {_id: 14, x: "cow", y: "lorem ipsum"},
            {_id: 2, x: "now", y: "lorem"},
            {_id: 4, x: "cow", y: "lorem ipsum"},
            {_id: 1, x: "ow"},
        ];

        const res = testColl.aggregate(pipeline).toArray();

        // In this case, mongod returns incorrect results (and doesn't crash). Check that the _set_
        // of results returned is correct, ignoring ordering.
        assert.sameMembers(res, expectedDocs);
    }
    runTestOnPrimaries(testMisbehavingMongot);
    runTestOnSecondaries(testMisbehavingMongot);

    // Tests that correct results are returned when $searchBeta is followed by a $sort on a
    // different key.
    function testSearchBetaFollowedBySortOnDifferentKey(shard0Conn, shard1Conn) {
        const responseOk = 1;

        const mongot0ResponseBatch = [
            {_id: 3, $searchScore: 100},
            {_id: 2, $searchScore: 10},
            {_id: 4, $searchScore: 1},
            {_id: 1, $searchScore: 0.99},
        ];
        const history0 = [{
            expectedCommand: mongotCommandForQuery(mongotQuery, collName, dbName, collUUID0),
            response:
                mongotResponseForBatch(mongot0ResponseBatch, NumberLong(0), collNS, responseOk),
        }];
        const s0Mongot = stWithMock.getMockConnectedToHost(shard0Conn);
        s0Mongot.setMockResponses(history0, cursorId);

        const mongot1ResponseBatch = [
            {_id: 11, $searchScore: 111},
            {_id: 13, $searchScore: 30},
            {_id: 12, $searchScore: 29},
            {_id: 14, $searchScore: 28},
        ];
        const history1 = [{
            expectedCommand: mongotCommandForQuery(mongotQuery, collName, dbName, collUUID1),
            response:
                mongotResponseForBatch(mongot1ResponseBatch, NumberLong(0), collNS, responseOk),
        }];
        const s1Mongot = stWithMock.getMockConnectedToHost(shard1Conn);
        s1Mongot.setMockResponses(history1, cursorId);

        const expectedDocs = [
            {_id: 1, x: "ow"},
            {_id: 2, x: "now", y: "lorem"},
            {_id: 3, x: "brown", y: "ipsum"},
            {_id: 4, x: "cow", y: "lorem ipsum"},
            {_id: 11, x: "brown", y: "ipsum"},
            {_id: 12, x: "cow", y: "lorem ipsum"},
            {_id: 13, x: "brown", y: "ipsum"},
            {_id: 14, x: "cow", y: "lorem ipsum"},
        ];

        assert.eq(testColl.aggregate(pipeline.concat([{$sort: {_id: 1}}])).toArray(), expectedDocs);
    }
    runTestOnPrimaries(testSearchBetaFollowedBySortOnDifferentKey);
    runTestOnSecondaries(testSearchBetaFollowedBySortOnDifferentKey);

    stWithMock.stop();
})();
