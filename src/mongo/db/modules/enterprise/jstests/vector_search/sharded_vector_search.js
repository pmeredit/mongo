/**
 * Sharding tests for the `$vectorSearch` aggregation pipeline stage.
 * @tags: [
 *   featureFlagVectorSearchPublicPreview,
 * ]
 */
load('jstests/libs/uuid_util.js');                 // For getUUIDFromListCollections.
load("jstests/libs/collection_drop_recreate.js");  // For assertCreateCollection.
load("src/mongo/db/modules/enterprise/jstests/mongot/lib/mongotmock.js");
load("src/mongo/db/modules/enterprise/jstests/mongot/lib/shardingtest_with_mongotmock.js");

const dbName = "test";
const collName = "sharded_vector_search";

const stWithMock = new ShardingTestWithMongotMock({
    name: "sharded_vector_search",
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

const vectorSearchQuery = {
    queryVector: [1.0, 2.0, 3.0],
    path: "x",
    numCandidates: 10,
    limit: 100
};
const expectedMongotCommand =
    mongotCommandForKnnQuery({...vectorSearchQuery, collName, dbName, collectionUUID: collUUID0});

const cursorId = NumberLong(123);
const pipeline = [
    {$vectorSearch: vectorSearchQuery},
];

function runTestOnPrimaries(testFn) {
    testDB.getMongo().setReadPref("primary");
    testFn(st.rs0.getPrimary(), st.rs1.getPrimary());
}

function runTestOnSecondaries(testFn) {
    testDB.getMongo().setReadPref("secondary");
    testFn(st.rs0.getSecondary(), st.rs1.getSecondary());
}

// Tests that $vectorSearch returns documents in descending $meta: vectorSearchScore.
function testBasicCase(shard0Conn, shard1Conn) {
    const responseOk = 1;

    const mongot0ResponseBatch = [
        {_id: 3, $vectorSearchScore: 0.99},
        {_id: 2, $vectorSearchScore: 0.10},
        {_id: 4, $vectorSearchScore: 0.01},
        {_id: 1, $vectorSearchScore: 0.0099},
    ];
    const history0 = [{
        expectedCommand: expectedMongotCommand,
        response: mongotResponseForBatch(mongot0ResponseBatch, NumberLong(0), collNS, responseOk),
    }];

    const s0Mongot = stWithMock.getMockConnectedToHost(shard0Conn);
    s0Mongot.setMockResponses(history0, cursorId);

    const mongot1ResponseBatch = [
        {_id: 11, $vectorSearchScore: 1.0},
        {_id: 13, $vectorSearchScore: 0.30},
        {_id: 12, $vectorSearchScore: 0.29},
        {_id: 14, $vectorSearchScore: 0.28},
    ];
    const history1 = [{
        expectedCommand: expectedMongotCommand,
        response: mongotResponseForBatch(mongot1ResponseBatch, NumberLong(0), collNS, responseOk),
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
        {_id: 3, $vectorSearchScore: 1.00},
    ];
    const history0 = [
        {
            expectedCommand: expectedMongotCommand,
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
        {_id: 11, $vectorSearchScore: 1.0},
        {_id: 13, $vectorSearchScore: 0.30},
        {_id: 12, $vectorSearchScore: 0.29},
        {_id: 14, $vectorSearchScore: 0.28},
    ];
    const history1 = [{
        expectedCommand: expectedMongotCommand,
        response: mongotResponseForBatch(mongot1ResponseBatch, NumberLong(0), collNS, responseOk),
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
        {_id: 3, $vectorSearchScore: 0.99},
    ];
    const history0 = [{
        expectedCommand: expectedMongotCommand,
        response: mongotResponseForBatch(mongot0ResponseBatch, NumberLong(0), collNS, responseOk),
    }];
    const s0Mongot = stWithMock.getMockConnectedToHost(shard0Conn);
    s0Mongot.setMockResponses(history0, cursorId);

    const history1 = [
        {
            expectedCommand: expectedMongotCommand,
            response: mongotResponseForBatch(
                [{_id: 11, $vectorSearchScore: 1.0}, {_id: 13, $vectorSearchScore: 0.30}],
                cursorId,
                collNS,
                responseOk),
        },

        {
            expectedCommand: {getMore: cursorId, collection: collName},
            response: mongotResponseForBatch(
                [{_id: 12, $vectorSearchScore: 0.29}, {_id: 14, $vectorSearchScore: 0.28}],
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
        {_id: 2, $vectorSearchScore: 0.10},
        {_id: 1, $vectorSearchScore: 0.0099},
        {_id: 3, $vectorSearchScore: 0.99},
        {_id: 4, $vectorSearchScore: 0.01},
    ];
    const history0 = [{
        expectedCommand: expectedMongotCommand,
        response: mongotResponseForBatch(mongot0ResponseBatch, NumberLong(0), collNS, responseOk),
    }];
    const s0Mongot = stWithMock.getMockConnectedToHost(shard0Conn);
    s0Mongot.setMockResponses(history0, cursorId);

    const mongot1ResponseBatch = [
        {_id: 12, $vectorSearchScore: 0.29},
        {_id: 14, $vectorSearchScore: 0.28},
        {_id: 11, $vectorSearchScore: 1.0},
        {_id: 13, $vectorSearchScore: 0.30},
    ];
    const history1 = [{
        expectedCommand: expectedMongotCommand,
        response: mongotResponseForBatch(mongot1ResponseBatch, NumberLong(0), collNS, responseOk),
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

// Tests that correct results are returned when $vectorSearch is followed by a $sort on a
// different key.
function testSearchFollowedBySortOnDifferentKey(shard0Conn, shard1Conn) {
    const responseOk = 1;

    const mongot0ResponseBatch = [
        {_id: 3, $vectorSearchScore: 0.99},
        {_id: 2, $vectorSearchScore: 0.10},
        {_id: 4, $vectorSearchScore: 0.01},
        {_id: 1, $vectorSearchScore: 0.0099},
    ];
    const history0 = [{
        expectedCommand: expectedMongotCommand,
        response: mongotResponseForBatch(mongot0ResponseBatch, NumberLong(0), collNS, responseOk),
    }];
    const s0Mongot = stWithMock.getMockConnectedToHost(shard0Conn);
    s0Mongot.setMockResponses(history0, cursorId);

    const mongot1ResponseBatch = [
        {_id: 11, $vectorSearchScore: 1.0},
        {_id: 13, $vectorSearchScore: 0.30},
        {_id: 12, $vectorSearchScore: 0.29},
        {_id: 14, $vectorSearchScore: 0.28},
    ];
    const history1 = [{
        expectedCommand: expectedMongotCommand,
        response: mongotResponseForBatch(mongot1ResponseBatch, NumberLong(0), collNS, responseOk),
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
runTestOnPrimaries(testSearchFollowedBySortOnDifferentKey);
runTestOnSecondaries(testSearchFollowedBySortOnDifferentKey);

function testGetMoreOnShard(shard0Conn, shard1Conn) {
    const responseOk = {ok: 1};

    // Mock response from shard 0's mongot.
    const mongot0ResponseBatch = [
        {_id: 2, $vectorSearchScore: 1.0},
        {_id: 3, $vectorSearchScore: 0.99},
        {_id: 1, $vectorSearchScore: 0.29},
        {_id: 4, $vectorSearchScore: 0.01},
    ];
    const history0 = [
        {
            expectedCommand: expectedMongotCommand,
            response: mongotResponseForBatch(
                mongot0ResponseBatch.slice(0, 2), NumberLong(10), collNS, responseOk),
        },
        {
            expectedCommand: {getMore: NumberLong(10), collection: collName},
            response: {
                cursor: {
                    id: NumberLong(0),
                    ns: collName,
                    nextBatch: mongot0ResponseBatch.slice(2),
                },
                ok: 1,
            }
        },
    ];

    const s0Mongot = stWithMock.getMockConnectedToHost(shard0Conn);
    s0Mongot.setMockResponses(history0, NumberLong(10));

    // Mock response from shard 1's mongot.
    const mongot1ResponseBatch = [
        {_id: 14, $vectorSearchScore: 0.28},
        {_id: 11, $vectorSearchScore: 0.10},
        {_id: 13, $vectorSearchScore: 0.02},
        {_id: 12, $vectorSearchScore: 0.0099},
    ];
    const history1 = [
        {
            expectedCommand: expectedMongotCommand,
            response: mongotResponseForBatch(
                mongot1ResponseBatch.slice(0, 1), NumberLong(30), collNS, responseOk),
        },
        {
            expectedCommand: {getMore: NumberLong(30), collection: collName},
            response: {
                cursor: {
                    id: NumberLong(0),
                    ns: collName,
                    nextBatch: mongot1ResponseBatch.slice(1),
                },
                ok: 1,
            }
        },
    ];

    const s1Mongot = stWithMock.getMockConnectedToHost(shard1Conn);
    s1Mongot.setMockResponses(history1, NumberLong(30));

    const expectedDocs = [
        {_id: 2, x: "now"},
        {_id: 3, x: "brown"},
        {_id: 1, x: "ow"},
        {_id: 14, x: "cow"},
        {_id: 11, x: "brown"},
        {_id: 13, x: "brown"},
        {_id: 4, x: "cow"},
        {_id: 12, x: "cow"},
    ];

    assert.eq(expectedDocs,
              testColl
                  .aggregate([
                      {$vectorSearch: vectorSearchQuery},
                      {$project: {_id: 1, x: 1}},
                  ])
                  .toArray());
}
runTestOnPrimaries(testGetMoreOnShard);
runTestOnSecondaries(testGetMoreOnShard);

stWithMock.stop();
