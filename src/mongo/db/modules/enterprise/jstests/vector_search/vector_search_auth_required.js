/**
 * Test that $vectorSearch command works when mongod and mongot require authentication.
 *
 * @tags: [
 *   featureFlagVectorSearchPublicPreview,
 *   requires_auth,
 * ]
 */
(function() {
"use strict";
load("src/mongo/db/modules/enterprise/jstests/mongot/lib/mongotmock.js");
load("src/mongo/db/modules/enterprise/jstests/mongot/lib/shardingtest_with_mongotmock.js");
load('jstests/libs/uuid_util.js');  // For getUUIDFromListCollections.

const dbName = jsTestName();
const collName = "testColl";

const vectorSearchQuery = {
    queryVector: [1.0, 2.0, 3.0],
    path: "x",
    numCandidates: 10,
    limit: 5
};

// Perform a simple vector search command, and test that it succeeds.
function testSimpleVectorSearchQuery(mongodConn, mongotConn) {
    let db = mongodConn.getDB(dbName);
    let coll = db.getCollection(collName);
    const collUUID = getUUIDFromListCollections(db, collName);

    const vectorSearchCmd =
        {knn: coll.getName(), collectionUUID: collUUID, ...vectorSearchQuery, $db: dbName};

    const history = [
        {
            expectedCommand: vectorSearchCmd,
            response: {
                cursor: {
                    id: NumberLong(0),
                    ns: coll.getFullName(),
                    nextBatch: [{_id: 1, $vectorSearchScore: 0.321}]
                },
                ok: 1
            }
        },
    ];

    assert.commandWorked(mongotConn.adminCommand(
        {setMockResponses: 1, cursorId: NumberLong(123), history: history}));

    let cursor = coll.aggregate([{$vectorSearch: vectorSearchQuery}], {cursor: {batchSize: 2}});
    const expected = [{"_id": 1, "title": "cakes"}];

    assert.eq(expected, cursor.toArray());
}

// Set up mongotmock. This mongot requires authentication.
const mongotmock = new MongotMock();
mongotmock.start();
const mongotConnWithAuth = mongotmock.getConnection();

// Start a mongod normally, and point it at the mongot server.
let mongodConn = MongoRunner.runMongod({setParameter: {mongotHost: mongotConnWithAuth.host}});

// Seed the server with a document.
let coll = mongodConn.getDB(dbName).getCollection(collName);
assert.commandWorked(coll.insert({"_id": 1, "title": "cakes"}));

// Test that a vector search query succeeds when mongod and mongot are both configured auth enabled.
testSimpleVectorSearchQuery(mongodConn, mongotConnWithAuth);

// Now test with sharded collection.
MongoRunner.stopMongod(mongodConn);
mongotmock.stop();
let shardingTestOptions = {mongos: 1, shards: {rs0: {nodes: 1}}};

let stWithMock = new ShardingTestWithMongotMock(shardingTestOptions);
stWithMock.start();
let st = stWithMock.st;

let mongos = st.s;
let testDBMongos = mongos.getDB(dbName);
let testCollMongos = testDBMongos.getCollection(collName);

// Create and shard the collection so the commands can succeed.
assert.commandWorked(testDBMongos.createCollection(collName));
assert.commandWorked(mongos.adminCommand({enableSharding: dbName}));
assert.commandWorked(
    mongos.adminCommand({shardCollection: testCollMongos.getFullName(), key: {a: 1}}));

// Seed the server with a document.
assert.commandWorked(testCollMongos.insert({"_id": 1, "title": "cakes"}));

// Test that a vector search query succeeds when auth is configured enabled.
testSimpleVectorSearchQuery(mongos,
                            stWithMock.getMockConnectedToHost(st.rs0.getPrimary()).getConnection());

stWithMock.stop();
})();
