/**
 * Test `$vectorSearch` retries on network error from the connection between mongot.
 * TODO: SERVER-79441 move to a shared folder for mongotmock tests.
 * @tags: [
 *   featureFlagVectorSearchPublicPreview,
 * ]
 */

(function() {
"use strict";
load("src/mongo/db/modules/enterprise/jstests/vector_search/lib/mongotmock.js");
load("src/mongo/db/modules/enterprise/jstests/search/lib/shardingtest_with_mongotmock.js");
load('jstests/libs/uuid_util.js');

const vectorSearchQuery = {
    queryVector: [1.0, 2.0, 3.0],
    path: "x",
    numCandidates: 10,
    limit: 5
};

const dbName = "test";
const collName = jsTestName();

// TODO: SERVER-79440 Factor out the common db/coll setup to util functions.
function prepCollection(conn, shouldShard = false) {
    const db = conn.getDB(dbName);
    const coll = db.getCollection(collName);
    coll.drop();

    if (shouldShard) {
        // Create and shard the collection so the commands can succeed.
        assert.commandWorked(db.createCollection(collName));
        assert.commandWorked(mongos.adminCommand({enableSharding: dbName}));
        assert.commandWorked(
            mongos.adminCommand({shardCollection: coll.getFullName(), key: {a: 1}}));
    }

    assert.commandWorked(coll.insert({"_id": 1, "title": "cakes"}));
    assert.commandWorked(coll.insert({"_id": 2, "title": "cookies and cakes"}));
    assert.commandWorked(coll.insert({"_id": 3, "title": "vegetables"}));
    assert.commandWorked(coll.insert({"_id": 4, "title": "oranges"}));
    assert.commandWorked(coll.insert({"_id": 5, "title": "cakes and oranges"}));
    assert.commandWorked(coll.insert({"_id": 6, "title": "cakes and apples"}));
    assert.commandWorked(coll.insert({"_id": 7, "title": "apples"}));
    assert.commandWorked(coll.insert({"_id": 8, "title": "cakes and kale"}));

    const collUUID = getUUIDFromListCollections(db, coll.getName());
    const vectorSearchCmd =
        {knn: coll.getName(), collectionUUID: collUUID, ...vectorSearchQuery, $db: "test"};

    const expected = [
        {"_id": 1, "title": "cakes"},
        {"_id": 2, "title": "cookies and cakes"},
        {"_id": 5, "title": "cakes and oranges"},
        {"_id": 6, "title": "cakes and apples"},
        {"_id": 8, "title": "cakes and kale"}
    ];
    return {vectorSearchCmd: vectorSearchCmd, expectedResults: expected};
}

// TODO: SERVER-79440 Factor out the common db/coll setup to util functions.
function prepMongotKnnResponse(vectorSearchCmd, conn, mongotConn) {
    const coll = conn.getDB("test").getCollection(collName);
    const cursorId = NumberLong(123);
    const history = [
        {
            expectedCommand: vectorSearchCmd,
            response: {
                cursor: {
                    id: cursorId,
                    ns: coll.getFullName(),
                    nextBatch: [
                        {_id: 1, $vectorSearchScore: 0.321},
                        {_id: 2, $vectorSearchScore: 0.654},
                        {_id: 5, $vectorSearchScore: 0.789}
                    ]
                },
                ok: 1
            }
        },
        {
            expectedCommand: {getMore: cursorId, collection: coll.getName()},
            response: {
                cursor: {
                    id: cursorId,
                    ns: coll.getFullName(),
                    nextBatch: [{_id: 6, $vectorSearchScore: 0.123}]
                },
                ok: 1
            }
        },
        {
            expectedCommand: {getMore: cursorId, collection: coll.getName()},
            response: {
                ok: 1,
                cursor: {
                    id: NumberLong(0),
                    ns: coll.getFullName(),
                    nextBatch: [{_id: 8, $vectorSearchScore: 0.345}]
                },
            }
        },
    ];

    assert.commandWorked(
        mongotConn.adminCommand({setMockResponses: 1, cursorId: cursorId, history: history}));
}

// Set up mongotmock and point the mongod to it.
const mongotmock = new MongotMock();
mongotmock.start();
const mongotConn = mongotmock.getConnection();

const conn = MongoRunner.runMongod({setParameter: {mongotHost: mongotConn.host}});

const collectionData = prepCollection(conn);
prepMongotKnnResponse(collectionData["vectorSearchCmd"], conn, mongotConn);
const coll = conn.getDB("test").getCollection(collName);

// Simulate a case where mongot closes the connection after getting a vector search command.
// Mongod should retry the vector search command and succeed.
{
    mongotmock.closeConnectionInResponseToNextNRequests(1);

    let cursor = coll.aggregate([{$vectorSearch: vectorSearchQuery}], {cursor: {batchSize: 2}});
    assert.eq(collectionData["expectedResults"], cursor.toArray());
}

// Simulate a case where mongot closes the connection after getting a vector search command,
// and closes the connection again after receiving the retry.
// Mongod should only retry once, and the network error from the closed connection should
// be propogated to the client on retry.
{
    mongotmock.closeConnectionInResponseToNextNRequests(2);

    const result = assert.throws(
        () => coll.aggregate([{$vectorSearch: vectorSearchQuery}], {cursor: {batchSize: 2}}));
    assert(isNetworkError(result));
    assert(/\$vectorSearch/.test(result), `Error wasn't due to $vectorSearch failing: ${result}`);
}

MongoRunner.stopMongod(conn);
mongotmock.stop();

// Now test on sharded cluster
const stWithMock = new ShardingTestWithMongotMock({
    name: "sharded_vector_search",
    shards: {
        rs0: {nodes: 1},
    },
    mongos: 1,
});
stWithMock.start();
let st = stWithMock.st;
let mongos = st.s;
let shardPrimary = st.rs0.getPrimary();
const shardedCollectionData = prepCollection(mongos, true);

const mongos_mongotmock = stWithMock.getMockConnectedToHost(shardPrimary);
const shard_mongot_conn = stWithMock.getMockConnectedToHost(shardPrimary).getConnection();

// Single failure
{
    // Mock responses to the eventual $vectorSearch command the mongod will issue.
    prepMongotKnnResponse(
        shardedCollectionData["vectorSearchCmd"], shardPrimary, shard_mongot_conn);

    // Tell the mongotmock connected to the mongod to close the connection when
    // it receives the $vectorSearch command from the mongod.
    mongos_mongotmock.closeConnectionInResponseToNextNRequests(1);

    // The mongod should retry the command, allowing the query to succeed.
    let coll = mongos.getDB(dbName).getCollection(collName);
    let cursor = coll.aggregate([{$vectorSearch: vectorSearchQuery}], {cursor: {batchSize: 2}});
    assert.eq(shardedCollectionData["expectedResults"], cursor.toArray());
}

// Multiple failures
{
    // Tell the mongotmock connected to the mongod to close the connection when
    // it receives the $vectorSearch command from the mongod.
    mongos_mongotmock.closeConnectionInResponseToNextNRequests(2);

    // Error on retry should be propogated out to client.
    let coll = mongos.getDB(dbName).getCollection(collName);
    const result = assert.throws(
        () => coll.aggregate([{$vectorSearch: vectorSearchQuery}], {cursor: {batchSize: 2}}));
    assert(isNetworkError(result));
    assert(/\$vectorSearch/.test(result), `Error wasn't due to $vectorSearch failing: ${result}`);
}

stWithMock.stop();
})();
