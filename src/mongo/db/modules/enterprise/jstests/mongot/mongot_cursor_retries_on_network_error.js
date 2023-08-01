/**
 * Test the mongot request retry functionality for the $search and $vectorSearch aggregation stages.
 */
import {FeatureFlagUtil} from "jstests/libs/feature_flag_util.js";

load("src/mongo/db/modules/enterprise/jstests/mongot/lib/mongotmock.js");
load("src/mongo/db/modules/enterprise/jstests/mongot/lib/shardingtest_with_mongotmock.js");
load('jstests/libs/uuid_util.js');

const dbName = jsTestName();
const collName = jsTestName();

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

    const collectionUUID = getUUIDFromListCollections(db, coll.getName());

    const expected = [
        {"_id": 1, "title": "cakes"},
        {"_id": 2, "title": "cookies and cakes"},
        {"_id": 5, "title": "cakes and oranges"},
        {"_id": 6, "title": "cakes and apples"},
        {"_id": 8, "title": "cakes and kale"}
    ];
    return {collectionUUID, expectedResults: expected};
}

function prepMongotResponse(expectedCommand, conn, mongotConn) {
    const coll = conn.getDB(dbName).getCollection(collName);
    const cursorId = NumberLong(123);
    const history = [
        {
            expectedCommand,
            response: {
                cursor: {
                    id: cursorId,
                    ns: coll.getFullName(),
                    nextBatch: [{_id: 1}, {_id: 2}, {_id: 5}]
                },
                ok: 1
            }
        },
        {
            expectedCommand: {getMore: cursorId, collection: coll.getName()},
            response: {cursor: {id: cursorId, ns: coll.getFullName(), nextBatch: [{_id: 6}]}, ok: 1}
        },
        {
            expectedCommand: {getMore: cursorId, collection: coll.getName()},
            response: {
                ok: 1,
                cursor: {id: NumberLong(0), ns: coll.getFullName(), nextBatch: [{_id: 8}]},
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

function runStandaloneTest(stageRegex, pipeline, expectedCommand) {
    const collectionData = prepCollection(conn);
    const coll = conn.getDB(dbName).getCollection(collName);

    expectedCommand["collectionUUID"] = collectionData.collectionUUID;
    prepMongotResponse(expectedCommand, conn, mongotConn);

    // Simulate a case where mongot closes the connection after getting a command.
    // Mongod should retry the command and succeed.
    {
        mongotmock.closeConnectionInResponseToNextNRequests(1);

        let cursor = coll.aggregate(pipeline, {cursor: {batchSize: 2}});
        assert.eq(collectionData["expectedResults"], cursor.toArray());
    }

    // Simulate a case where mongot closes the connection after getting a command,
    // and closes the connection again after receiving the retry.
    // Mongod should only retry once, and the network error from the closed connection should
    // be propogated to the client on retry.
    {
        mongotmock.closeConnectionInResponseToNextNRequests(2);

        const result = assert.throws(() => coll.aggregate(pipeline, {cursor: {batchSize: 2}}));
        assert(isNetworkError(result));
        assert(stageRegex.test(result), `Error wasn't due to stage failing: ${result}`);
    }
}

const searchQuery = {
    query: "cakes",
    path: "title"
};
runStandaloneTest(
    /\$search/, [{$search: searchQuery}], {search: collName, query: searchQuery, $db: dbName});

// TODO SERVER-75690 Enable this test.
if (FeatureFlagUtil.isEnabled(conn, "VectorSearchPublicPreview")) {
    const vectorSearchQuery =
        {queryVector: [1.0, 2.0, 3.0], path: "x", numCandidates: 10, limit: 5};
    runStandaloneTest(/\$vectorSearch/,
                      [{$vectorSearch: vectorSearchQuery}],
                      {knn: collName, ...vectorSearchQuery, $db: dbName});
}

MongoRunner.stopMongod(conn);
mongotmock.stop();

// Now test planShardedSearch (only applicable for $search).
const stWithMock = new ShardingTestWithMongotMock({
    name: "sharded_search",
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
const shardedSearchCmd = {
    search: collName,
    collectionUUID: shardedCollectionData.collectionUUID,
    query: searchQuery,
    $db: dbName
};

const mongos_mongotmock = stWithMock.getMockConnectedToHost(mongos);
const shard_mongot_conn = stWithMock.getMockConnectedToHost(shardPrimary).getConnection();

// Single failure
{
    // Mock responses to the planShardedSearch the mongos will issue and the eventual
    // $search command the mongod will issue.
    mockPlanShardedSearchResponse(collName, searchQuery, dbName, undefined, stWithMock);
    prepMongotResponse(shardedSearchCmd, shardPrimary, shard_mongot_conn);

    // Tell the mongotmock connected to the mongos to close the connection when
    // it receives the initial planShardedSearch from the mongos.
    mongos_mongotmock.closeConnectionInResponseToNextNRequests(1);

    // The mongos should retry the planShardedSearch, allowing the query to succeed.
    let coll = mongos.getDB(dbName).getCollection(collName);
    let cursor = coll.aggregate([{$search: searchQuery}], {cursor: {batchSize: 2}});
    assert.eq(shardedCollectionData["expectedResults"], cursor.toArray());
}

// Multiple failures
{
    // Tell the mongotmock connected to the mongos to close the connection when
    // it receives the initial planShardedSearch from the mongos and the retry.
    mongos_mongotmock.closeConnectionInResponseToNextNRequests(2);

    // Error on retry should be propogated out to client.
    let coll = mongos.getDB(dbName).getCollection(collName);
    const result =
        assert.throws(() => coll.aggregate([{$search: searchQuery}], {cursor: {batchSize: 2}}));
    assert(isNetworkError(result));
    assert(/planShardedSearch/.test(result),
           `Error wasn't due to planShardedSearch failing: ${result}`);
}

stWithMock.stop();
