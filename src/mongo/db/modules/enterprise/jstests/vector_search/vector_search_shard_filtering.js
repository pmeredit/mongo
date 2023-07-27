/**
 * Tests that the _id lookups performed by $vectorSearch have a shard filter applied to them so that
 * orphans are not returned.
 * @tags: [
 *   featureFlagVectorSearchPublicPreview,
 * ]
 */
load('jstests/libs/uuid_util.js');                 // For getUUIDFromListCollections.
load("jstests/libs/collection_drop_recreate.js");  // For assertCreateCollection.
load("src/mongo/db/modules/enterprise/jstests/vector_search/lib/mongotmock.js");
load("src/mongo/db/modules/enterprise/jstests/search/lib/shardingtest_with_mongotmock.js");

const dbName = "test";
const collName = "vector_search_shard_filtering";

// This test deliberately creates orphans to test shard filtering.
TestData.skipCheckOrphans = true;

const stWithMock = new ShardingTestWithMongotMock({
    name: "sharded_vector_search",
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
const testColl = testDB.getCollection(collName);
const collNS = testColl.getFullName();

// Documents that end up on shard0.
assert.commandWorked(testColl.insert({_id: 1, shardKey: 0, x: "ow"}));
assert.commandWorked(testColl.insert({_id: 2, shardKey: 0, x: "now", y: "lorem"}));
assert.commandWorked(testColl.insert({_id: 3, shardKey: 0, x: "brown", y: "ipsum"}));
assert.commandWorked(testColl.insert({_id: 4, shardKey: 0, x: "cow", y: "lorem ipsum"}));
// Documents that end up on shard1.
assert.commandWorked(testColl.insert({_id: 11, shardKey: 100, x: "brown", y: "ipsum"}));
assert.commandWorked(testColl.insert({_id: 12, shardKey: 100, x: "cow", y: "lorem ipsum"}));
assert.commandWorked(testColl.insert({_id: 13, shardKey: 100, x: "brown", y: "ipsum"}));
assert.commandWorked(testColl.insert({_id: 14, shardKey: 100, x: "cow", y: "lorem ipsum"}));

// Shard the test collection, split it at {shardKey: 10}, and move the higher chunk to shard1.
assert.commandWorked(testColl.createIndex({shardKey: 1}));
assert.commandWorked(testDB.adminCommand({enableSharding: dbName}));
st.ensurePrimaryShard(dbName, st.shard0.name);

// 'waitForDelete' is set to 'true' so that range deletion completes before we insert our orphan.
st.shardColl(
    testColl, {shardKey: 1}, {shardKey: 10}, {shardKey: 10 + 1}, dbName, true /* waitForDelete */);

const shard0Conn = st.rs0.getPrimary();
const shard1Conn = st.rs1.getPrimary();

// Shard0 should have exactly 4 documents; none of which get filtered out.
assert.eq(shard0Conn.getDB(dbName)[collName].find().itcount(), 4);
assert.eq(testColl.find().itcount(), 8);

// Insert a document into shard 0 which is not owned by that shard.
assert.commandWorked(shard0Conn.getDB(dbName)[collName].insert(
    {_id: 15, shardKey: 100, x: "_should be filtered out"}));

// Verify that the orphaned document exists on shard0, but that it gets filtered out when
// querying 'testColl'.
assert.eq(shard0Conn.getDB(dbName)[collName].find({_id: 15}).itcount(), 1);
assert.eq(testColl.find().itcount(), 8);

// Insert a document into shard 0 which doesn't have a shard key. This document should not be
// skipped when mongot returns a result indicating that it matched the text query. The server
// should not crash and the operation should not fail.
assert.commandWorked(shard0Conn.getDB(dbName)[collName].insert({_id: 16}));

const collUUID0 = getUUIDFromListCollections(st.rs0.getPrimary().getDB(dbName), collName);
const collUUID1 = getUUIDFromListCollections(st.rs1.getPrimary().getDB(dbName), collName);

const vectorSearchQuery = {
    queryVector: [1.0, 2.0, 3.0],
    path: "x",
    numCandidates: 10,
    index: "index",
    limit: 10,
};
const responseOk = 1;

const mongot0ResponseBatch = [
    // Mongot will act "stale": it will have an index entry for a document not owned by shard
    // 0.
    {_id: 15, $vectorSearchScore: 0.99},

    // The document with _id 16 has no shard key. (Perhaps it was inserted manually). This should
    // not be filtered out, because documents with missing shard key values will be placed on the
    // chunk that they would be placed at if there were null values for the shard key fields.
    {_id: 16, $vectorSearchScore: 0.98},

    // The remaining documents rightfully belong to shard 0.
    {_id: 3, $vectorSearchScore: 0.97},
    {_id: 2, $vectorSearchScore: 0.10},
    {_id: 4, $vectorSearchScore: 0.02},
    {_id: 1, $vectorSearchScore: 0.01},
];
const expectedMongotCommand =
    mongotCommandForKnnQuery({...vectorSearchQuery, collName, dbName, collectionUUID: collUUID0});

const history0 = [{
    expectedCommand: expectedMongotCommand,
    response: mongotResponseForBatch(mongot0ResponseBatch, NumberLong(0), collNS, responseOk)
}];
const s0Mongot = stWithMock.getMockConnectedToHost(shard0Conn);
s0Mongot.setMockResponses(history0, NumberLong(123));

const mongot1ResponseBatch = [
    {_id: 11, $vectorSearchScore: 1.0},
    {_id: 13, $vectorSearchScore: 0.30},
    {_id: 12, $vectorSearchScore: 0.29},
    {_id: 14, $vectorSearchScore: 0.28},
];
const history1 = [{
    expectedCommand: expectedMongotCommand,
    response: mongotResponseForBatch(mongot1ResponseBatch, NumberLong(0), collNS, responseOk)
}];
const s1Mongot = stWithMock.getMockConnectedToHost(shard1Conn);
s1Mongot.setMockResponses(history1, NumberLong(456));

const expectedDocs = [
    {_id: 11, shardKey: 100, x: "brown", y: "ipsum"},
    {_id: 16},
    {_id: 3, shardKey: 0, x: "brown", y: "ipsum"},
    {_id: 13, shardKey: 100, x: "brown", y: "ipsum"},
    {_id: 12, shardKey: 100, x: "cow", y: "lorem ipsum"},
    {_id: 14, shardKey: 100, x: "cow", y: "lorem ipsum"},
    {_id: 2, shardKey: 0, x: "now", y: "lorem"},
    {_id: 4, shardKey: 0, x: "cow", y: "lorem ipsum"},
    {_id: 1, shardKey: 0, x: "ow"},
];

assert.eq(testColl.aggregate([{$vectorSearch: vectorSearchQuery}]).toArray(), expectedDocs);

// Verify that our orphaned document is still on shard0.
assert.eq(shard0Conn.getDB(dbName)[collName].find({_id: 15}).itcount(), 1);

stWithMock.stop();
