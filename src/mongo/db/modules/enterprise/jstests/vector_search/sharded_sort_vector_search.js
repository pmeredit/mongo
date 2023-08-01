/**
 * Tests for the `$vectorSearch` aggregation pipeline stage.
 * @tags: [
 *  featureFlagVectorSearchPublicPreview,
 * ]
 */
(function() {
"use strict";

load('jstests/libs/uuid_util.js');                 // For getUUIDFromListCollections.
load("jstests/libs/collection_drop_recreate.js");  // For assertCreateCollection.
load("src/mongo/db/modules/enterprise/jstests/mongot/lib/shardingtest_with_mongotmock.js");
load("src/mongo/db/modules/enterprise/jstests/mongot/lib/mongotmock.js");

const dbName = "test";
const collName = jsTestName();

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
stWithMock.assertEmptyMocks();
const st = stWithMock.st;

const mongos = st.s;
const testDB = mongos.getDB(dbName);
const testColl = testDB.getCollection(collName);
testColl.drop();
const collNS = testColl.getFullName();

assert.commandWorked(testColl.insert([
    {_id: 1, x: "ow"},
    {_id: 2, x: "now", y: "lorem"},
    {_id: 3, x: "brown", y: "ipsum"},
    {_id: 4, x: "cow", y: "lorem ipsum"},
    {_id: 10},
    {_id: 11, x: "brown", y: "ipsum"},
    {_id: 12, x: "cow", y: "lorem ipsum"},
    {_id: 13, x: "brown", y: "ipsum"},
    {_id: 14, x: "cow", y: "lorem ipsum"},
    {_id: 15, x: "crown", y: "ipsum"},
]));

// Shard the test collection, split it at {_id: 10}, and move the higher chunk to shard1.
assert.commandWorked(mongos.getDB("admin").runCommand({enableSharding: dbName}));
st.ensurePrimaryShard(dbName, st.shard0.name);
st.shardColl(testColl, {_id: 1}, {_id: 10}, {_id: 10 + 1});

const collUUID0 = getUUIDFromListCollections(st.rs0.getPrimary().getDB(dbName), collName);
const collUUID1 = getUUIDFromListCollections(st.rs1.getPrimary().getDB(dbName), collName);

const cursorId = NumberLong(123);

let shard0Conn = st.rs0.getPrimary();
let shard1Conn = st.rs1.getPrimary();

const queryVector = [1.0, 2.0, 3.0];
const path = "x";
const numCandidates = 10;
const limit = 5;

/**
 * Helper function to set the mock responses for the two shards' mongots.
 * @param {Array<Object>} shard0MockResponse
 * @param {Array<Object>} shard1MockResponse
 */
function mockMongotShardResponses(shard0MockResponse, shard1MockResponse) {
    const responseOk = 1;
    const history0 = [{
        expectedCommand: mongotCommandForKnnQuery(
            {queryVector, path, numCandidates, limit, collName, dbName, collectionUUID: collUUID0}),
        response: mongotResponseForBatch(shard0MockResponse, NumberLong(0), collNS, responseOk),
    }];
    const s0Mongot = stWithMock.getMockConnectedToHost(shard0Conn);
    s0Mongot.setMockResponses(history0, cursorId);

    const history1 = [{
        expectedCommand: mongotCommandForKnnQuery(
            {queryVector, path, numCandidates, limit, collName, dbName, collectionUUID: collUUID1}),
        response: mongotResponseForBatch(shard1MockResponse, NumberLong(0), collNS, responseOk),
    }];
    const s1Mongot = stWithMock.getMockConnectedToHost(shard1Conn);
    s1Mongot.setMockResponses(history1, cursorId);
}

// $vectorSearch can merge sort documents from shards correctly.
(function testVectorSearchMultipleBatches() {
    const pipeline = [
        {$vectorSearch: {queryVector, path, numCandidates, limit}},
        {$project: {_id: 1, score: {$meta: "vectorSearchScore"}, x: 1, y: 1}}
    ];

    const shard0MongotResponseBatch = [
        {_id: 4, $vectorSearchScore: 1.0},
        {_id: 1, $vectorSearchScore: 0.9},
        {_id: 2, $vectorSearchScore: 0.8},
    ];
    const shard1MongotResponseBatch = [
        {_id: 10, $vectorSearchScore: 0.99},
        {_id: 15, $vectorSearchScore: 0.98},
        {_id: 11, $vectorSearchScore: 0.2},
    ];

    const expectedDocs = [
        {_id: 4, x: "cow", y: "lorem ipsum", score: 1.0},
        {_id: 10, score: 0.99},
        {_id: 15, x: "crown", y: "ipsum", score: 0.98},
        {_id: 1, x: "ow", score: 0.9},
        {_id: 2, x: "now", y: "lorem", score: 0.8},
        {_id: 11, x: "brown", y: "ipsum", score: 0.2},
    ];

    mockMongotShardResponses(shard0MongotResponseBatch, shard1MongotResponseBatch);
    assert.eq(testDB[collName].aggregate(pipeline).toArray(), expectedDocs);
})();

stWithMock.stop();
})();
