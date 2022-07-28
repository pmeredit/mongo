/**
 * Test that a $limit gets pushed to the shards.
 */
(function() {
"use strict";

load("jstests/libs/uuid_util.js");                 // For getUUIDFromListCollections.
load("jstests/libs/collection_drop_recreate.js");  // For assertCreateCollection.
load("src/mongo/db/modules/enterprise/jstests/search/lib/mongotmock.js");
load("src/mongo/db/modules/enterprise/jstests/search/lib/shardingtest_with_mongotmock.js");
load("jstests/libs/feature_flag_util.js");

const dbName = "test";
const collName = "internal_search_mongot_remote";

const stWithMock = new ShardingTestWithMongotMock({
    name: "sharded_search",
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

// Shard the test collection, split it at {_id: 10}, and move the higher chunk to shard1.
assert.commandWorked(mongos.getDB("admin").runCommand({enableSharding: dbName}));
st.ensurePrimaryShard(dbName, st.shard0.name);
st.shardColl(testColl, {_id: 1}, {_id: 10}, {_id: 10 + 1});

assert.commandWorked(testColl.insert({_id: 1, x: "ow"}));
assert.commandWorked(testColl.insert({_id: 2, x: "now", y: "lorem"}));
assert.commandWorked(testColl.insert({_id: 3, x: "brown", y: "ipsum"}));
assert.commandWorked(testColl.insert({_id: 4, x: "cow", y: "lorem ipsum"}));
assert.commandWorked(testColl.insert({_id: 11, x: "brown", y: "ipsum"}));
assert.commandWorked(testColl.insert({_id: 12, x: "cow", y: "lorem ipsum"}));
assert.commandWorked(testColl.insert({_id: 13, x: "brown", y: "ipsum"}));
assert.commandWorked(testColl.insert({_id: 14, x: "cow", y: "lorem ipsum"}));

const collUUID0 = getUUIDFromListCollections(st.rs0.getPrimary().getDB(dbName), collName);

let mongotQuery = {};
const explainVerbosity = "executionStats";
let expectedMongotCommand = {
    search: "internal_search_mongot_remote",
    collectionUUID: collUUID0,
    query: mongotQuery,
    explain: {verbosity: explainVerbosity},
    $db: "test"
};
let cursorId = NumberLong(123);
let pipeline = [
    {$search: mongotQuery},
    {$limit: 2},
];

function runTestOnPrimaries(testFn, cursorId) {
    testDB.getMongo().setReadPref("primary");
    testFn(st.rs0.getPrimary(), st.rs1.getPrimary(), cursorId);
}

function assertLimitAbsorbed(explainRes, query) {
    let shardsArray = explainRes["shards"];
    for (let [_, individualShardObj] of Object.entries(shardsArray)) {
        let stages = individualShardObj["stages"];
        if ("returnStoredSource" in query) {
            assert.eq(stages[0]["$_internalSearchMongotRemote"].limit, 2, explainRes);

        } else {
            assert.eq(stages[1]["$_internalSearchIdLookup"].limit, 2, explainRes);
        }
    }
}

function testBasicCase(shard0Conn, shard1Conn, cursorId) {
    const history = [{
        expectedCommand: expectedMongotCommand,
        response: {explain: {"garbage": true}, ok: 1},
    }];
    const s0Mongot = stWithMock.getMockConnectedToHost(shard0Conn);
    s0Mongot.setMockResponses(history, cursorId);

    const s1Mongot = stWithMock.getMockConnectedToHost(shard1Conn);
    s1Mongot.setMockResponses(history, cursorId);

    setGenericMergePipeline(testColl.getName(), mongotQuery, dbName, stWithMock);
    const explainResult = testColl.explain(explainVerbosity).aggregate(pipeline);
    assertLimitAbsorbed(explainResult, mongotQuery);
}
runTestOnPrimaries(testBasicCase, NumberLong(123));

// Run again with storedSource.
mongotQuery = {
    returnStoredSource: true
};
pipeline = [
    {$search: mongotQuery},
    {$limit: 2},
];
expectedMongotCommand = {
    search: "internal_search_mongot_remote",
    collectionUUID: collUUID0,
    query: mongotQuery,
    explain: {verbosity: explainVerbosity},
    $db: "test"
};
runTestOnPrimaries(testBasicCase, NumberLong(124));
stWithMock.stop();
})();
