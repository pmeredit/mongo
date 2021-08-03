/**
 * Verify that `$search` queries that set '$$SEARCH_META' fail on sharded collections.
 */
(function() {
"use strict";
load("src/mongo/db/modules/enterprise/jstests/search/lib/mongotmock.js");  // For
                                                                           // mongotCommandForQuery.
load('jstests/libs/uuid_util.js');  // For getUUIDFromListCollections.
load("src/mongo/db/modules/enterprise/jstests/search/lib/shardingtest_with_mongotmock.js");

const dbName = jsTestName();
const collName = jsTestName();
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
const getSearchMetaParam = testDB.adminCommand({getParameter: 1, featureFlagSearchMeta: 1});
const isSearchMetaEnabled = getSearchMetaParam.hasOwnProperty("featureFlagSearchMeta") &&
    getSearchMetaParam.featureFlagSearchMeta.value;
if (!isSearchMetaEnabled) {
    stWithMock.stop();
    return;
}

const testColl = testDB.getCollection(collName);

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
st.shardColl(testColl, {shardKey: 1}, {shardKey: 10}, {shardKey: 10 + 1});

const shard0Conn = st.rs0.getPrimary();
const shard1Conn = st.rs1.getPrimary();
const collUUID0 = getUUIDFromListCollections(shard0Conn.getDB(dbName), collName);
const collUUID1 = getUUIDFromListCollections(shard1Conn.getDB(dbName), collName);

const mongotQuery = {};
{
    const shard0History = [
        {
            expectedCommand:
                mongotCommandForQuery(mongotQuery, testColl.getName(), testDB.getName(), collUUID0),
            response: {
                ok: 1,
                cursor: {
                    id: NumberLong(0),
                    ns: testColl.getFullName(),
                    nextBatch: [
                        {_id: 2, $searchScore: 0.654},
                        {_id: 1, $searchScore: 0.321},
                        {_id: 3, $searchScore: .2},
                        {_id: 4, $searchScore: .5}
                    ]
                },
                vars: {SEARCH_META: {value: 1}}
            }
        },
    ];

    const s0Mongot = stWithMock.getMockConnectedToHost(shard0Conn);
    s0Mongot.setMockResponses(shard0History, NumberLong(123));
}

{
    const shard1History = [
        {
            expectedCommand:
                mongotCommandForQuery(mongotQuery, testColl.getName(), testDB.getName(), collUUID1),
            response: {
                ok: 1,
                cursor: {
                    id: NumberLong(0),
                    ns: testColl.getFullName(),
                    nextBatch: [
                        {_id: 11, $searchScore: 0.654},
                        {_id: 12, $searchScore: 0.321},
                        {_id: 13, $searchScore: .2},
                        {_id: 14, $searchScore: .5}
                    ]
                },
                vars: {SEARCH_META: {value: 1}}
            }
        },
    ];

    const s1Mongot = stWithMock.getMockConnectedToHost(shard1Conn);
    s1Mongot.setMockResponses(shard1History, NumberLong(123));
}
assert.commandFailedWithCode(testDB.runCommand({
    aggregate: testColl.getName(),
    pipeline: [{$search: mongotQuery}, {$project: {val: "$$SEARCH_META"}}],
    cursor: {}
}),
                             5858100);
stWithMock.stop();
})();
