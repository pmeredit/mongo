/**
 * Verify that `$search` queries that set '$$SEARCH_META' succeed on unsharded collections on
 * sharded clusters.
 */
(function() {
"use strict";
load("src/mongo/db/modules/enterprise/jstests/search/lib/mongotmock.js");
load('jstests/libs/uuid_util.js');  // For getUUIDFromListCollections.
load("src/mongo/db/modules/enterprise/jstests/search/lib/shardingtest_with_mongotmock.js");
load("jstests/libs/feature_flag_util.js");

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
const testColl = testDB.getCollection(collName);

assert.commandWorked(testColl.insert({_id: 1, shardKey: 0, x: "ow"}));
assert.commandWorked(testColl.insert({_id: 2, shardKey: 0, x: "now", y: "lorem"}));
assert.commandWorked(testColl.insert({_id: 11, shardKey: 100, x: "brown", y: "ipsum"}));
assert.commandWorked(testColl.insert({_id: 12, shardKey: 100, x: "cow", y: "lorem ipsum"}));
// Ensure primary shard so we only set the correct mongot to have history.
st.ensurePrimaryShard(dbName, st.shard1.shardName);

const shard0Conn = st.rs0.getPrimary();
const shard1Conn = st.rs1.getPrimary();
const collUUID = getUUIDFromListCollections(testDB, testColl.getName());
const mongotQuery = {};
const searchCmd = {
    search: testColl.getName(),
    collectionUUID: collUUID,
    query: mongotQuery,
    $db: testDB.getName()
};
{
    const shard1History = [
        {
            expectedCommand: searchCmd,
            response: {
                ok: 1,
                cursor: {
                    id: NumberLong(0),
                    ns: testColl.getFullName(),
                    nextBatch: [
                        {_id: 2, score: 0.654},
                        {_id: 1, score: 0.321},
                        {_id: 11, score: .2},
                        {_id: 12, score: .5}
                    ]
                },
                vars: {SEARCH_META: {value: 1}}
            }
        },
    ];

    const s1Mongot = stWithMock.getMockConnectedToHost(shard1Conn);

    const historyObj = {
        expectedCommand: {planShardedSearch: testColl.getName(), query: mongotQuery, $db: dbName},
        response: {
            ok: 1,
            protocolVersion: NumberInt(42),
            // This test doesn't use metadata. Give a trivial pipeline.
            metaPipeline: [{$limit: 1}]
        }
    };
    const mergingPipelineHistory = [historyObj];
    s1Mongot.setMockResponses(mergingPipelineHistory, 1423);

    s1Mongot.setMockResponses(shard1History, NumberLong(123));
}

let cursor = testColl.aggregate(
    [{$search: mongotQuery}, {$project: {_id: 1, meta: "$$SEARCH_META"}}], {cursor: {}});

const expected = [
    {"_id": 2, "meta": {value: 1}},
    {"_id": 1, "meta": {value: 1}},
    {"_id": 11, "meta": {value: 1}},
    {"_id": 12, "meta": {value: 1}}
];
assert.eq(expected, cursor.toArray());
stWithMock.stop();
})();
