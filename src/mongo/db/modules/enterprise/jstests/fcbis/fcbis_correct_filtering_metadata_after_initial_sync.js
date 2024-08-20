/**
 * Verifies the correctness of filtering metadata after a file copy based initial sync. The test
 * adds a new node to a shard replica set (which triggers an initial sync), then relies on a stale
 * router to forward a query to the newly added node and verifies the correctness of the returned
 * data.
 *
 * @tags: [requires_persistence, requires_wiredtiger]
 */

import {ShardingTest} from "jstests/libs/shardingtest.js";
import {ShardingStateTest} from "jstests/sharding/libs/sharding_state_test.js";

const st = new ShardingTest({config: 1, shards: {rs0: {nodes: 1}, rs1: {nodes: 2}}, mongos: 2});

const dbName = "test";
const collName = "coll";
const ns = dbName + "." + collName;
const staleMongos = st.s1;
const mongos = st.s0;
const nDocs = 100;

// Create a collection as UNTRACKED and refresh the stale router so that it will install the version
// as UNTRACKED.
assert.commandWorked(
    mongos.adminCommand({enableSharding: dbName, primaryShard: st.shard0.shardName}));
assert.commandWorked(st.s0.getDB(dbName).createCollection(collName));
staleMongos.getDB(dbName).getCollection(collName).find({}).toArray();

// Shard the collection using the non-stale router.
/*
 * current shard version for test.coll:
 * staleMongos: UNTRACKED
 * mongos: TRACKED
 */
assert.commandWorked(mongos.adminCommand({shardCollection: ns, key: {x: 1}, unique: false}));

// Insert some data and balance them equally among the 2 shards (half amount of docs per shard).
for (var i = 0; i < nDocs; i++) {
    mongos.getDB(dbName).getCollection(collName).insert({x: i});
}
const midDocs = nDocs / 2;
assert.commandWorked(mongos.adminCommand(
    {moveRange: ns, toShard: st.shard1.shardName, min: {x: midDocs}, max: {x: MaxKey}}));

// Initial sync a new node as primary (primary here is needed just to make sure we can direct query
// to it).
ShardingStateTest.addReplSetNode({
    replSet: st.rs0,
    serverTypeFlag: "shardsvr",
    newNodeParams: {initialSyncMethod: "fileCopyBased"}
});

// The find should return all the documents.
staleMongos.setReadPref("secondary");
var res = staleMongos.getDB(dbName).getCollection(collName).find({}).itcount();
assert.eq(nDocs, res);

st.stop();
