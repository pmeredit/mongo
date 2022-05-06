/**
 * Tests that sharding state is properly initialized on new shard members that were added into live
 * shards using FCBIS.
 *
 * @tags: [requires_persistence, requires_wiredtiger]
 */

(function() {
"use strict";

// TODO(SERVER-64628): Re-enable this test and add corresponding 'requires_fcv_*' tag.
if (true) {
    jsTest.log("This test is temporary disabled.");
    return;
}

load("jstests/sharding/libs/sharding_state_test.js");

const st = new ShardingTest({config: 1, shards: {rs0: {nodes: 1}}});
const rs = st.rs0;

// Fsync the primary first,
assert.commandWorked(rs.getPrimary().adminCommand({fsync: 1}));

const newNode = ShardingStateTest.addReplSetNode({
    replSet: rs,
    serverTypeFlag: "shardsvr",
    newNodeParams: {"initialSyncMethod": "fileCopyBased"}
});

jsTestLog("Checking sharding state before failover.");
ShardingStateTest.checkShardingState(st);

jsTestLog("Checking sharding state after failover.");
ShardingStateTest.failoverToMember(rs, newNode);
ShardingStateTest.checkShardingState(st);

st.stop();
})();
