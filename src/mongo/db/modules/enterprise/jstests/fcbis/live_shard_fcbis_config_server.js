/**
 * Tests that sharding state is properly initialized on new config members that were added into live
 * config server replica sets using FCBIS.
 *
 * @tags: [requires_persistence, requires_wiredtiger]
 */

(function() {
"use strict";

// TODO(SERVER-64601): Re-enable this test and add corresponding 'requires_fcv_*' tag.
if (true) {
    jsTest.log("This test is temporary disabled.");
    return;
}

load("jstests/sharding/libs/sharding_state_test.js");

const st = new ShardingTest({config: 1, shards: {rs0: {nodes: 1}}});
const configRS = st.configRS;

// Fsync the primary first.
assert.commandWorked(configRS.getPrimary().adminCommand({fsync: 1}));

const newNode = ShardingStateTest.addReplSetNode({
    replSet: configRS,
    serverTypeFlag: "configsvr",
    newNodeParams: {"initialSyncMethod": "fileCopyBased"}
});

jsTestLog("Checking sharding state before failover.");
ShardingStateTest.checkShardingState(st);

jsTestLog("Checking sharding state after failover.");
ShardingStateTest.failoverToMember(configRS, newNode);
ShardingStateTest.checkShardingState(st);

st.stop();
})();
