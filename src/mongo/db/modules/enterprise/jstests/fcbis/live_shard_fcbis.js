/**
 * Tests that sharding state is properly initialized on new shard members that were added into live
 * shards using FCBIS.
 *
 * We control our own failovers, and we also need the RSM to react reasonably quickly to those.
 * @tags: [
 *  requires_fcv_60,
 *  requires_persistence,
 *  requires_wiredtiger,
 *  does_not_support_stepdowns,
 *  requires_streamable_rsm
 * ]
 */

import {ShardingTest} from "jstests/libs/shardingtest.js";
import {ShardingStateTest} from "jstests/sharding/libs/sharding_state_test.js";

TestData.skipEnforceFastCountOnValidate = true;
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