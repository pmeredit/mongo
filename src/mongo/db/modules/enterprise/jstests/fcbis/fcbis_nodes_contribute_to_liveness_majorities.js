/*
 * Tests that nodes in FCBIS are considered a part of the liveness majority. This test starts with a
 * two-node replica set. We then add a secondary and force it to hang in FCBIS. We proceed to remove
 * all communication for the other secondary. We verify that the primary does not step down and no
 * elections were called, since the node in FCBIS contributes to the liveness majority. Finally, we
 * verify that nodes in FCBIS are able to vote in an election. This is done by stepping down the
 * primary, waiting for it to become a secondary, and allowing it to run for primary again. Since
 * the other secondary is disconnected, the primary must receive a vote from the FCBIS node to get
 * elected again.
 *
 * @tags: [requires_persistence, requires_wiredtiger]
 */
import {kDefaultWaitForFailPointTimeout} from "jstests/libs/fail_point_util.js";
import {reconfig, waitForState} from "jstests/replsets/rslib.js";

TestData.skipEnforceFastCountOnValidate = true;
const name = jsTestName();
const rst = new ReplSetTest({
    name,
    nodes: [{}, {rsConfig: {priority: 0}}],
    settings: {electionTimeoutMillis: 3000, heartbeatIntervalMillis: 250},
    useBridge: true
});
rst.startSet();
rst.initiate();

const primary = rst.getPrimary();
const primaryDb = primary.getDB("test");

// Add some data to be cloned.
assert.commandWorked(primaryDb.test.insert([{a: 1}, {b: 2}, {c: 3}]));

jsTestLog("Ensuring there is an up-to-date stable checkpoint for FCBIS to copy from.");
rst.awaitLastStableRecoveryTimestamp();
assert.commandWorked(primary.adminCommand({fsync: 1}));

const secondary = rst.getSecondaries()[0];

const initialSyncSecondary = rst.add({
    rsConfig: {priority: 0, votes: 0},
    setParameter: {
        'initialSyncMethod': 'fileCopyBased',
        'failpoint.fCBISHangBeforeFinish': tojson({mode: 'alwaysOn'}),
    },
});

rst.reInitiate();

// Add the new node with votes:0 and then give it votes:1 to avoid 'newlyAdded' and mimic a resync,
// where a node is in initial sync with 1 vote.
let nextConfig = rst.getReplSetConfigFromNode(0);
nextConfig.members[2].votes = 1;
reconfig(rst, nextConfig, false /* force */, true /* wait */);

jsTestLog("Waiting for FCBIS node to get to end of initial sync");

assert.soonNoExcept(() => {
    assert.commandWorked(initialSyncSecondary.adminCommand({
        waitForFailPoint: "fCBISHangBeforeFinish",
        timesEntered: 1,
        maxTimeMS: kDefaultWaitForFailPointTimeout
    }));
    return true;
});

secondary.disconnect(primary);

// Verify that the primary should not step down due to not seeing a quorum. This is because the
// primary should be receiving heartbeats from the initial sync node. We specifically look for the
// 'Can't see a majority of the set, relinquishing primary' log message.
assert.throws(() => checkLog.containsJson(primary, 21809, {} /* attrs */, 3000));

assert.eq(primary, rst.getPrimary());

// Verify that the term is still 1, so the primary did not call any elections.
let primaryReplSetStatus = assert.commandWorked(primary.adminCommand({replSetGetStatus: 1}));
assert.eq(1, primaryReplSetStatus.term);

jsTestLog("Stepping down primary");

// Step down the primary and prevent it from running for elections with a high freeze timeout.
assert.commandWorked(
    primary.adminCommand({replSetStepDown: ReplSetTest.kForeverSecs, force: true}));
waitForState(primary, ReplSetTest.State.SECONDARY);

// Unfreeze the primary so that it can run for election. It should immediately run for election as
// no other nodes are able to call for an election.
assert.commandWorked(primary.adminCommand({replSetFreeze: 0}));

jsTestLog("Re-electing same primary");

// Verify that the primary was elected.
assert.eq(primary, rst.getPrimary());

// Verify that the term has incremented due to the last election.
primaryReplSetStatus = assert.commandWorked(primary.adminCommand({replSetGetStatus: 1}));
assert.gt(primaryReplSetStatus.term, 1);

// Verify that initial sync node voted in the election and sets the correct term in its
// electionParticipantMetrics field.
const initialSyncSecondaryReplSetStatus =
    assert.commandWorked(initialSyncSecondary.adminCommand({replSetGetStatus: 1}));
assert.eq(2, initialSyncSecondaryReplSetStatus.electionParticipantMetrics.electionTerm);

// The disconnected secondary did not vote in the last election, so its electionParticipantMetrics
// field should not be set.
const disconnectedSecondaryReplSetStatus =
    assert.commandWorked(secondary.adminCommand({replSetGetStatus: 1}));
assert.eq(undefined, disconnectedSecondaryReplSetStatus.electionParticipantMetrics);

// Since the primary sends a shut down command to all secondaries in `rst.stopSet()`, we reconnect
// the disconnected secondary to the primary to allow it to be shut down.
secondary.reconnect(primary);

jsTestLog("Waiting for the FCBIS node to become a secondary");

assert.commandWorked(
    initialSyncSecondary.adminCommand({configureFailPoint: "fCBISHangBeforeFinish", mode: "off"}));
waitForState(initialSyncSecondary, ReplSetTest.State.SECONDARY);

rst.stopSet();
