/**
 * This test ensures that a node being resynced with FCBIS cannot vote twice in the same term, once
 * before switching storage and once after, even after being restarted.
 *
 * The particular scenario which could cause this to happen is
 *   * A node must be being re-synced (so it is voting) with FCBIS
 *   * The node must be connected to a bare majority of the set (N/2+1 including itself)
 *   * An election must occur before the FCBIS storage change, which the node votes in.
 *   * The FCBIS must complete and apply no oplog in the new term
 *   * The node must then restart
 *   * When it comes back up, it must be connected to the OTHER half of the set.
 *
 * If the lastVote was not preserved across the storage change, at this point it will be in the
 * old term and can vote again.
 *
 * @tags: [requires_persistence, requires_wiredtiger]
 */
import {kDefaultWaitForFailPointTimeout} from "jstests/libs/fail_point_util.js";
import {ReplSetTest} from "jstests/libs/replsettest.js";

const testName = jsTestName();
const dbName = "testdb";
const collName = "testcoll";

TestData.skipEnforceFastCountOnValidate = true;
const rst = new ReplSetTest({
    name: testName,
    nodes: [{}, {}, {rsConfig: {priority: 0}}],
    settings: {chainingAllowed: false},
    useBridge: true
});
rst.startSet();
rst.initiateWithHighElectionTimeout();

const primary = rst.getPrimary();
const primaryDb = primary.getDB(dbName);
const primaryColl = primaryDb.getCollection(collName);
const initialSyncNode = rst.nodes[2];

jsTestLog("Adding some data to clone");
assert.commandWorked(primaryColl.insert({"starting": "doc"}, {writeConcern: {w: 3}}));

jsTestLog("Ensuring there is an up-to-date stable checkpoint for FCBIS to copy from.");
rst.awaitLastStableRecoveryTimestamp();
assert.commandWorked(primary.adminCommand({fsync: 1}));

jsTestLog("Restarting a node so it will do an initial sync. Stopping it after cloning files");
rst.restart(initialSyncNode, {
    startClean: true,
    setParameter: {
        'initialSyncMethod': "fileCopyBased",
        'failpoint.fCBISHangAfterFileCloning': tojson({mode: 'alwaysOn'}),
        'numInitialSyncAttempts': 1,
        'logComponentVerbosity': tojson({replication: {verbosity: 2}}),
    }
});

assert.commandWorked(initialSyncNode.adminCommand({
    waitForFailPoint: "fCBISHangAfterFileCloning",
    timesEntered: 1,
    maxTimeMS: kDefaultWaitForFailPointTimeout
}));

jsTestLog("Stepping down the primary");
assert.commandWorked(primary.adminCommand({replSetStepDown: 60}));
// Trigger an election that requires the initial syncing node's vote.
rst.nodes[0].disconnect(rst.nodes[1]);
jsTestLog("Electing node 1 -- this should succeed");
// TODO (SERVER-75608): Investigate bridge flakiness.
// Sometimes the bridge connection is flaky between node[1] and initialSyncNode hence retrying.
assert.soon(() => {
    let res = rst.nodes[1].adminCommand({replSetStepUp: 1});
    return res.ok === 1;
});

// Don't let the initial sync node get oplog from the new primary.
initialSyncNode.disconnect(rst.nodes[1]);

jsTestLog("Allowing initial sync to continue");
assert.commandWorked(
    initialSyncNode.adminCommand({configureFailPoint: "fCBISHangAfterFileCloning", mode: "off"}));

jsTestLog("Waiting for initial sync to complete");
rst.awaitSecondaryNodes(undefined, [initialSyncNode]);

// Restarting the nodes makes it re-initialize its term from the oplog.  We disconnect
// them while we do it to avoid it getting a higher term from elsewhere.
jsTestLog("Restarting the node we initial synced");
initialSyncNode.disconnect(rst.nodes[0]);
rst.restart(initialSyncNode, {
    startClean: false,
    setParameter: {
        'logComponentVerbosity': tojson({replication: {verbosity: 2}}),
    }
});

jsTestLog("Waiting for initial sync node to become secondary again");
rst.awaitSecondaryNodes(undefined, [initialSyncNode]);

let initialSyncNodeStatus =
    assert.commandWorked(initialSyncNode.adminCommand({replSetGetStatus: 1}));
let node1Status = assert.commandWorked(rst.nodes[1].adminCommand({replSetGetStatus: 1}));

// Make sure it has the new term.
assert.eq(node1Status.term, initialSyncNodeStatus.term);
// Make sure it didn't get the correct term from the oplog.
assert.lt(initialSyncNodeStatus.optimes.appliedOpTime.t, initialSyncNodeStatus.term);

rst.nodes[1].reconnect(rst.nodes);
initialSyncNode.reconnect(rst.nodes);

rst.stopSet();
