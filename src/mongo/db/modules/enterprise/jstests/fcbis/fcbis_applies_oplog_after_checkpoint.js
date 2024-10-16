/**
 * Tests that oplog entries not part of the stable snapshot on the syncing node are applied
 * when no extensions are used.
 * @tags: [requires_persistence, requires_wiredtiger]
 */

import {ReplSetTest} from "jstests/libs/replsettest.js";

TestData.skipEnforceFastCountOnValidate = true;
const testName = TestData.testName;
const rst = new ReplSetTest({
    name: testName,
    nodes: [{
        setParameter: {
            'initialSyncMethod': "fileCopyBased",
        }
    }]
});
rst.startSet();
rst.initiateWithHighElectionTimeout();
const primary = rst.getPrimary();
const primaryDb = primary.getDB("test");

// Add some data to be cloned.
assert.commandWorked(primaryDb.test.insert([{a: 1}, {b: 2}, {c: 3}]));
rst.awaitReplication();

// Ensure there's an up-to-date stable checkpoint.
rst.awaitLastStableRecoveryTimestamp();
assert.commandWorked(primary.adminCommand({fsync: 1}));

assert.commandWorked(
    primary.adminCommand({configureFailPoint: "disableSnapshotting", mode: "alwaysOn"}));
jsTestLog("Doing more writes after stopping stable checkpoints");
assert.commandWorked(primaryDb.runCommand(
    {insert: "test", documents: [{a: 4}, {a: 5}, {a: 6}, {a: 7}], writeConcern: {w: 1}}));

jsTestLog("Adding the initial sync destination node to the replica set");
const initialSyncNode = rst.add({
    rsConfig: {priority: 0, votes: 0},
    setParameter: {
        'initialSyncMethod': 'fileCopyBased',
        // Avoid extension.
        'fileBasedInitialSyncMaxCyclesWithoutProgress': 1,
        // Don't start steady state replication.
        'failpoint.stopReplProducer': tojson({mode: 'alwaysOn'}),
        'numInitialSyncAttempts': 1,
        'logComponentVerbosity': tojson({replication: {verbosity: 1, initialSync: 2}})
    }
});
rst.reInitiate();
rst.waitForState(initialSyncNode, ReplSetTest.State.SECONDARY);

const initialSyncNodeDb = initialSyncNode.getDB("test");
// Fast count should be accurate because backup syncs size storer immediately before
// opening WT backup cursor.
assert.eq(7, initialSyncNodeDb.test.count());

// If we didn't actually apply the oplog entries, the actual count will be wrong.
assert.eq(7, initialSyncNodeDb.test.find({}).itcount());

assert.commandWorked(
    initialSyncNode.adminCommand({configureFailPoint: "stopReplProducer", mode: "off"}));
assert.commandWorked(
    primary.adminCommand({configureFailPoint: "disableSnapshotting", mode: "off"}));

// If we erroneously re-apply the oplog entries after restarting replication it will show up
// as an incorrect fast count in validation.
rst.stopSet();
