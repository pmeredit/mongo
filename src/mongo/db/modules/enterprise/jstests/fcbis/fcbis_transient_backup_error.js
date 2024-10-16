/**
 * Tests that a transient error due to a snapshot occurring when opening a backup cursor doesn't
 * result in a failed initial sync.
 *
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

// Ensure there's an up-to-date stable checkpoint for FCBIS to copy.
rst.awaitLastStableRecoveryTimestamp();
assert.commandWorked(primary.adminCommand({fsync: 1}));

// Make the first few attempts to open a backup cursor fail.
assert.commandWorked(primary.adminCommand(
    {configureFailPoint: "backupCursorForceCheckpointConflict", mode: {times: 3}}));

jsTestLog("Adding the initial sync destination node to the replica set");
const initialSyncNode = rst.add({
    rsConfig: {priority: 0, votes: 0},
    setParameter: {
        'initialSyncMethod': 'fileCopyBased',
        'numInitialSyncAttempts': 1,
        'logComponentVerbosity': tojson({replication: {verbosity: 1, initialSync: 2}}),
    }
});
rst.reInitiate();
// We use this assert instead of waitForSecondary because we want errors due to the node
// crashing to fail, not timeout.
assert.soon(() => initialSyncNode.adminCommand({hello: 1}).secondary);
rst.stopSet();
