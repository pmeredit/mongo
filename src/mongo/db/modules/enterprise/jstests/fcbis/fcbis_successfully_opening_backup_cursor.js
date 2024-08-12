/**
 * Tests that backupCursor can be opened on FCBIS destination node after initial sync completed.
 * @tags: [requires_persistence, requires_wiredtiger]
 */

import {
    validateReplicaSetBackupCursor
} from "src/mongo/db/modules/enterprise/jstests/hot_backups/libs/backup_cursor_helpers.js";

TestData.skipEnforceFastCountOnValidate = true;
const testName = "fcbis";
const rst = new ReplSetTest({
    name: testName,
    nodes: [{
        setParameter: {
            'initialSyncMethod': "fileCopyBased",
        }
    }]
});
rst.startSet();
rst.initiate();

const primary = rst.getPrimary();
const primaryDb = primary.getDB("test");

jsTestLog("Inserting data to the primary before adding the initial sync node.");
for (var i = 1; i < 100; i++) {
    assert.commandWorked(primaryDb.test.insert([{a: i}, {b: i * 2}, {c: i * 3}]));
}
// Makes sure our stable timestamp is current.
rst.awaitLastStableRecoveryTimestamp();
// Forces a checkpoint to be taken.
assert.commandWorked(primary.adminCommand({fsync: 1}));

jsTestLog("Adding the initial sync destination node to the replica set.");
const initialSyncNode = rst.add({
    rsConfig: {},
    setParameter: {
        'initialSyncMethod': 'fileCopyBased',
        'numInitialSyncAttempts': 1,
        'logComponentVerbosity': tojson({replication: {verbosity: 2}, storage: {verbosity: 2}}),
    }
});
rst.reInitiate();
// Wait for initial sync to finish.
rst.awaitSecondaryNodes();
rst.awaitReplication();

jsTestLog("Validating that backupCursor can be opened on FCBIS node.");
validateReplicaSetBackupCursor(initialSyncNode.getDB("test"));

rst.stopSet();
