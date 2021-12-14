/**
 * Tests that incremental backups remain viable across a file copy based initial sync targeted at
 * the backup source.
 *
 * @tags: [
 *   requires_fcv_52,
 *   requires_persistence,
 *   requires_wiredtiger,
 * ]
 */
(function() {
"use strict";

const db1Name = "incremental_backup1";
const db2Name = "incremental_backup2";
const coll1Name = "coll1";
const coll2Name = "coll2";
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
const featureEnabled = assert
                           .commandWorked(primary.adminCommand(
                               {getParameter: 1, featureFlagFileCopyBasedInitialSync: 1}))
                           .featureFlagFileCopyBasedInitialSync.value;
if (!featureEnabled) {
    jsTestLog("Skipping test because the file copy based initial sync feature flag is disabled");
    rst.stopSet();
    return;
}

const db1 = primary.getDB(db1Name);
const coll1 = db1.getCollection(coll1Name);
const db2 = primary.getDB(db2Name);
const coll2 = db2.getCollection(coll2Name);

const x = 'x'.repeat(1 * 1024 * 1024);
const y = 'y'.repeat(1 * 1024 * 1024);
const z = 'z'.repeat(1 * 1024 * 1024);

// i = 250 is chosen as it results in files which are large enough that the incremental backup
// has offsets greater than 0.
for (let i = 0; i < 250; i++) {
    assert.commandWorked(coll1.insert({x: x, y: y, i: i}));
    assert.commandWorked(coll2.insert({y: y, z: z, i: i}));
}

// Take a stable checkpoint for the initial backup
rst.awaitLastStableRecoveryTimestamp();
assert.commandWorked(primary.adminCommand({fsync: 1}));

jsTest.log("Taking a full backup for incremental purposes.");
let backupCursor = primary.getDB("admin").aggregate(
    [{$backupCursor: {incrementalBackup: true, thisBackupName: "A"}}]);
while (backupCursor.hasNext()) {
    backupCursor.next();
}
backupCursor.close();

// Do updates between the backup and the FCBIS.
assert.commandWorked(coll1.update({i: 240}, {$set: {u: 1}}));
assert.commandWorked(coll2.insert({i: 250, z: z}));

// Take a stable checkpoint for the FCBIS
rst.awaitLastStableRecoveryTimestamp();
assert.commandWorked(primary.adminCommand({fsync: 1}));

jsTestLog("Adding the initial sync destination node to the replica set");
const initialSyncNode = rst.add({
    rsConfig: {priority: 0, votes: 0},
    setParameter: {
        'initialSyncMethod': 'fileCopyBased',
        'numInitialSyncAttempts': 1,
        'logComponentVerbosity': tojson({replication: {verbosity: 2}, storage: {verbosity: 2}}),
    }
});
rst.reInitiate();
rst.waitForState(initialSyncNode, ReplSetTest.State.SECONDARY);

// Verify that we can take an incremental backup on "A", after the file copy based initial sync
// completes.
jsTest.log("Taking an incremental backup on the previous one");
backupCursor = primary.getDB("admin").aggregate(
    [{$backupCursor: {incrementalBackup: true, thisBackupName: "B", srcBackupName: "A"}}]);
let partialFiles = 0;
while (backupCursor.hasNext()) {
    let backupData = backupCursor.next();
    if (backupData["offset"] instanceof NumberLong &&
        backupData["offset"].compare(NumberLong(0)) > 0) {
        partialFiles += 1;
    }
}
backupCursor.close();
assert.gt(
    partialFiles, 0, "Expected some incremental backup files to have a non-zero offset, none did");
rst.stopSet();
}());
