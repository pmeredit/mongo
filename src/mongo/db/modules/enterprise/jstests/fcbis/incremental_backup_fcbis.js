/**
 * Tests that incremental backups remain viable across a file copy based initial sync targeted at
 * the backup source.
 *
 * @tags: [
 *   requires_persistence,
 *   requires_wiredtiger,
 * ]
 */

import {getBackupCursorDB} from "jstests/libs/backup_utils.js";
import {ReplSetTest} from "jstests/libs/replsettest.js";

const db1Name = "incremental_backup1";
const db2Name = "incremental_backup2";
const coll1Name = "coll1";
const coll2Name = "coll2";
const testName = TestData.testName;

TestData.skipEnforceFastCountOnValidate = true;
const rst = new ReplSetTest({
    name: testName,
    nodeOptions: {
        wiredTigerCacheSizeGB: 0.256,
    },
    nodes: [{
        setParameter: {
            'initialSyncMethod': "fileCopyBased",
        }
    }]
});
rst.startSet();
rst.initiate();

const primary = rst.getPrimary();
const backupCursorDB = getBackupCursorDB(primary);

// Verify storage engine cache size in effect during recovery.
const actualCacheSizeGB = assert.commandWorked(primary.adminCommand({getCmdLineOpts: 1}))
                              .parsed.storage.wiredTiger.engineConfig.cacheSizeGB;
jsTestLog('Storage cache size of ' + actualCacheSizeGB + ' GB.');
assert.close(0.256, actualCacheSizeGB);

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
    if ((i % 10) == 9) {
        rst.awaitReplication();
    }
}

// Take a stable checkpoint for the initial backup
rst.awaitLastStableRecoveryTimestamp();
assert.commandWorked(primary.adminCommand({fsync: 1}));

jsTest.log("Taking a full backup for incremental purposes.");
let backupCursor = null;
while (!backupCursor) {
    try {
        backupCursor = backupCursorDB.aggregate(
            [{$backupCursor: {incrementalBackup: true, thisBackupName: "A"}}]);
    } catch (e) {
        if (e.code != ErrorCodes.BackupCursorOpenConflictWithCheckpoint)
            throw e;
        jsTestLog({"Failed to open a backup cursor, retrying.": e});
        // Release the incremental backup started with an inconsistent checkpoint.
        backupCursor =
            backupCursorDB.aggregate([{$backupCursor: {disableIncrementalBackup: true}}]);
        backupCursor.close();
        backupCursor = null;
    }
}
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
        'logComponentVerbosity':
            tojson({replication: {verbosity: 2, initialSync: 2}, storage: {verbosity: 2}}),
    }
});
rst.reInitiate();
rst.awaitSecondaryNodes(null, [initialSyncNode]);

// Verify that we can take an incremental backup on "A", after the file copy based initial sync
// completes.
jsTest.log("Taking an incremental backup on the previous one");
let backupInc = 1;
backupCursor = null;
while (!backupCursor) {
    try {
        let backupName = "B" + backupInc++;
        backupCursor = backupCursorDB.aggregate([{
            $backupCursor: {incrementalBackup: true, thisBackupName: backupName, srcBackupName: "A"}
        }]);
    } catch (e) {
        if (e.code != ErrorCodes.BackupCursorOpenConflictWithCheckpoint)
            throw e;
        jsTestLog({"Failed to open an incremental backup cursor, retrying.": e});
        // We can't discard just the information for the backup which just failed. So instead
        // we retry with a different backup ID, based on the original source ID.
    }
}
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
