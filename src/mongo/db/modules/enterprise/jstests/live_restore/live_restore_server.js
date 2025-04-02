/**
 * Tests the live restore process. By default, live restore uses a background server to manage
 * multiple threads that perform the data migration. This occurs in conjunction with any reads or
 * writes that are issued while live restore is in progress. The background threads ensure that
 * all data will be written to the destination.
 *
 * @tags: [
 *     requires_replication,
 *     requires_wiredtiger,
 *     requires_persistence,
 * ]
 */

// FIXME: WT-14051 Live restore does not support windows.
if (_isWindows()) {
    print("Skipping test on windows");
    quit();
}

import {
    copyBackupCursorFiles,
    getBackupCursorMetadata,
    openBackupCursor
} from "jstests/libs/backup_utils.js";

const numDocs = 100;
const MB = 1024 * 1024;
const liveRestoreState = {
    INIT: 0,
    IN_PROGRESS: 1,
    COMPLETE: 2
};

// Set the paths to use.
const dbpath = MongoRunner.dataPath + 'live_restore';
const dstPath = MongoRunner.dataPath + 'live_restore_destination';
const backupPath = MongoRunner.dataPath + "live_restore_backup";
mkdir(backupPath);

// Start a standalone connection to insert some documents.
let conn = MongoRunner.runMongod({dbpath: dbpath});
let db = conn.getDB("test");
let coll = db.getCollection(jsTestName());

for (let i = 0; i < numDocs; i++) {
    assert.commandWorked(coll.insert({a: i}));
}

// Force a checkpoint.
assert.commandWorked(db.adminCommand({fsync: 1}));

// Take a full backup.
let adminDB = conn.getDB("admin");
const backupCursor = openBackupCursor(adminDB);
const metadata = getBackupCursorMetadata(backupCursor);
copyBackupCursorFiles(
    backupCursor, /*namespacesToSkip=*/[], metadata.dbpath, backupPath, false /*async*/);

MongoRunner.stopMongod(conn);

// Open a connection in a new directory using the backup path as the source for live restore.
const liveRestoreConn = MongoRunner.runMongod({
    dbpath: dstPath,
    wiredTigerLiveRestoreSource: backupPath,
    wiredTigerLiveRestoreThreads: 8,
    wiredTigerLiveRestoreReadSizeMB: 1,
    noCleanData: true,
    setParameter: {logComponentVerbosity: tojson({storage: {wt: {wtLiveRestore: 2}}})}
});
// Ensure we've started up correctly.
assert(liveRestoreConn);

// Wait for the restore to finish.
adminDB = liveRestoreConn.getDB("admin");
db = liveRestoreConn.getDB("test");
coll = db.getCollection(jsTestName());
let serverStatus = adminDB.runCommand({serverStatus: 1});
let liveRestoreCurrentState = serverStatus["wiredTiger"]["live-restore"]["state"];
while (liveRestoreCurrentState != liveRestoreState.COMPLETE) {
    serverStatus = adminDB.runCommand({serverStatus: 1});
    liveRestoreCurrentState = serverStatus["wiredTiger"]["live-restore"]["state"];
}

// Check we can read all the data.
db = liveRestoreConn.getDB("test");
coll = db.getCollection(jsTestName());
assert.eq(coll.find().itcount(), numDocs);

MongoRunner.stopMongod(liveRestoreConn);

conn = MongoRunner.runMongod({
    dbpath: dstPath,
    noCleanData: true,
});
assert(conn);

MongoRunner.stopMongod(conn);
