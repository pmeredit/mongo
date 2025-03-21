/*
 * Tests a live restore connection that does not use the background worker threads to move data
 * from the source to the destination. All reads should be first serviced by the source.
 *
 *  @tags: [
 *     requires_replication,
 *     requires_wiredtiger,
 *     requires_persistence,
 * ]
 */

// FIXME: SERVER-102707 Re-enable live restore test
quit();

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
    wiredTigerLiveRestoreThreads: 0,
    noCleanData: true,
    setParameter: {logComponentVerbosity: tojson({storage: {wt: {wtLiveRestore: 2}}})}
});
// Ensure we've started up correctly.
assert(liveRestoreConn);

// Check we can read all the data.
db = liveRestoreConn.getDB("test");
coll = db.getCollection(jsTestName());
assert.eq(coll.find().itcount(), numDocs);

MongoRunner.stopMongod(liveRestoreConn);
