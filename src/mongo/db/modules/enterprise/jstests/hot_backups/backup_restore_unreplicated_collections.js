/**
 * All collection drops are non-transactional and unreplicated collections are dropped immediately
 * as they do not use two-phase drops. This test creates a scenario where there are collections in
 * the catalog that are unknown to the storage engine after restoring from backed up data files. See
 * SERVER-55552.
 *
 * @tags: [
 *     requires_replication,
 *     requires_wiredtiger,
 *     requires_persistence,
 * ]
 */
(function() {
"use strict";

load("jstests/libs/backup_utils.js");

const rst = new ReplSetTest({
    name: "backup_restore_unreplicated_collections",
    nodes: 1,
    nodeOptions: {
        // Disable checkpoints to avoid races with taking backups.
        syncdelay: 0,
        setParameter: {
            // Set the history window to zero to prevent delaying ident drops.
            minSnapshotHistoryWindowInSeconds: 0
        }
    }
});

rst.startSet();
rst.initiate();

const primary = rst.getPrimary();
const db = primary.getDB("test");

assert.commandWorked(db.createCollection("abc"));
assert.commandWorked(db.createCollection("system.profile"));

// Take a checkpoint and drop the database immediately after.
assert.commandWorked(db.adminCommand({fsync: 1}));
assert.commandWorked(db.dropDatabase());

// Deferring the ident drop for both of the collections.
checkLog.containsJson(primary, 22214, {namespace: "test.abc"});
checkLog.containsJson(primary, 22214, {namespace: "test.system.profile"});

// The unreplicated "system.profile" collection doesn't have a drop timestamp.
checkLog.containsJson(primary, 22237, {dropTimestamp: {$timestamp: {t: 0, i: 0}}});

// Take a backup.
const backupPath = primary.dbpath + "backup_restore";
const backupCursor = openBackupCursor(primary);
const metadata = getBackupCursorMetadata(backupCursor);
copyBackupCursorFiles(
    backupCursor, /*namespacesToSkip=*/[], metadata.dbpath, backupPath, false /* async */);

rst.stopSet(/*signal=*/ null, /*forRestart=*/ true);

// Perform startup recovery on the backed up data files. The catalog entry for "system.profile"
// should still exist but its underlying table does not.
let conn = MongoRunner.runMongod({
    dbpath: backupPath,
    noCleanData: true,
});
assert(conn);

// Removed unknown unreplicated collection from the catalog.
checkLog.containsJson(conn, 5555201, {namespace: "test.system.profile"});
MongoRunner.stopMongod(conn);
}());
