/**
 * Tests that unreplicated collections use two-phase drops in order to be part of a backup where the
 * catalog entry still exists for these collections.
 *
 * @tags: [
 *     requires_replication,
 *     requires_wiredtiger,
 *     requires_persistence,
 * ]
 */
import {
    copyBackupCursorFiles,
    getBackupCursorMetadata,
    openBackupCursor
} from "jstests/libs/backup_utils.js";
import {ReplSetTest} from "jstests/libs/replsettest.js";

const rst = new ReplSetTest({
    name: "backup_restore_unreplicated_collections",
    nodes: 1,
    nodeOptions: {
        // Disable checkpoints to avoid races with taking backups.
        syncdelay: 0,
        setParameter: {
            // Set the history window to zero to prevent delaying ident drops.
            minSnapshotHistoryWindowInSeconds: 0,
            // Control the timestamp monitor to prevent the system.profile collection from being
            // dropped before a backup is taken.
            "failpoint.pauseTimestampMonitor": tojson({mode: "alwaysOn"}),
            // Set storage logComponentVerbosity to one so we see the log id 22214.
            logComponentVerbosity: tojson({storage: 1}),
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

// Take a backup.
const backupPath = primary.dbpath + "backup_restore";
const backupCursor = openBackupCursor(primary.getDB("admin"));
const metadata = getBackupCursorMetadata(backupCursor);
copyBackupCursorFiles(
    backupCursor, /*namespacesToSkip=*/[], metadata.dbpath, backupPath, false /* async */);

assert.commandWorked(
    primary.adminCommand({configureFailPoint: "pauseTimestampMonitor", mode: "off"}));

rst.stopSet(/*signal=*/ null, /*forRestart=*/ true);

// Perform startup recovery on the backed up data files. The catalog entry and underlying table for
// the unreplicated "system.profile" collection should exist.
let conn = MongoRunner.runMongod({
    dbpath: backupPath,
    noCleanData: true,
});
assert(conn);

assert.eq(conn.getDB("test").getCollectionNames(), ["abc", "system.profile"]);
MongoRunner.stopMongod(conn);