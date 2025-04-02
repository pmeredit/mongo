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
// Opt out of taking a checkpoint when we open the backup cursor so we preserve the state of the
// catalog entries before the database drop.
const backupCursor = openBackupCursor(primary.getDB("admin"), {takeCheckpoint: false});
const metadata = getBackupCursorMetadata(backupCursor);
copyBackupCursorFiles(
    backupCursor, /*namespacesToSkip=*/[], metadata.dbpath, backupPath, false /* async */);
backupCursor.close();

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