/**
 * Tests that the drop pending ident reaper is paused while a backup is in progress. The drop
 * should continue and complete after the backup cursor is closed.
 *
 * @tags: [
 *     requires_persistence,
 *     requires_replication,
 *     requires_wiredtiger
 * ]
 */
import {openBackupCursor} from "jstests/libs/backup_utils.js";
import {ReplSetTest} from "jstests/libs/replsettest.js";

const rst = new ReplSetTest({
    nodes: 1,
    nodeOptions: {
        setParameter: {
            // Set the history window to zero to explicitly control the oldest timestamp.
            minSnapshotHistoryWindowInSeconds: 0,
            logComponentVerbosity: tojson({storage: 1}),
        }
    }
});
rst.startSet();
rst.initiate();

const primary = rst.getPrimary();
const dbName = "test";
const db = primary.getDB(dbName);

assert.commandWorked(db.createCollection("a"));
let cursor = openBackupCursor(primary.getDB("admin"));

assert(db.getCollection("a").drop());

// Check that the drop-pending ident reaper thread is paused.
checkLog.containsJson(primary, 9810500);
cursor.close();

// Perform another operation and checkpoint to move the checkpoint and stable timestamps.
assert.commandWorked(db.createCollection("b"));
assert.commandWorked(db.adminCommand({fsync: 1}));

checkLog.containsJson(primary, 6776600);
rst.stopSet();
