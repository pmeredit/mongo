/**
 * Tests that the ident metadata is pulled from the catalog using a checkpoint cursor, if it exists.
 * Otherwise we fallback to retrieving the ident metadata from the latest catalog as a best guess.
 *
 * @tags: [
 *   requires_persistence,
 *   requires_replication,
 * ]
 */
import {openBackupCursor} from "jstests/libs/backup_utils.js";
import {ReplSetTest} from "jstests/libs/replsettest.js";

const rst = new ReplSetTest({
    nodes: 1,
    nodeOptions: {
        setParameter:
            // Control checkpoints.
            {"failpoint.pauseCheckpointThread": tojson({mode: "alwaysOn"})}
    }
});
rst.startSet();
rst.initiate();

let primary = rst.getPrimary();
let primaryDB = primary.getDB("test");

// Take the initial checkpoint.
assert.commandWorked(primaryDB.adminCommand({fsync: 1}));

// Create "a" and ensure it is in the checkpoint.
assert.commandWorked(primaryDB.createCollection("a"));
assert.commandWorked(primaryDB.adminCommand({fsync: 1}));

// Create "b", which will not be part of the checkpoint used by $backupCursor.
assert.commandWorked(primaryDB.createCollection("b"));

// Rename "a" to "c". The backup cursor will still report the namespace as "a".
assert.commandWorked(primaryDB.adminCommand({renameCollection: "test.a", to: "test.c"}));

let namespacesFound = {};
let backupCursor = openBackupCursor(primary.getDB("admin"));
while (backupCursor.hasNext()) {
    let doc = backupCursor.next();
    jsTestLog(doc);

    if (namespacesFound.hasOwnProperty(doc.ns)) {
        namespacesFound[doc.ns] += 1;
    } else {
        namespacesFound[doc.ns] = 1;
    }
}

// Two entries for these namespaces. One for the collection, and one for the _id index.
assert.eq(2, namespacesFound["test.a"]);
assert.eq(2, namespacesFound["test.b"]);

// The rename was not part of the checkpoint.
assert.eq(undefined, namespacesFound["test.c"]);

backupCursor.close();

assert.commandWorked(
    primary.adminCommand({configureFailPoint: "pauseCheckpointThread", mode: "off"}));
rst.stopSet();