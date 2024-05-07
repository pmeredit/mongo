/**
 * Tests that the 'ns' field in the backup blocks is the namespace at the checkpoint timestamp the
 * backup is being taken at. Orphaned files that are no longer part of the durable catalog at the
 * checkpoint timestamp will not have a namespace. These files would be dropped during startup
 * recovery regardless if they were copied.
 *
 * @tags: [
 *     requires_persistence,
 *     requires_replication,
 *     requires_wiredtiger
 * ]
 */
import {getUriForColl, getUriForIndex} from "jstests/disk/libs/wt_file_helper.js";
import {openBackupCursor} from "jstests/libs/backup_utils.js";

const rst = new ReplSetTest({
    nodes: 1,
    nodeOptions: {
        setParameter: {
            // Set the history window to zero to explicitly control the oldest timestamp.
            minSnapshotHistoryWindowInSeconds: 0,
            logComponentVerbosity: tojson({storage: 2}),
        }
    }
});
rst.startSet();
rst.initiate();

const primary = rst.getPrimary();

const dbName = "test";
const db = primary.getDB(dbName);

// Opening backup cursors can race with taking a checkpoint, so disable checkpoints.
// This makes testing quicker and more predictable. In production, a poorly interleaved checkpoint
// will return an error, requiring retry.
assert.commandWorked(
    primary.adminCommand({configureFailPoint: "pauseCheckpointThread", mode: "alwaysOn"}));

// Control the timestamp monitor for the duration of this test. This delays the second phase of
// collection drops and allows us to test the behaviour of idents still known to the storage engine,
// but are no longer part of the backed up durable catalog. These orphaned idents would be dropped
// during startup recovery if copied during the backup.
assert.commandWorked(
    primary.adminCommand({configureFailPoint: "pauseTimestampMonitor", mode: "alwaysOn"}));

// Wait for the timestamp monitor to pause.
checkLog.containsJson(primary, 6321800);

// Create some collections with data.
// Collection "a" - No action.
// Collection "b" - Drop it before the checkpoint the backup cursor will be open on.
// Collection "c" - Drop it after the checkpoint the backup cursor will be open on.
// Collection "d" - Perform a rename before the checkpoint the backup cursor will be open on.
// Collection "e" - Perform a rename after the checkpoint the backup cursor will be open on.
// Collection "f" - Perform several renames before and after the checkpoint the backup cursor will
//                  be open on.
// Collection "g" - Same as "f", but drop the collection after the checkpoint the backup cursor will
//                  be open on.
const idents = {};
const collectionsToCreate = ["a", "b", "c", "d", "e", "f", "g"];
for (const collectionToCreate of collectionsToCreate) {
    assert.commandWorked(db.createCollection(collectionToCreate));

    const collUri = getUriForColl(db.getCollection(collectionToCreate));
    const indexUri = getUriForIndex(db.getCollection(collectionToCreate), /*indexName=*/ "_id_");

    idents[collectionToCreate] = {collUri: collUri, indexUri: indexUri};

    for (let i = 0; i < 5; i++) {
        assert.commandWorked(db.getCollection(collectionToCreate).insert({_id: i}));
    }
}

jsTestLog("Collection to idents map: " + tojson(idents));

// Take the initial checkpoint.
assert.commandWorked(db.adminCommand({fsync: 1}));

// Perform actions on collections to be part of the next checkpoint, which the backup cursor will be
// open on.
assert(db.getCollection("b").drop());
assert.commandWorked(db.getCollection("d").renameCollection("dd"));
assert.commandWorked(db.getCollection("f").renameCollection("ff"));
assert.commandWorked(db.getCollection("ff").renameCollection("fff"));
assert.commandWorked(db.getCollection("g").renameCollection("gg"));
assert.commandWorked(db.getCollection("gg").renameCollection("ggg"));

// Take the checkpoint to be used by the backup cursor.
assert.commandWorked(db.adminCommand({fsync: 1}));

// Perform actions on collections after the checkpoint.
assert(db.getCollection("c").drop());
assert.commandWorked(db.getCollection("e").renameCollection("ee"));
assert.commandWorked(db.getCollection("fff").renameCollection("ffff"));
assert.commandWorked(db.getCollection("ffff").renameCollection("fffff"));
assert.commandWorked(db.getCollection("ggg").renameCollection("gggg"));
assert.commandWorked(db.getCollection("gggg").renameCollection("ggggg"));
assert(db.getCollection("ggggg").drop());

// Perform an insert, which will be the next checkpoint's timestamp.
assert.commandWorked(db.getCollection("a").insert({_id: 100}));

function validate(backupCursor, expectedNamespaces, orphanedNamespaces, droppedNamespaces) {
    let numExpectedSeen = 0;
    let numOrphanedSeen = 0;

    while (backupCursor.hasNext()) {
        const backupBlock = backupCursor.next();
        jsTestLog("Validating: " + tojson(backupBlock));

        const kSeparator = _isWindows() ? "\\" : "/";

        const filePath =
            backupBlock.filename.substring(backupBlock.filename.lastIndexOf(kSeparator) + 1);
        const identStem = filePath.split(".")[0];
        const ns = backupBlock.ns;
        const uuid = backupBlock.uuid;

        // These were dropped before the checkpoint was taken.
        for (const droppedNamespace of droppedNamespaces) {
            // Make sure we don't see the namespace.
            assert.neq(ns, droppedNamespace);

            // Make sure we don't see the ident.
            const identKey = droppedNamespace.split(".")[1].charAt(0);
            assert.neq(idents[identKey].collUri, identStem);
            assert.neq(idents[identKey].indexUri, identStem);
        }

        // These were dropped before the checkpoint was taken, but not yet dropped from the storage
        // engine.
        for (const orphanedNamespace of orphanedNamespaces) {
            const identKey = orphanedNamespace.split(".")[1].charAt(0);

            if (idents[identKey].collUri == identStem || idents[identKey].indexUri == identStem) {
                assert.eq("", ns);
                assert.eq("", uuid);
                numOrphanedSeen++;
            }
        }

        // These are part of the checkpoint the backup cursor is open on.
        for (const expectedNamespace of expectedNamespaces) {
            const identKey = expectedNamespace.split(".")[1].charAt(0);

            if (idents[identKey].collUri == identStem || idents[identKey].indexUri == identStem) {
                assert.eq(ns, expectedNamespace);
                assert.neq("", uuid);
                numExpectedSeen++;
            }
        }
    }

    // One for the collection, one for the _id index.
    assert.eq(numOrphanedSeen, orphanedNamespaces.length * 2);
    assert.eq(numExpectedSeen, expectedNamespaces.length * 2);
}

let backupCursor = openBackupCursor(primary.getDB("admin"));

// Print the metadata document.
assert(backupCursor.hasNext());
jsTestLog(backupCursor.next());

let expectedNamespaces = ["test.a", "test.c", "test.dd", "test.e", "test.fff", "test.ggg"];
let orphanedNamespaces = ["test.b"];
let droppedNamespaces = [];
validate(backupCursor, expectedNamespaces, orphanedNamespaces, droppedNamespaces);
backupCursor.close();

// Let the timestamp monitor run to drop the orphaned collection "b".
assert.commandWorked(
    primary.adminCommand({configureFailPoint: "pauseTimestampMonitor", mode: "off"}));

checkLog.containsJson(primary, 22237, {
    ident: function(ident) {
        return ident == idents["b"].collUri;
    }
});
checkLog.containsJson(primary, 22237, {
    ident: function(ident) {
        return ident == idents["b"].indexUri;
    }
});
assert.commandWorked(
    primary.adminCommand({configureFailPoint: "pauseTimestampMonitor", mode: "alwaysOn"}));

// Verify that the collection "b" is now dropped, and no longer orphaned.
backupCursor = openBackupCursor(primary.getDB("admin"));

// Print the metadata document.
assert(backupCursor.hasNext());
jsTestLog(backupCursor.next());

expectedNamespaces = ["test.a", "test.c", "test.dd", "test.e", "test.fff", "test.ggg"];
orphanedNamespaces = [];
droppedNamespaces = ["test.b"];
validate(backupCursor, expectedNamespaces, orphanedNamespaces, droppedNamespaces);
backupCursor.close();

// Take the checkpoint to be used by the backup cursor.
assert.commandWorked(db.adminCommand({fsync: 1}));

backupCursor = openBackupCursor(primary.getDB("admin"));

// Print the metadata document.
assert(backupCursor.hasNext());
jsTestLog(backupCursor.next());

expectedNamespaces = ["test.a", "test.dd", "test.ee", "test.fffff"];
orphanedNamespaces = ["test.c", "test.ggggg"];
droppedNamespaces = ["test.b"];
validate(backupCursor, expectedNamespaces, orphanedNamespaces, droppedNamespaces);
backupCursor.close();

// Let the timestamp monitor run to drop the orphaned collections "c" and "ggggg".
assert.commandWorked(
    primary.adminCommand({configureFailPoint: "pauseTimestampMonitor", mode: "off"}));
checkLog.containsJson(primary, 22237, {
    ident: function(ident) {
        return ident == idents["c"].collUri;
    }
});
checkLog.containsJson(primary, 22237, {
    ident: function(ident) {
        return ident == idents["c"].indexUri;
    }
});
checkLog.containsJson(primary, 22237, {
    ident: function(ident) {
        return ident == idents["g"].collUri;
    }
});
checkLog.containsJson(primary, 22237, {
    ident: function(ident) {
        return ident == idents["g"].indexUri;
    }
});
assert.commandWorked(
    primary.adminCommand({configureFailPoint: "pauseTimestampMonitor", mode: "alwaysOn"}));

// Verify that the collections "c" and "ggggg" are now dropped, and no longer orphaned.
backupCursor = openBackupCursor(primary.getDB("admin"));

// Print the metadata document.
assert(backupCursor.hasNext());
jsTestLog(backupCursor.next());

expectedNamespaces = ["test.a", "test.dd", "test.ee", "test.fffff"];
orphanedNamespaces = [];
droppedNamespaces = ["test.b", "test.c", "test.ggggg"];
validate(backupCursor, expectedNamespaces, orphanedNamespaces, droppedNamespaces);
backupCursor.close();

assert.commandWorked(
    primary.adminCommand({configureFailPoint: "pauseTimestampMonitor", mode: "off"}));
assert.commandWorked(
    primary.adminCommand({configureFailPoint: "pauseCheckpointThread", mode: "off"}));

rst.stopSet();
