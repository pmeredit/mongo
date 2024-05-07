/**
 * Tests that with all combinations of 'directoryPerDb'/'wiredTigerDirectoryForIndexes' an
 * incremental backup cursor emits ns and uuid correctly.
 *
 * @tags: [
 *   requires_persistence,
 *   requires_wiredtiger,
 * ]
 */

import {getBackupCursorDB, openBackupCursor} from "jstests/libs/backup_utils.js";

function checkBackup(backupCursor) {
    // Print the metadata document.
    assert(backupCursor.hasNext());
    jsTestLog(backupCursor.next());

    while (backupCursor.hasNext()) {
        let doc = backupCursor.next();

        jsTestLog("File for backup: " + tojson(doc));

        if (!doc.required) {
            assert.neq(doc.ns, "");
            assert.neq(doc.uuid, "");
        } else {
            let pathsep = _isWindows() ? '\\' : '/';
            let stem = doc.filename.substr(doc.filename.lastIndexOf(pathsep) + 1);
            // Denylisting internal files that don't need to have ns/uuid set. Denylisting known
            // patterns will help catch subtle API changes if new filename patterns are added that
            // don't generate ns/uuid.
            if (!stem.startsWith("size") && !stem.startsWith("Wired") && !stem.startsWith("_")) {
                assert.neq(doc.ns, "");
                assert.neq(doc.uuid, "");
            }
        }
    }
}

function runTest(nodeOptionsArg) {
    jsTestLog("runTest( nodeOptions: " + tojson(nodeOptionsArg) + " )");
    const rst = new ReplSetTest({nodes: 1, nodeOptions: nodeOptionsArg});
    rst.startSet();
    rst.initiate();

    const primary = rst.getPrimary();
    const dbName = "test";
    const db = primary.getDB(dbName);

    // Opening backup cursors can race with taking a checkpoint, so disable checkpoints.
    // This makes testing quicker and more predictable. In production, a poorly interleaved
    // checkpoint will return an error, requiring retry.
    assert.commandWorked(
        primary.adminCommand({configureFailPoint: 'pauseCheckpointThread', mode: 'alwaysOn'}));

    // Create two collections. One before the checkpoint, one after the checkpoint.
    assert.commandWorked(db.createCollection("a"));
    db.a.insert({x: 1});

    // Take the checkpoint to be used by the first incremental backup cursor.
    assert.commandWorked(db.adminCommand({fsync: 1}));

    assert.commandWorked(db.createCollection("b"));

    const backupCursorDB = getBackupCursorDB(primary);
    let backupCursor =
        openBackupCursor(backupCursorDB, {incrementalBackup: true, thisBackupName: 'A0'});

    jsTestLog("Checking the first full incremental backup call.");
    checkBackup(backupCursor);

    backupCursor.close();

    // Take the checkpoint to be used by the second incremental backup cursor.
    assert.commandWorked(db.adminCommand({fsync: 1}));

    assert.commandWorked(db.createCollection("c"));

    backupCursor = openBackupCursor(
        backupCursorDB, {incrementalBackup: true, thisBackupName: 'A1', srcBackupName: 'A0'});

    jsTestLog("Checking the second incremental backup call (the actual incremental backup).");
    checkBackup(backupCursor);

    backupCursor.close();

    assert.commandWorked(
        primary.adminCommand({configureFailPoint: 'pauseCheckpointThread', mode: 'off'}));

    rst.stopSet();
}

runTest({});
runTest({directoryperdb: ""});
// TODO(SERVER-89919): Re-enable these test cases in separate files. Currently run into test
// infra/evergreen limits.
// runTest({wiredTigerDirectoryForIndexes: ""});
// runTest({wiredTigerDirectoryForIndexes: "", directoryperdb: ""});
