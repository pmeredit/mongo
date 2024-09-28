/**
 * Tests that with all combinations of 'directoryPerDb'/'wiredTigerDirectoryForIndexes' an
 * incremental backup cursor emits ns and uuid correctly.
 *
 * @tags: [
 *   requires_persistence,
 *   requires_wiredtiger,
 * ]
 */

import {checkBackup, getBackupCursorDB, openBackupCursor} from "jstests/libs/backup_utils.js";
import {ReplSetTest} from "jstests/libs/replsettest.js";

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
