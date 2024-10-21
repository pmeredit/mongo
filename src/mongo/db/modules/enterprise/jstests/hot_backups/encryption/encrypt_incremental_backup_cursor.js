/**
 * This file tests that opening an incremental backup cursor while running with encryptdb also
 * returns the WT files to copy that are part of the keystore database AND that the proper ns and
 * uuid are reported in various server configurations.
 *
 * @tags: [
 *   requires_persistence,
 *   requires_wiredtiger,
 * ]
 */

import {openBackupCursor} from "jstests/libs/backup_utils.js";

var assetsPath = "src/mongo/db/modules/enterprise/jstests/encryptdb/libs/";

var ekfValid1 = assetsPath + "ekf";
run("chmod", "600", ekfValid1);

function checkEncryptedBackup(backupCursor) {
    // The `keystore` table is the only non-WT metadata file that can identify the backup cursor
    // as being successful.
    let foundKeystoreTable = false;

    // Print the metadata document.
    assert(backupCursor.hasNext());
    jsTestLog(backupCursor.next());

    while (backupCursor.hasNext()) {
        let doc = backupCursor.next();

        if (!doc["filename"]) {
            continue;
        }

        jsTestLog("File for backup: " + tojson(doc));

        // All files for the encrypted storage engine need to be marked as required.
        if (doc["filename"].includes("key.store")) {
            assert(doc["required"]);
        } else if (!doc.required) {
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

        foundKeystoreTable = foundKeystoreTable || doc["filename"].indexOf("keystore.wt") > -1;
    }

    assert(foundKeystoreTable);
}

function runTest(nodeOptionsArg) {
    let encryptedOptions = {enableEncryption: "", encryptionKeyFile: ekfValid1};
    let fullEncryptedNodeOptions = Object.assign(encryptedOptions, nodeOptionsArg);
    jsTestLog("runTest( nodeOptions: " + tojson(fullEncryptedNodeOptions) + " )");

    var conn = MongoRunner.runMongod(fullEncryptedNodeOptions);
    assert.neq(null, conn, "Mongod did not start up with a valid key file.");

    // Opening backup cursors can race with taking a checkpoint, so disable checkpoints.
    // This makes testing quicker and more predictable. In production, a poorly interleaved
    // checkpoint will return an error, requiring retry.
    assert.commandWorked(
        conn.adminCommand({configureFailPoint: 'pauseCheckpointThread', mode: 'alwaysOn'}));

    var testdb = conn.getDB("test");

    // Create two collections. One before the checkpoint, one after the checkpoint.
    assert.commandWorked(testdb.createCollection("a"));
    testdb.a.insert({x: 1});
    testdb.a.insert({x: 2});

    // Take the checkpoint to be used by the first incremental backup cursor.
    assert.commandWorked(testdb.adminCommand({fsync: 1}));

    assert.commandWorked(testdb.createCollection("b"));
    testdb.a.insert({x: 3});
    testdb.a.insert({x: 4});
    testdb.b.insert({x: 1});

    // Open up a backup cursor and verify the `keystore.wt` file is in the list of results.
    let backupCursor = openBackupCursor(testdb);
    jsTestLog("Checking the first full incremental backup call.");
    checkEncryptedBackup(backupCursor);
    backupCursor.close();

    // Take the checkpoint to be used by the second incremental backup cursor.
    assert.commandWorked(testdb.adminCommand({fsync: 1}));

    assert.commandWorked(testdb.createCollection("c"));
    testdb.b.insert({x: 2});
    testdb.c.insert({x: 1});

    backupCursor = openBackupCursor(testdb);
    jsTestLog("Checking the second incremental backup call (the actual incremental backup).");
    checkEncryptedBackup(backupCursor);
    backupCursor.close();

    MongoRunner.stopMongod(conn);
}

runTest({});
runTest({directoryperdb: ""});
runTest({wiredTigerDirectoryForIndexes: ""});
runTest({wiredTigerDirectoryForIndexes: "", directoryperdb: ""});
