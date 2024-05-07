/**
 * This file tests that opening a backup cursor while running with encryptdb also returns the WT
 * files to copy that are part of the keystore database.
 */

import {openBackupCursor} from "jstests/libs/backup_utils.js";

var assetsPath = "src/mongo/db/modules/enterprise/jstests/encryptdb/libs/";

var ekfValid1 = assetsPath + "ekf";
run("chmod", "600", ekfValid1);

function runTest(nodeOptionsArg) {
    let encryptedOptions = {enableEncryption: "", encryptionKeyFile: ekfValid1};
    let fullEncryptedNodeOptions = Object.assign(encryptedOptions, nodeOptionsArg);
    jsTestLog("runTest( nodeOptions: " + tojson(fullEncryptedNodeOptions) + " )");

    var conn = MongoRunner.runMongod(fullEncryptedNodeOptions);
    assert.neq(null, conn, "Mongod did not start up with a valid key file.");

    var testdb = conn.getDB("test");
    testdb["foo"].insert({x: 1});

    // Open up a backup cursor and verify the `keystore.wt` file is in the list of results.
    let backupCursor = openBackupCursor(testdb);
    // The `keystore` table is the only non-WT metadata file that can identify the backup cursor
    // as being successful.
    let foundKeystoreTable = false;
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

    // Assert that a backup cursor can be successfully closed and re-opened (i.e: the backup
    // cursor on the keystore database was not leaked).
    backupCursor.close();

    backupCursor = openBackupCursor(testdb);
    assert.gt(backupCursor.itcount(), 1);
    assert(!backupCursor.isExhausted());
    backupCursor.close();

    MongoRunner.stopMongod(conn);
}

runTest({});
runTest({directoryperdb: ""});
runTest({wiredTigerDirectoryForIndexes: ""});
runTest({wiredTigerDirectoryForIndexes: "", directoryperdb: ""});
