/**
 * This file tests that opening a backup cursor while running with encryptdb also returns the WT
 * files to copy that are part of the keystore database.
 */
(function() {
    'use strict';

    var assetsPath = "src/mongo/db/modules/enterprise/jstests/encryptdb/libs/";

    var ekfValid1 = assetsPath + "ekf";
    run("chmod", "600", ekfValid1);

    var conn = MongoRunner.runMongod({enableEncryption: "", encryptionKeyFile: ekfValid1});
    assert.neq(null, conn, "Mongod did not start up with a valid key file.");

    var testdb = conn.getDB("test");
    testdb["foo"].insert({x: 1});

    // Open up a backup cursor and verify the `keystore.wt` file is in the list of results.
    let backupCursor = testdb.aggregate([{$backupCursor: {}}]);
    // The `keystore` table is the only non-WT metadata file that can identify the backup cursor
    // as being successful.
    let foundKeystoreTable = false;
    while (backupCursor.hasNext()) {
        let doc = backupCursor.next();
        if (!doc["filename"]) {
            continue;
        }

        foundKeystoreTable = foundKeystoreTable || doc["filename"].indexOf("keystore.wt") > -1;
    }
    assert(foundKeystoreTable);

    // Assert that a backup cursor can be successfully closed and re-opened (i.e: the backup
    // cursor on the keystore database was not leaked).
    backupCursor.close();

    backupCursor = testdb.aggregate([{$backupCursor: {}}]);
    assert.gt(backupCursor.itcount(), 1);
    assert(!backupCursor.isExhausted());
    backupCursor.close();

    MongoRunner.stopMongod(conn);
})();
