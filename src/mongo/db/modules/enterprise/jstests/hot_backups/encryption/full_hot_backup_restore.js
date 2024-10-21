/*
 * This is a full integration test of hot backups with the ESE enabled for both cipher modes.
 *
 * @tags: [
 *   requires_persistence,
 *   requires_wiredtiger,
 * ]
 */
import {openBackupCursor} from "jstests/libs/backup_utils.js";
import {
    platformSupportsGCM
} from "src/mongo/db/modules/enterprise/jstests/encryptdb/libs/helpers.js";

const assetsPath = "src/mongo/db/modules/enterprise/jstests/encryptdb/libs/";
const ekfValid1 = assetsPath + "ekf";
run("chmod", "600", ekfValid1);

const runTest = function(cipherMode) {
    const encryptionOptions = {
        enableEncryption: "",
        encryptionKeyFile: ekfValid1,
        encryptionCipherMode: cipherMode,
    };

    // Start a mongod with encryption enabled and make sure we can write documents into it
    let conn = MongoRunner.runMongod(encryptionOptions);
    assert(conn, "Mongod did not start up with a valid key file.");

    let testdb = conn.getDB("foo");
    assert.writeOK(testdb["bar"].insert({_id: "bizz"}));

    // Prepare the new dbpath that we're going to "hot backup" into
    const newDBPath = MongoRunner.dataPath + "restoredDBPath";
    removeFile(newDBPath);
    assert(mkdir(newDBPath).created);

    // Go through all the files the backup cursor says to back up and copy them into the
    // new dbpath. This is simulating a filesystem snapshot backup.
    let cursor = openBackupCursor(testdb);

    let oldDbPath;
    while (cursor.hasNext()) {
        let doc = cursor.next();
        if (doc["metadata"]) {
            oldDbPath = doc["metadata"]["dbpath"];
        } else if (doc["filename"]) {
            assert(oldDbPath);
            assert(doc["filename"].startsWith(oldDbPath));

            const newFilePath = newDBPath + doc["filename"].substring(oldDbPath.length);
            let separator = "/";
            if (_isWindows()) {
                separator = "\\";
            }
            const newFileDirectory = newFilePath.substring(0, newFilePath.lastIndexOf(separator));
            mkdir(newFileDirectory);
            print(`Copying ${doc["filename"]} to ${newFilePath}`);
            copyFile(doc["filename"], newFilePath);
        }
    }

    // Close the backup cursor and restart mongod in the same dbpath. It should come back up
    // and we should be able to find the document we inserted.
    cursor.close();
    MongoRunner.stopMongod(conn);

    conn = MongoRunner.runMongod(Object.merge(conn.fullOptions, {restart: true}));
    assert(conn);
    testdb = conn.getDB("foo");
    assert.docEq(testdb.bar.findOne(), {_id: "bizz"});
    assert.writeOK(testdb.bar.insert({_id: "buzz"}));

    MongoRunner.stopMongod(conn);

    // Start a new mongod from the "restored" newDBPath data. It should start up and we
    // should be able to find our original test document and write new documents.
    conn =
        MongoRunner.runMongod(Object.merge(encryptionOptions, {dbpath: newDBPath, restart: true}));

    assert(conn);
    testdb = conn.getDB("foo");
    assert.docEq(testdb.bar.findOne(), {_id: "bizz"});
    assert.writeOK(testdb.bar.insert({_id: "buzz"}));

    MongoRunner.stopMongod(conn);
};

runTest("AES256-CBC");

if (platformSupportsGCM) {
    runTest("AES256-GCM");
}
