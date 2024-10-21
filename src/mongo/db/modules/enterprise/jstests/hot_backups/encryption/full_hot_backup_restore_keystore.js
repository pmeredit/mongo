/**
 * This is a full integration test of hot backups with the ESE enabled for both cipher modes.
 * Specifically, this test is targeting that no KeyStore files are copied in $backupCursorExtend.
 *
 * Note: if a new database is created and a document is inserted into a collection while the
 * $backupCursor is open, then when we open $backupCursorExtend with the operation time of the
 * document insertion, we will not have to copy the KeyStore data for the new database. Instead,
 * these operations will be stored in the oplog and will be replayed when starting up a node on the
 * copied data files. The reason they are stored in the oplog is that these new operations are
 * ahead of the 'checkpointTimestamp' returned from $backupCursor. During oplog recovery, the
 * database will be assigned a new key in the KeyStore when its creation is replayed.
 *
 * @tags: [
 *   requires_persistence,
 *   requires_wiredtiger,
 * ]
 */
import {getBackupCursorDB, openBackupCursor} from "jstests/libs/backup_utils.js";
import {ReplSetTest} from "jstests/libs/replsettest.js";
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

    const replSet =
        new ReplSetTest({name: "backupCursorExtendEncryption", nodes: [encryptionOptions]});
    replSet.startSet();
    replSet.initiate();

    let primary = replSet.getPrimary();
    let primaryDB = primary.getDB("first");

    assert.writeOK(primaryDB["first"].insert({x: "A"}));

    // Prepare the new dbpath that we're going to "hot backup" into.
    const port = primary.port;
    const newDBPath = MongoRunner.dataPath + "restoredDBPath";
    removeFile(newDBPath);
    assert(mkdir(newDBPath).created);

    let cursor = openBackupCursor(primaryDB);
    let oldDbPath;
    let backupId;
    let checkpointTimestamp;
    while (cursor.hasNext()) {
        let doc = cursor.next();
        if (doc["metadata"]) {
            backupId = doc["metadata"]["backupId"];
            checkpointTimestamp = doc["metadata"]["checkpointTimestamp"];
            oldDbPath = doc["metadata"]["dbpath"];
            print("$backupCursor metadata - " + tojson(doc["metadata"]));
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

    jsTest.log("backupId: " + backupId + ", checkpointTimestamp: " + tojson(checkpointTimestamp));

    jsTest.log("Creating a new database and inserting a document");
    let newPrimaryDB = primary.getDB("second");
    let operationTime =
        assert.commandWorked(newPrimaryDB.runCommand({insert: "second", documents: [{x: "B"}]}))
            .operationTime;

    jsTest.log("Calling $backupCursorExtend with insert operation timestamp: " +
               tojson(operationTime));

    let cursorExtend = getBackupCursorDB(primary).aggregate(
        [{$backupCursorExtend: {backupId: backupId, timestamp: operationTime}}]);
    while (cursorExtend.hasNext()) {
        let doc = cursorExtend.next();
        if (doc["filename"]) {
            assert(oldDbPath);
            assert(doc["filename"].startsWith(oldDbPath));

            // Extending a cursor does not currently return additional KeyStore files, but there's
            // no harm in doing so if needed.
            assert(!doc["filename"].includes("key.store"));

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

    cursorExtend.close();
    cursor.close();

    replSet.stopSet();

    let newReplSet = new ReplSetTest({
        name: "backupCursorExtendEncryption",
        nodes: [encryptionOptions],
        nodeOptions: {port: port, dbpath: newDBPath, restart: true}
    });
    newReplSet.startSet();

    let newPrimary = newReplSet.getPrimary();
    let firstDB = newPrimary.getDB("first");
    let secondDB = newPrimary.getDB("second");

    let firstCursor = firstDB.getCollection("first").find({}, {_id: 0, x: 1});
    let secondCursor = secondDB.getCollection("second").find({}, {_id: 0, x: 1});

    assert.docEq(firstCursor.next(), {x: "A"});
    assert.docEq(secondCursor.next(), {x: "B"});

    assert(!firstCursor.hasNext());
    assert(!secondCursor.hasNext());
    newReplSet.stopSet();
};

runTest("AES256-CBC");

if (platformSupportsGCM) {
    runTest("AES256-GCM");
}
