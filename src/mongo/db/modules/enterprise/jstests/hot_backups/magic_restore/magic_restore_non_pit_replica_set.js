/*
 * Tests a non-PIT replica set restore with magic restore. The test does the following:
 *
 * - Starts a replica set, inserts some initial data, and creates the backup data files.
 * - Copies data files to the restore dbpath and closes the backup cursor.
 * - Writes a restore configuration object to a named pipe via the mongo shell.
 * - Starts a mongod with --magicRestore that parses the restore configuration and exits cleanly.
 * - Restarts the initial replica set and asserts the replica set config and data are what we
 *   expect.
 *
 * @tags: [
 *     requires_persistence,
 *     requires_wiredtiger,
 *     incompatible_with_windows_tls
 * ]
 */

import {MagicRestoreTest} from "jstests/libs/magic_restore_test.js";
import {ReplSetTest} from "jstests/libs/replsettest.js";

function runTest(insertHigherTermOplogEntry, testAuth) {
    jsTestLog("Running non-PIT magic restore with insertHigherTermOplogEntry: " +
              insertHigherTermOplogEntry + " and testAuth: " + testAuth);
    let rst = new ReplSetTest({nodes: 1});
    rst.startSet();
    rst.initiate();

    let primary = rst.getPrimary();
    const dbName = "db";
    const coll = "coll";

    if (testAuth) {
        const admin = primary.getDB("admin");
        admin.createUser({user: "root", pwd: "root", roles: ["root"]});

        assert(!admin.auth("root", "root1"), "auth succeeded with wrong password");
        assert(admin.auth("root", "root"), "auth failed with right password");
    }

    const db = primary.getDB(dbName);
    // Insert some data to restore. This data will be reflected in the restored node.
    ['a', 'b', 'c'].forEach(
        key => { assert.commandWorked(db.getCollection(coll).insert({[key]: 1})); });
    const expectedDocs = db.getCollection(coll).find().toArray();

    const magicRestoreTest = new MagicRestoreTest({
        rst: rst,
        pipeDir: MongoRunner.dataDir,
        insertHigherTermOplogEntry: insertHigherTermOplogEntry
    });
    magicRestoreTest.takeCheckpointAndOpenBackup();

    // These documents will be truncated by magic restore, since they were written after the backup
    // cursor was opened.
    ['e', 'f', 'g'].forEach(
        key => { assert.commandWorked(db.getCollection(coll).insert({[key]: 1})); });
    assert.eq(db.getCollection(coll).find().toArray().length, 6);

    magicRestoreTest.assertOplogCountForNamespace(primary, {ns: dbName + "." + coll, op: "i"}, 6);
    let {entriesAfterBackup} = magicRestoreTest.getEntriesAfterBackup(primary);
    assert.eq(entriesAfterBackup.length, 3);

    magicRestoreTest.copyFilesAndCloseBackup();
    magicRestoreTest.rst.stopSet(null /* signal */, true /* forRestart */, {noCleanData: true});

    let restoreConfiguration = {
        "nodeType": "replicaSet",
        "replicaSetConfig": magicRestoreTest.getExpectedConfig(),
        "maxCheckpointTs": magicRestoreTest.getCheckpointTimestamp(),
    };
    restoreConfiguration =
        magicRestoreTest.appendRestoreToHigherTermThanIfNeeded(restoreConfiguration);

    const magicRestoreDebugPath = MongoRunner.dataDir + "/magic_restore_debug.log";

    // When using the 'logpath' parameter, magic restore outputs logs to a separate log file so we
    // can test filtering logs on the RESTORE component. However, this omits mongod logs from
    // test output,so we explicitly print the contents of the log file to stdout for debugging
    // purposes.
    try {
        magicRestoreTest.writeObjsAndRunMagicRestore(
            restoreConfiguration, [], {"replSet": jsTestName(), "logpath": magicRestoreDebugPath});
    } finally {
        jsTestLog("Magic restore logs:");
        cat(magicRestoreDebugPath).split("\n").forEach((line) => { print(line); });
    }

    jsTestLog("Restart the replica set fixture on the dbpath.");
    magicRestoreTest.rst.startSet(
        {restart: true, dbpath: magicRestoreTest.getBackupDbPath(), noCleanData: true});

    primary = magicRestoreTest.rst.getPrimary();

    // See if auth we setup before restore is still valid.
    if (testAuth) {
        const admin = primary.getDB("admin");

        assert(!admin.auth("root", "root1"), "auth succeeded with wrong password");
        assert(admin.auth("root", "root"), "auth failed with right password");
    }

    const restoredDocs = primary.getDB(dbName).getCollection(coll).find().toArray();
    // The later 3 writes were truncated during magic restore.
    assert.eq(restoredDocs.length, 3);
    assert.eq(restoredDocs, expectedDocs);

    magicRestoreTest.postRestoreChecks({
        node: primary,
        dbName: dbName,
        collName: coll,
        expectedOplogCountForNs: 3,
        opFilter: "i",
        expectedNumDocsSnapshot: 3,
        logPath: magicRestoreDebugPath
    });

    magicRestoreTest.rst.stopSet();
}

// insertHigherTermOplogEntry causes a no-op oplog entry insert with a higher term. This affects the
// stable timestamp on magic restore node shutdown. testAuth causes us to create users on the node
// before restore.
for (const insertHigherTermOplogEntry of [false, true]) {
    for (const testAuth of [false, true]) {
        runTest(insertHigherTermOplogEntry, testAuth);
    }
}
