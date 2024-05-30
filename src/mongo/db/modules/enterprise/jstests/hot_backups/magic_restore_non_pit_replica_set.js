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
 *     requires_wiredtiger
 * ]
 */

import {MagicRestoreUtils} from "jstests/libs/backup_utils.js";

// TODO SERVER-86034: Run on Windows machines once named pipe related failures are resolved.
if (_isWindows()) {
    jsTestLog("Temporarily skipping test for Windows variants. See SERVER-86034.");
    quit();
}

function runTest(insertHigherTermOplogEntry, testAuth) {
    jsTestLog("Running non-PIT magic restore with insertHigherTermOplogEntry: " +
              insertHigherTermOplogEntry);
    let rst = new ReplSetTest({nodes: 1});
    rst.startSet();
    rst.initiateWithHighElectionTimeout();

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

    const magicRestoreUtils = new MagicRestoreUtils({
        backupSource: primary,
        pipeDir: MongoRunner.dataDir,
        isPit: false,
        insertHigherTermOplogEntry: insertHigherTermOplogEntry
    });
    magicRestoreUtils.takeCheckpointAndOpenBackup();

    // These documents will be truncated by magic restore, since they were written after the backup
    // cursor was opened.
    ['e', 'f', 'g'].forEach(
        key => { assert.commandWorked(db.getCollection(coll).insert({[key]: 1})); });
    assert.eq(db.getCollection(coll).find().toArray().length, 6);

    magicRestoreUtils.assertOplogCountForNamespace(primary, dbName + "." + coll, 6, "i");
    let {entriesAfterBackup} = magicRestoreUtils.getEntriesAfterBackup(primary);
    assert.eq(entriesAfterBackup.length, 3);

    magicRestoreUtils.copyFilesAndCloseBackup();

    let expectedConfig = assert.commandWorked(primary.adminCommand({replSetGetConfig: 1})).config;
    // The new node will be allocated a new port by the test fixture.
    expectedConfig.members[0].host = getHostName() + ":" + (Number(primary.port) + 2);
    rst.stopSet(null /* signal */, false /* forRestart */, {noCleanData: true});

    let restoreConfiguration = {
        "nodeType": "replicaSet",
        "replicaSetConfig": expectedConfig,
        "maxCheckpointTs": magicRestoreUtils.getCheckpointTimestamp(),
    };
    restoreConfiguration =
        magicRestoreUtils.appendRestoreToHigherTermThanIfNeeded(restoreConfiguration);

    const magicRestoreDebugPath = MongoRunner.dataDir + "/magic_restore_debug.log";
    magicRestoreUtils.writeObjsAndRunMagicRestore(
        restoreConfiguration, [], {"replSet": jsTestName(), "logpath": magicRestoreDebugPath});

    // Restart the destination replica set.
    rst = new ReplSetTest({nodes: 1});
    rst.startSet({dbpath: magicRestoreUtils.getBackupDbPath(), noCleanData: true});

    primary = rst.getPrimary();
    const restoredConfig = assert.commandWorked(primary.adminCommand({replSetGetConfig: 1})).config;

    // See if auth we setup before restore is still valid.
    if (testAuth) {
        const admin = primary.getDB("admin");

        assert(!admin.auth("root", "root1"), "auth succeeded with wrong password");
        assert(admin.auth("root", "root"), "auth failed with right password");
    }

    magicRestoreUtils.assertConfigIsCorrect(expectedConfig, restoredConfig);

    const restoredDocs = primary.getDB(dbName).getCollection(coll).find().toArray();
    // The later 3 writes were truncated during magic restore.
    assert.eq(restoredDocs.length, 3);
    assert.eq(restoredDocs, expectedDocs);

    magicRestoreUtils.assertOplogCountForNamespace(primary, dbName + "." + coll, 3, "i");
    magicRestoreUtils.assertMinValidIsCorrect(primary);
    magicRestoreUtils.assertStableCheckpointIsCorrectAfterRestore(primary);
    magicRestoreUtils.assertCannotDoSnapshotRead(primary, 3 /* expectedNumDocs */);

    magicRestoreUtils.checkRestoreSpecificLogs(magicRestoreDebugPath);
    rst.stopSet();
}

// insertHigherTermOplogEntry causes a no-op oplog entry insert with a higher term. This affects the
// stable timestamp on magic restore node shutdown. testAuth causes us to create users on the node
// before restore.
for (const insertHigherTermOplogEntry of [false, true]) {
    for (const testAuth of [false, true]) {
        runTest(insertHigherTermOplogEntry, testAuth);
    }
}
