/*
 * Tests a non-PIT replica set restore with magic restore using incremental backup cursors. The test
 * does the following:
 *
 * - Starts a replica set, inserts some initial data, and creates the incremental backup data files.
 * - Copies data files to the restore dbpath and closes the backup cursor.
 * - Writes additional entries to the replica set and takes an incremental backup cursor.
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
import {ReplSetTest} from "jstests/libs/replsettest.js";

// TODO SERVER-86034: Run on Windows machines once named pipe related failures are resolved.
if (_isWindows()) {
    jsTestLog("Temporarily skipping test for Windows variants. See SERVER-86034.");
    quit();
}

function runTest(insertHigherTermOplogEntry) {
    jsTestLog("Running non-PIT magic restore with insertHigherTermOplogEntry: " +
              insertHigherTermOplogEntry);
    let rst = new ReplSetTest({nodes: 1});
    rst.startSet();
    rst.initiate();

    let primary = rst.getPrimary();
    const dbName = "db";
    const coll = "coll";

    const db = primary.getDB(dbName);
    // Insert some data to restore. This data will be reflected in the restored node.
    ['a', 'b', 'c'].forEach(
        key => { assert.commandWorked(db.getCollection(coll).insert({[key]: 1})); });

    const magicRestoreUtils = new MagicRestoreUtils({
        backupSource: primary,
        pipeDir: MongoRunner.dataDir,
        insertHigherTermOplogEntry: insertHigherTermOplogEntry
    });
    let metadata = magicRestoreUtils.takeCheckpointAndOpenBackup(
        {incrementalBackup: true, thisBackupName: "backup"});
    assert.eq(metadata.incrementalBackup, true);
    const firstCheckpointTs = metadata.checkpointTimestamp;

    // These documents will not be captured by the first backup cursor since they were added after
    // the cursor was opened but will be captured by the second incremental cursor.
    ['e', 'f', 'g'].forEach(
        key => { assert.commandWorked(db.getCollection(coll).insert({[key]: 1})); });
    assert.eq(db.getCollection(coll).find().toArray().length, 6);

    magicRestoreUtils.assertOplogCountForNamespace(primary, dbName + "." + coll, 6, "i");
    const expectedDocs = db.getCollection(coll).find().toArray();

    magicRestoreUtils.copyFilesAndCloseBackup();

    // Take another incremental backup to capture 'e', 'f', 'g' entries.
    metadata = magicRestoreUtils.takeCheckpointAndOpenBackup(
        {incrementalBackup: true, thisBackupName: "newbackup", srcBackupName: "backup"});
    assert.eq(metadata.incrementalBackup, true);
    // Make sure the incremental backup has a greater timestamp than the initial backup.
    assert.gt(metadata.checkpointTimestamp, firstCheckpointTs);

    // This insert will be truncated since it was done after the final incremental backup cursor was
    // opened.
    assert.commandWorked(db.getCollection(coll).insert({'h': 1}));

    let {entriesAfterBackup} = magicRestoreUtils.getEntriesAfterBackup(primary);
    // Only the entry for 'h'.
    assert.eq(entriesAfterBackup.length, 1);
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

    magicRestoreUtils.writeObjsAndRunMagicRestore(
        restoreConfiguration, [], {"replSet": jsTestName()});

    // Restart the destination replica set.
    rst = new ReplSetTest({nodes: 1});
    rst.startSet({dbpath: magicRestoreUtils.getBackupDbPath(), noCleanData: true});

    primary = rst.getPrimary();
    const restoredDocs = primary.getDB(dbName).getCollection(coll).find().toArray();
    // 'a', 'b', 'c' were captured by the initial backup cursor.
    // 'e', 'f', 'g' were captured by the second incremental backup cursor.
    // 'h' was truncated during magic restore.
    assert.eq(restoredDocs.length, 6);
    assert.eq(restoredDocs, expectedDocs);

    magicRestoreUtils.postRestoreChecks({
        node: primary,
        expectedConfig: expectedConfig,
        dbName: dbName,
        collName: coll,
        expectedOplogCountForNs: 6,
        opFilter: "i",
        expectedNumDocsSnapshot: 6,
    });

    rst.stopSet();
}

// insertHigherTermOplogEntry causes a no-op oplog entry insert with a higher term. This affects the
// stable timestamp on magic restore node shutdown.
for (const insertHigherTermOplogEntry of [false, true]) {
    runTest(insertHigherTermOplogEntry);
}
