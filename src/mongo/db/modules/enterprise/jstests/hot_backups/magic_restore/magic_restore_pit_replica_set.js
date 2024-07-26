/*
 * Tests a PIT replica set restore with magic restore. The test does the following:
 *
 * - Starts a replica set, inserts some initial data, and opens a backup cursor.
 * - Writes additional data that will be truncated by magic restore, since the writes occur after
 *   the checkpoint timestamp.
 * - Copies data files to the restore dbpath and closes the backup cursor.
 * - Writes a restore configuration object and the source oplog entries from after the checkpoint
 *   timestamp to a named pipe via the mongo shell.
 * - Starts a mongod with --magicRestore that parses the restore configuration, inserts and applies
 *   the additional oplog entries, and exits cleanly.
 * - Restarts the initial replica set and asserts the replica set config and data are what we
 *   expect.
 *
 * @tags: [
 *     requires_persistence,
 *     requires_wiredtiger
 * ]
 */

import {MagicRestoreUtils} from "jstests/libs/magic_restore_test.js";
import {ReplSetTest} from "jstests/libs/replsettest.js";

// TODO SERVER-86034: Run on Windows machines once named pipe related failures are resolved.
if (_isWindows()) {
    jsTestLog("Temporarily skipping test for Windows variants. See SERVER-86034.");
    quit();
}

// TODO SERVER-87225: Enable fast count on validate when operations applied during a restore are
// counted correctly.
TestData.skipEnforceFastCountOnValidate = true;

function runTest(insertHigherTermOplogEntry, testAuth) {
    jsTestLog("Running PIT magic restore with insertHigherTermOplogEntry: " +
              insertHigherTermOplogEntry + " and testAuth: " + testAuth);
    const sourceCluster = new ReplSetTest({nodes: 1});
    sourceCluster.startSet();
    sourceCluster.initiate();

    const sourcePrimary = sourceCluster.getPrimary();
    const dbName = "db";
    const coll = "coll";

    const sourceDb = sourcePrimary.getDB(dbName);
    // Insert some data to restore. This data will be reflected in the restored node.
    ['a', 'b', 'c'].forEach(
        key => { assert.commandWorked(sourceDb.getCollection(coll).insert({[key]: 1})); });

    // Create a user whose password will be changed after taking the backup.
    if (testAuth) {
        sourceDb.createUser({user: "user1", pwd: "pass1", roles: jsTest.basicUserRoles});
        assert(!sourceDb.auth("user1", "pass"), "auth succeeded with wrong password");
        assert(sourceDb.auth("user1", "pass1"), "auth failed with right password");
        sourceDb.logout();
    }

    // This timestamp will be used for a snapshot read.
    const snapshotTs = assert.commandWorked(sourcePrimary.adminCommand({replSetGetStatus: 1}))
                           .optimes.lastCommittedOpTime.ts;

    const magicRestoreUtils = new MagicRestoreUtils({
        rst: sourceCluster,
        pipeDir: MongoRunner.dataDir,
        insertHigherTermOplogEntry: insertHigherTermOplogEntry
    });

    magicRestoreUtils.takeCheckpointAndOpenBackup();

    if (testAuth) {
        sourceDb.createUser({user: "user2", pwd: "pass2", roles: jsTest.basicUserRoles});
        assert(!sourceDb.auth("user2", "pass1"), "auth succeeded with wrong password");
        assert(sourceDb.auth("user2", "pass2"), "auth failed with right password");

        sourceDb.logout();

        // Change password of user1.
        assert(sourceDb.auth("user1", "pass1"), "auth failed with right password");
        sourceDb.changeUserPassword("user1", "pass");
        sourceDb.logout();
        assert(!sourceDb.auth("user1", "pass1"), "auth succeeded with wrong password");
        assert(sourceDb.auth("user1", "pass"), "auth failed with right password");
    }

    // These documents will be truncated by magic restore, since they were written after the backup
    // cursor was opened. We will pass these oplog entries to magic restore to perform a PIT
    // restore, so they will be reinserted and reflected in the final state of the data.
    ['e',
     'f',
     'g',
     'h']
        .forEach(key => { assert.commandWorked(sourceDb.getCollection(coll).insert({[key]: 1})); });
    assert.eq(sourceDb.getCollection(coll).find().toArray().length, 7);

    const checkpointTimestamp = magicRestoreUtils.getCheckpointTimestamp();
    let {lastOplogEntryTs, entriesAfterBackup} =
        magicRestoreUtils.getEntriesAfterBackup(sourcePrimary);
    assert.eq(entriesAfterBackup.length, testAuth ? 6 : 4);

    magicRestoreUtils.copyFilesAndCloseBackup();

    let expectedConfig = magicRestoreUtils.getExpectedConfig();
    // The new node will be allocated a new port by the test fixture.
    expectedConfig.members[0].host = getHostName() + ":" + (Number(sourcePrimary.port) + 2);
    let restoreConfiguration = {
        "nodeType": "replicaSet",
        "replicaSetConfig": expectedConfig,
        "maxCheckpointTs": checkpointTimestamp,
        // Restore to the timestamp of the last oplog entry on the source cluster.
        "pointInTimeTimestamp": lastOplogEntryTs
    };
    restoreConfiguration =
        magicRestoreUtils.appendRestoreToHigherTermThanIfNeeded(restoreConfiguration);

    const magicRestoreDebugPath = MongoRunner.dataDir + "/magic_restore_debug.log";
    magicRestoreUtils.writeObjsAndRunMagicRestore(
        restoreConfiguration,
        entriesAfterBackup,
        {"replSet": jsTestName(), "logpath": magicRestoreDebugPath});

    // Start a new replica set fixture on the dbpath.
    const destinationCluster = new ReplSetTest({nodes: 1});
    destinationCluster.startSet({dbpath: magicRestoreUtils.getBackupDbPath(), noCleanData: true});

    const destPrimary = destinationCluster.getPrimary();

    magicRestoreUtils.postRestoreChecks({
        node: destPrimary,
        expectedConfig: expectedConfig,
        dbName: dbName,
        collName: coll,
        expectedOplogCountForNs: 7,
        opFilter: "i",
        expectedNumDocsSnapshot: 7,
        logPath: magicRestoreDebugPath
    });

    // Make sure auth settings we applied after the backup are still valid.
    if (testAuth) {
        const destDb = destPrimary.getDB(dbName);
        assert(!destDb.auth("user2", "pass1"), "auth succeeded with wrong password");
        assert(destDb.auth("user2", "pass2"), "auth failed with right password");

        destDb.logout();

        assert(!destDb.auth("user1", "pass1"), "auth succeeded with wrong password");
        assert(destDb.auth("user1", "pass"), "auth failed with right password");
    }

    // The original node still maintains the history store, so point-in-time reads will succeed.
    let res = sourcePrimary.getDB("db").runCommand(
        {find: "coll", readConcern: {level: "snapshot", atClusterTime: snapshotTs}});
    assert.commandWorked(res);
    assert.eq(res.cursor.firstBatch.length, 3);

    let diff = DataConsistencyChecker.getDiff(
        sourcePrimary.getDB("db").getCollection("coll").find().sort({_id: 1}),
        destPrimary.getDB("db").getCollection("coll").find().sort({_id: 1}));

    assert.eq(diff,
              {docsWithDifferentContents: [], docsMissingOnFirst: [], docsMissingOnSecond: []});

    sourceCluster.stopSet();
    destinationCluster.stopSet();
}

// insertHigherTermOplogEntry causes a no-op oplog entry insert with a higher term. This affects the
// stable timestamp on magic restore node shutdown. testAuth causes us to create users on the node
// before restore.
for (const insertHigherTermOplogEntry of [false, true]) {
    for (const testAuth of [false, true]) {
        runTest(insertHigherTermOplogEntry, testAuth);
    }
}
