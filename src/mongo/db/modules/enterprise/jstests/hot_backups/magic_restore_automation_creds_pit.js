/*
 * Tests a non-PIT replica set magic restore with the optional automationCredentials field in the
 * restoreConfiguration. This field upserts automation agent roles and users into auth collections.
 *
 * @tags: [
 *     requires_persistence,
 *     requires_wiredtiger
 * ]
 */

import {_copyFileHelper, MagicRestoreUtils} from "jstests/libs/backup_utils.js";

// TODO SERVER-86034: Run on Windows machines once named pipe related failures are resolved.
if (_isWindows()) {
    jsTestLog("Temporarily skipping test for Windows variants. See SERVER-86034.");
    quit();
}

// TODO SERVER-87225: Enable fast count on validate when operations applied during a restore are
// counted correctly.
TestData.skipEnforceFastCountOnValidate = true;

function runTest(updateAutoCreds) {
    jsTestLog("Running PIT magic restore with automation credentials upsert. updateAutoCreds: " +
              updateAutoCreds);

    const sourceCluster = new ReplSetTest({nodes: 1});
    sourceCluster.startSet();
    sourceCluster.initiateWithHighElectionTimeout();

    const sourcePrimary = sourceCluster.getPrimary();
    const dbName = "db";
    const coll = "coll";

    // TODO SERVER-88169: Use systemUuids parameter in restore configuration to create auth
    // collections.
    assert.commandWorked(sourcePrimary.getDB("admin").createCollection("system.roles"));
    assert.commandWorked(sourcePrimary.getDB("admin").createCollection("system.users"));

    const sourceDb = sourcePrimary.getDB(dbName);
    // Insert some data to restore. This data will be reflected in the restored node.
    ['a', 'b', 'c'].forEach(
        key => { assert.commandWorked(sourceDb.getCollection(coll).insert({[key]: 1})); });

    // This timestamp will be used for a snapshot read.
    const snapshotTs = assert.commandWorked(sourcePrimary.adminCommand({replSetGetStatus: 1}))
                           .optimes.lastCommittedOpTime.ts;

    const magicRestoreUtils = new MagicRestoreUtils({
        backupSource: sourcePrimary,
        pipeDir: MongoRunner.dataDir,
    });
    magicRestoreUtils.takeCheckpointAndOpenBackup();

    // These documents will be truncated by magic restore, since they were written after the backup
    // cursor was opened. We will pass these oplog entries to magic restore to perform a PIT
    // restore, so they will be reinserted and reflected in the final state of the data.
    ['e', 'f', 'g', 'h'].forEach(
        key => { assert.commandWorked(sourceDb.getCollection(coll).insert({[key]: 1})); });
    assert.eq(sourceDb.getCollection(coll).find().toArray().length, 7);

    // If the auth credentials exist in the snapshotted data files, the 'createRole' and
    // 'createUser' commands will be converted to update operations. If we're testing the update
    // code path, create the credentials before the backup is taken. We create these preliminary
    // credentials after the backup, so the oplog entries are streamed to the restore node.
    if (updateAutoCreds) {
        sourcePrimary.getDB("admin").createRole({role: 'testRole', privileges: [], roles: []});
        sourcePrimary.getDB("admin").createUser({user: 'testUser', pwd: 'password', roles: []});
    }

    const checkpointTimestamp = magicRestoreUtils.getCheckpointTimestamp();
    let {lastOplogEntryTs, entriesAfterBackup} =
        magicRestoreUtils.getEntriesAfterBackup(sourcePrimary);
    // When testing restore with credentials already inserted, there are three extra oplog entries:
    // the role write, user write, and a write to admin.system.version to update the authSchema.
    assert.eq(entriesAfterBackup.length, updateAutoCreds ? 7 : 4);

    magicRestoreUtils.copyFilesAndCloseBackup();

    let expectedConfig =
        assert.commandWorked(sourcePrimary.adminCommand({replSetGetConfig: 1})).config;
    // The new node will be allocated a new port by the test fixture.
    expectedConfig.members[0].host = getHostName() + ":" + (Number(sourcePrimary.port) + 2);

    let roles = [];
    // If we are testing the update code path, include a new role to be included in the update.
    if (updateAutoCreds) {
        roles.push("readWriteAnyDatabase");
    }
    const autoCreds = {
        createRoleCommands: [{createRole: "testRole", $db: "admin", privileges: [], roles: roles}],
        createUserCommands: [{createUser: "testUser", $db: "admin", pwd: "password", roles: roles}],
    };

    let restoreConfiguration = {
        "nodeType": "replicaSet",
        "replicaSetConfig": expectedConfig,
        "maxCheckpointTs": checkpointTimestamp,
        // Restore to the timestamp of the last oplog entry on the source cluster.
        "pointInTimeTimestamp": lastOplogEntryTs,
        "automationCredentials": autoCreds,
    };
    restoreConfiguration =
        magicRestoreUtils.appendRestoreToHigherTermThanIfNeeded(restoreConfiguration);

    magicRestoreUtils.writeObjsAndRunMagicRestore(restoreConfiguration, entriesAfterBackup);

    // Start a new replica set fixture on the dbpath.
    const destinationCluster = new ReplSetTest({nodes: 1});
    destinationCluster.startSet({dbpath: magicRestoreUtils.getBackupDbPath(), noCleanData: true});

    const destPrimary = destinationCluster.getPrimary();
    const restoredConfig =
        assert.commandWorked(destPrimary.adminCommand({replSetGetConfig: 1})).config;

    magicRestoreUtils.assertConfigIsCorrect(expectedConfig, restoredConfig);
    magicRestoreUtils.assertStableCheckpointIsCorrectAfterRestore(destPrimary);

    magicRestoreUtils.assertOplogCountForNamespace(
        sourcePrimary, dbName + "." + coll, 7, "i" /* op filter*/);

    magicRestoreUtils.assertMinValidIsCorrect(destPrimary);

    // The original node still maintains the history store, so point-in-time reads will succeed.
    let res = sourcePrimary.getDB("db").runCommand(
        {find: "coll", readConcern: {level: "snapshot", atClusterTime: snapshotTs}});
    assert.commandWorked(res);
    assert.eq(res.cursor.firstBatch.length, 3);

    magicRestoreUtils.assertCannotDoSnapshotRead(destPrimary, 7 /* expectedNumDocs */);

    let diff = DataConsistencyChecker.getDiff(
        sourcePrimary.getDB("db").getCollection("coll").find().sort({_id: 1}),
        destPrimary.getDB("db").getCollection("coll").find().sort({_id: 1}));

    assert.eq(diff,
              {docsWithDifferentContents: [], docsMissingOnFirst: [], docsMissingOnSecond: []});

    let expectedRoleDoc =
        {"_id": "admin.testRole", "role": "testRole", "db": "admin", "privileges": [], "roles": []};
    if (updateAutoCreds) {
        expectedRoleDoc.roles.push({"role": "readWriteAnyDatabase", "db": "admin"});
    }
    let testRoleDoc = destPrimary.getDB("admin").system.roles.findOne({"role": "testRole"});
    assert.eq(testRoleDoc, expectedRoleDoc);

    let testUserDoc = destPrimary.getDB("admin").system.users.findOne({"user": "testUser"});
    // The user document contains randomly generated fields such as UUIDs and SCRAM-SHA-1
    // credentials, so we can only assert on certain fields.
    assert.eq(testUserDoc._id, "admin.testUser");
    assert.eq(testUserDoc.user, "testUser");
    assert.eq(testUserDoc.db, "admin");
    let expectedRoles = [];
    if (updateAutoCreds) {
        expectedRoles.push({"role": "readWriteAnyDatabase", "db": "admin"});
    }
    assert.eq(testUserDoc.roles, expectedRoles);

    assert(destPrimary.getDB("admin").auth('testUser', 'password'));
    // The writes to the auth collections should not have been written to the oplog. If we updated
    // automation credentials, the initial inserts should exist in the oplog from the snapshot.
    magicRestoreUtils.assertOplogCountForNamespace(
        destPrimary, "admin.system.roles", updateAutoCreds ? 1 : 0);
    magicRestoreUtils.assertOplogCountForNamespace(
        destPrimary, "admin.system.users", updateAutoCreds ? 1 : 0);

    sourceCluster.stopSet();
    destinationCluster.stopSet();
}

// The create operations in the automationCredentials field are automatically converted to updates
// if the role or user already exists. The 'updateAutoCreds' parameter signals whether we are
// testing the create or update code path in magic restore.
runTest(false /* updateAutoCreds */);
runTest(true /* updateAutoCreds */);
