/*
 * Tests a PIT replica set magic restore with the optional automationCredentials field in the
 * restoreConfiguration. This field upserts automation agent roles and users into auth collections.
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

function runTest(updateAutoCreds) {
    jsTestLog("Running PIT magic restore with automation credentials upsert. updateAutoCreds: " +
              updateAutoCreds);

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

    // This timestamp will be used for a snapshot read.
    const snapshotTs = assert.commandWorked(sourcePrimary.adminCommand({replSetGetStatus: 1}))
                           .optimes.lastCommittedOpTime.ts;

    const magicRestoreUtils = new MagicRestoreUtils({
        rst: sourceCluster,
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
    let rolesCollUuid = UUID();
    let userCollUuid = UUID();
    if (updateAutoCreds) {
        sourcePrimary.getDB("admin").createRole({role: 'testRole', privileges: [], roles: []});
        sourcePrimary.getDB("admin").createUser({user: 'testUser', pwd: 'password', roles: []});
        // The collections are implicitly created outside of restore, so we should overwrite the
        // UUIDs with their new randomly generated values. We'll check the UUIDs after a restore to
        // ensure that systemUuids does not overwrite existing collections.
        rolesCollUuid =
            sourcePrimary.getDB("admin").getCollectionInfos({name: "system.roles"})[0].info.uuid;
        userCollUuid =
            sourcePrimary.getDB("admin").getCollectionInfos({name: "system.users"})[0].info.uuid;
    }

    const checkpointTimestamp = magicRestoreUtils.getCheckpointTimestamp();
    let {lastOplogEntryTs, entriesAfterBackup} =
        magicRestoreUtils.getEntriesAfterBackup(sourcePrimary);
    // When testing restore with credentials already inserted, there are six extra oplog entries.
    // The first four are the role and user auth collection and index creation entries. The
    // following two are the role write and user write.
    // These oplog entries are generated on the source before restore begins.
    assert.eq(entriesAfterBackup.length, updateAutoCreds ? 10 : 4);

    magicRestoreUtils.copyFilesAndCloseBackup();

    let expectedConfig = magicRestoreUtils.getExpectedConfig();
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
        "systemUuids": [
            {"ns": "admin.system.roles", "uuid": rolesCollUuid},
            {"ns": "admin.system.users", "uuid": userCollUuid}
        ],
    };
    restoreConfiguration =
        magicRestoreUtils.appendRestoreToHigherTermThanIfNeeded(restoreConfiguration);

    magicRestoreUtils.writeObjsAndRunMagicRestore(
        restoreConfiguration, entriesAfterBackup, {"replSet": jsTestName()});

    // Start a new replica set fixture on the dbpath.
    const destinationCluster = new ReplSetTest({nodes: 1});
    destinationCluster.startSet({dbpath: magicRestoreUtils.getBackupDbPath(), noCleanData: true});

    const destPrimary = destinationCluster.getPrimary();

    magicRestoreUtils.postRestoreChecks({
        node: destPrimary,
        dbName: dbName,
        collName: coll,
        expectedOplogCountForNs: 7,
        opFilter: "i",
        expectedNumDocsSnapshot: 7,
        rolesCollUuid: rolesCollUuid,
        userCollUuid: userCollUuid,
    });

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
        destPrimary, {ns: "admin.system.roles"}, updateAutoCreds ? 1 : 0);
    magicRestoreUtils.assertOplogCountForNamespace(
        destPrimary, {ns: "admin.system.users"}, updateAutoCreds ? 1 : 0);

    sourceCluster.stopSet();
    destinationCluster.stopSet();
}

// The create operations in the automationCredentials field are automatically converted to updates
// if the role or user already exists. The 'updateAutoCreds' parameter signals whether we are
// testing the create or update code path in magic restore.
runTest(false /* updateAutoCreds */);
runTest(true /* updateAutoCreds */);
