/*
 * Tests a non-PIT replica set magic restore with the optional automationCredentials field in the
 * restoreConfiguration. This field upserts automation agent roles and users into auth collections.
 *
 * @tags: [
 *     requires_persistence,
 *     requires_wiredtiger,
 *     incompatible_with_windows_tls
 * ]
 */

import {MagicRestoreTest} from "jstests/libs/magic_restore_test.js";
import {ReplSetTest} from "jstests/libs/replsettest.js";

function runTest(updateAutoCreds) {
    jsTestLog(
        "Running non-PIT magic restore with automation credentials upsert. updateAutoCreds: " +
        updateAutoCreds);
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
    const expectedDocs = db.getCollection(coll).find().toArray();

    // If the auth credentials exist in the snapshotted data files, the 'createRole' and
    // 'createUser' commands will be converted to upserts. If we're testing the update code path,
    // create the credentials before the backup is taken.
    let rolesCollUuid = UUID();
    let userCollUuid = UUID();
    if (updateAutoCreds) {
        primary.getDB("admin").createRole({role: 'testRole', privileges: [], roles: []});
        primary.getDB("admin").createUser({user: 'testUser', pwd: 'password', roles: []});
        // The collections are implicitly created outside of restore, so we should overwrite the
        // UUIDs with their new randomly generated values. We'll check the UUIDs after a restore to
        // ensure that systemUuids does not overwrite existing collections.
        rolesCollUuid =
            primary.getDB("admin").getCollectionInfos({name: "system.roles"})[0].info.uuid;
        userCollUuid =
            primary.getDB("admin").getCollectionInfos({name: "system.users"})[0].info.uuid;
    }

    const magicRestoreTest = new MagicRestoreTest({
        rst: rst,
        pipeDir: MongoRunner.dataDir,
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

    let expectedConfig = magicRestoreTest.getExpectedConfig();
    // The new node will be allocated a new port by the test fixture.
    expectedConfig.members[0].host = getHostName() + ":" + (Number(primary.port) + 2);
    rst.stopSet(null /* signal */, false /* forRestart */, {noCleanData: true});

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
        "maxCheckpointTs": magicRestoreTest.getCheckpointTimestamp(),
        "automationCredentials": autoCreds,
        "systemUuids": [
            {"ns": "admin.system.roles", "uuid": rolesCollUuid},
            {"ns": "admin.system.users", "uuid": userCollUuid}
        ],
    };
    restoreConfiguration =
        magicRestoreTest.appendRestoreToHigherTermThanIfNeeded(restoreConfiguration);

    magicRestoreTest.writeObjsAndRunMagicRestore(
        restoreConfiguration, [], {"replSet": jsTestName()});

    // Restart the destination replica set.
    rst = new ReplSetTest({nodes: 1});
    rst.startSet({dbpath: magicRestoreTest.getBackupDbPath(), noCleanData: true});

    primary = rst.getPrimary();
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
        rolesCollUuid: rolesCollUuid,
        userCollUuid: userCollUuid,
    });

    let expectedRoleDoc =
        {"_id": "admin.testRole", "role": "testRole", "db": "admin", "privileges": [], "roles": []};
    if (updateAutoCreds) {
        expectedRoleDoc.roles.push({"role": "readWriteAnyDatabase", "db": "admin"});
    }
    let testRoleDoc = primary.getDB("admin").system.roles.findOne({"role": "testRole"});
    assert.eq(testRoleDoc, expectedRoleDoc);

    let testUserDoc = primary.getDB("admin").system.users.findOne({"user": "testUser"});
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

    assert(primary.getDB("admin").auth('testUser', 'password'));
    // The writes to the auth collections should not have been written to the oplog. If we updated
    // automation credentials, the initial inserts should exist in the oplog from the snapshot.
    magicRestoreTest.assertOplogCountForNamespace(
        primary, {ns: "admin.system.roles"}, updateAutoCreds ? 1 : 0);
    magicRestoreTest.assertOplogCountForNamespace(
        primary, {ns: "admin.system.users"}, updateAutoCreds ? 1 : 0);
    rst.stopSet();
}

// The create operations in the automationCredentials field are automatically converted to updates
// if the role or user already exists. The 'updateAutoCreds' parameter signals whether we are
// testing the create or update code path in magic restore.
runTest(false /* updateAutoCreds */);
runTest(true /* updateAutoCreds */);
