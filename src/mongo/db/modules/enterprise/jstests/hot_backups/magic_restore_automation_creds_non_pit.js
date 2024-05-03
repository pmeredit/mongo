/*
 * Tests a non-PIT replica set magic restore with the optional automationCredentials field in the
 * restoreConfiguration. This field upserts automation agent roles and users into auth collections.
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

function runTest(updateAutoCreds) {
    jsTestLog(
        "Running non-PIT magic restore with automation credentials upsert. updateAutoCreds: " +
        updateAutoCreds);
    let rst = new ReplSetTest({nodes: 1});
    rst.startSet();
    rst.initiateWithHighElectionTimeout();

    let primary = rst.getPrimary();
    const dbName = "db";
    const coll = "coll";

    // TODO SERVER-88169: Use systemUuids parameter in restore configuration to create auth
    // collections.
    assert.commandWorked(primary.getDB("admin").createCollection("system.roles"));
    assert.commandWorked(primary.getDB("admin").createCollection("system.users"));

    const db = primary.getDB(dbName);
    // Insert some data to restore. This data will be reflected in the restored node.
    ['a', 'b', 'c'].forEach(
        key => { assert.commandWorked(db.getCollection(coll).insert({[key]: 1})); });
    const expectedDocs = db.getCollection(coll).find().toArray();

    // If the auth credentials exist in the snapshotted data files, the 'createRole' and
    // 'createUser' commands will be converted to upserts. If we're testing the update code path,
    // create the credentials before the backup is taken.
    if (updateAutoCreds) {
        primary.getDB("admin").createRole({role: 'testRole', privileges: [], roles: []});
        primary.getDB("admin").createUser({user: 'testUser', pwd: 'password', roles: []});
    }

    const magicRestoreUtils = new MagicRestoreUtils({
        backupSource: primary,
        pipeDir: MongoRunner.dataDir,
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
        "maxCheckpointTs": magicRestoreUtils.getCheckpointTimestamp(),
        "automationCredentials": autoCreds,

    };
    restoreConfiguration =
        magicRestoreUtils.appendRestoreToHigherTermThanIfNeeded(restoreConfiguration);

    magicRestoreUtils.writeObjsAndRunMagicRestore(restoreConfiguration);

    // Restart the destination replica set.
    rst = new ReplSetTest({nodes: 1});
    rst.startSet({dbpath: magicRestoreUtils.getBackupDbPath(), noCleanData: true});

    primary = rst.getPrimary();
    const restoredConfig = assert.commandWorked(primary.adminCommand({replSetGetConfig: 1})).config;

    magicRestoreUtils.assertConfigIsCorrect(expectedConfig, restoredConfig);

    const restoredDocs = primary.getDB(dbName).getCollection(coll).find().toArray();
    // The later 3 writes were truncated during magic restore.
    assert.eq(restoredDocs.length, 3);
    assert.eq(restoredDocs, expectedDocs);

    magicRestoreUtils.assertOplogCountForNamespace(primary, dbName + "." + coll, 3, "i");
    magicRestoreUtils.assertMinValidIsCorrect(primary);
    magicRestoreUtils.assertStableCheckpointIsCorrectAfterRestore(primary);
    magicRestoreUtils.assertCannotDoSnapshotRead(primary, 3 /* expectedNumDocs */);

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
    magicRestoreUtils.assertOplogCountForNamespace(
        primary, "admin.system.roles", updateAutoCreds ? 1 : 0);
    magicRestoreUtils.assertOplogCountForNamespace(
        primary, "admin.system.users", updateAutoCreds ? 1 : 0);
    rst.stopSet();
}

// The create operations in the automationCredentials field are automatically converted to updates
// if the role or user already exists. The 'updateAutoCreds' parameter signals whether we are
// testing the create or update code path in magic restore.
runTest(false /* updateAutoCreds */);
runTest(true /* updateAutoCreds */);
