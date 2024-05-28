/*
 * Tests that using the systemUuids parameter in the restore configuration ensures that created
 * replicated collections have the same UUID, by checking the UUID across two restored nodes.
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

jsTestLog("Running non-PIT magic restore on a two-node replica set, testing that systemUuids " +
          "correctly creates replicated collections");
// With 2 nodes, it can happen that both run for election at the same time, vote for themselves,
// in which case the subsequent successful election will be at a higher term, and
// assertConfigIsCorrect will mismatch.
let rst = new ReplSetTest({nodes: [{}, {rsConfig: {priority: 0}}]});
let nodes = rst.startSet();
rst.initiate();

let primary = rst.getPrimary();
const dbName = "db";
const coll = "coll";

const db = primary.getDB(dbName);
// Insert some data to restore. This data will be reflected in the restored node.
['a', 'b', 'c'].forEach(
    key => { assert.commandWorked(db.getCollection(coll).insert({[key]: 1})); });

const magicRestoreUtils = [];
const expectedConfigs = [];
const ports = [];
nodes.forEach((node, idx) => {
    magicRestoreUtils.push(new MagicRestoreUtils(
        {backupSource: node, pipeDir: MongoRunner.dataDir, backupDbPathSuffix: `_${idx}`}));
    magicRestoreUtils[idx].takeCheckpointAndOpenBackup();
    expectedConfigs.push(assert.commandWorked(primary.adminCommand({replSetGetConfig: 1})).config);
    ports.push(node.port);
});

['e', 'f', 'g'].forEach(
    key => { assert.commandWorked(db.getCollection(coll).insert({[key]: 1})); });

nodes.forEach((_, idx) => magicRestoreUtils[idx].copyFilesAndCloseBackup());
rst.stopSet(null /* signal */, false /* forRestart */, {noCleanData: true});

const rolesCollUuid = UUID();
const userCollUuid = UUID();

nodes.forEach((_, idx) => {
    let restoreConfiguration = {
        "nodeType": "replicaSet",
        "replicaSetConfig": expectedConfigs[idx],
        "maxCheckpointTs": magicRestoreUtils[idx].getCheckpointTimestamp(),
        "systemUuids": [
            {"ns": "admin.system.roles", "uuid": rolesCollUuid},
            {"ns": "admin.system.users", "uuid": userCollUuid}
        ],
    };
    restoreConfiguration =
        magicRestoreUtils[idx].appendRestoreToHigherTermThanIfNeeded(restoreConfiguration);

    magicRestoreUtils[idx].writeObjsAndRunMagicRestore(
        restoreConfiguration, [], {"replSet": jsTestName()});
});

// Restart the destination replica set.
rst = new ReplSetTest({
    nodes: [
        {dbpath: magicRestoreUtils[0].getBackupDbPath(), port: ports[0]},
        {dbpath: magicRestoreUtils[1].getBackupDbPath(), port: ports[1]}
    ]
});
nodes = rst.startSet(
    {dbpath: magicRestoreUtils[0].getBackupDbPath().slice(0, -1) + "$node", noCleanData: true});
rst.awaitNodesAgreeOnPrimary();
nodes.forEach((node, idx) => {
    jsTestLog(`Verifying node ${idx}`);
    node.getDB(dbName).getMongo().setSecondaryOk();
    const restoredConfig = assert.commandWorked(node.adminCommand({replSetGetConfig: 1})).config;
    magicRestoreUtils[idx].assertConfigIsCorrect(expectedConfigs[idx], restoredConfig);
    magicRestoreUtils[idx].assertOplogCountForNamespace(node, dbName + "." + coll, 3, "i");
    magicRestoreUtils[idx].assertMinValidIsCorrect(node);
    magicRestoreUtils[idx].assertStableCheckpointIsCorrectAfterRestore(node);
    magicRestoreUtils[idx].assertCannotDoSnapshotRead(node, 3 /* expectedNumDocs */);
    assert.eq(rolesCollUuid, magicRestoreUtils[idx].getCollUuid(node, "admin", "system.roles"));
    assert.eq(userCollUuid, magicRestoreUtils[idx].getCollUuid(node, "admin", "system.users"));
});

rst.stopSet();
