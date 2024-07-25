/*
 * Tests that using the systemUuids parameter in the restore configuration ensures that created
 * replicated collections have the same UUID, by checking the UUID across two restored nodes.
 *
 * @tags: [
 *     requires_persistence,
 *     requires_wiredtiger
 * ]
 */

import {MagicRestoreUtils} from "jstests/libs/magic_restore_test.js";
import {ReplSetTest} from "jstests/libs/replsettest.js";
import {isConfigCommitted} from "jstests/replsets/rslib.js";

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
rst.initiate(null, null, {initiateWithDefaultElectionTimeout: true});

let primary = rst.getPrimary();
const dbName = "db";
const coll = "coll";

const db = primary.getDB(dbName);
// Insert some data to restore. This data will be reflected in the restored node.
['a', 'b', 'c'].forEach(
    key => { assert.commandWorked(db.getCollection(coll).insert({[key]: 1})); });

const magicRestoreUtil = new MagicRestoreUtils({rst: rst, pipeDir: MongoRunner.dataDir});
magicRestoreUtil.takeCheckpointAndOpenBackup();

['e', 'f', 'g'].forEach(
    key => { assert.commandWorked(db.getCollection(coll).insert({[key]: 1})); });

magicRestoreUtil.copyFilesAndCloseBackup();
rst.stopSet(null /* signal */, false /* forRestart */, {noCleanData: true});

const rolesCollUuid = UUID();
const userCollUuid = UUID();

let expectedConfig = magicRestoreUtil.getExpectedConfig();
let restoreConfiguration = {
    "nodeType": "replicaSet",
    "replicaSetConfig": expectedConfig,
    "maxCheckpointTs": magicRestoreUtil.getCheckpointTimestamp(),
    "systemUuids": [
        {"ns": "admin.system.roles", "uuid": rolesCollUuid},
        {"ns": "admin.system.users", "uuid": userCollUuid}
    ],
};
restoreConfiguration = magicRestoreUtil.appendRestoreToHigherTermThanIfNeeded(restoreConfiguration);
magicRestoreUtil.writeObjsAndRunMagicRestore(restoreConfiguration, [], {"replSet": jsTestName()});

// Restart the destination replica set.
rst = new ReplSetTest({nodes: [{port: rst.ports[0]}, {port: rst.ports[1]}]});
nodes = rst.startSet({dbpath: magicRestoreUtil.getBackupDbPath(), noCleanData: true});
rst.awaitNodesAgreeOnPrimary();
// Make sure that all nodes have installed the config before moving on.
primary = rst.getPrimary();
rst.waitForConfigReplication(primary);
assert.soonNoExcept(() => isConfigCommitted(primary));

nodes.forEach((node, idx) => {
    jsTestLog(`Verifying node ${idx}`);
    node.getDB(dbName).getMongo().setSecondaryOk();
    magicRestoreUtil.postRestoreChecks({
        node: node,
        dbName: dbName,
        collName: coll,
        expectedOplogCountForNs: 3,
        opFilter: "i",
        expectedNumDocsSnapshot: 3,
        rolesCollUuid: rolesCollUuid,
        userCollUuid: userCollUuid,
    });
});

rst.stopSet();
