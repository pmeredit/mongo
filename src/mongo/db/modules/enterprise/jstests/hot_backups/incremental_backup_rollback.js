/**
 * Tests that incremental backups remain viable across replication rollback on the backup source.
 *
 * @tags: [
 *   requires_persistence,
 *   requires_replication,
 *   requires_wiredtiger,
 *   requires_mongobridge,
 * ]
 */
import {openBackupCursor} from "jstests/libs/backup_utils.js";
import {RollbackTest} from "jstests/replsets/libs/rollback_test.js";

const dbName = "incremental_backup";
const collName = "rollback";

// These strings are 1MB in length.
let x = 'x'.repeat(1 * 1024 * 1024);
let y = 'y'.repeat(1 * 1024 * 1024);
let z = 'z'.repeat(1 * 1024 * 1024);

const rollbackTest = new RollbackTest();

let CommonOps = (node) => {
    const db = node.getDB(dbName);
    const coll = db.getCollection(collName);

    for (let i = 0; i < 25; i++) {
        assert.commandWorked(coll.insert({x: x, y: y, z: z}));
    }

    assert.commandWorked(db.adminCommand({fsync: 1}));
};

let RollbackOps = (node) => {
    const db = node.getDB(dbName);
    const coll = db.getCollection(collName);

    for (let i = 0; i < 25; i++) {
        assert.commandWorked(coll.insert({x: x, y: y, z: z}));
    }

    assert.commandWorked(db.adminCommand({fsync: 1}));
};

CommonOps(rollbackTest.getPrimary());

const rollbackNode = rollbackTest.transitionToRollbackOperations();

jsTest.log("Taking a full backup for incremental purposes on the rollback node.");
let backupCursor =
    openBackupCursor(rollbackNode.getDB("admin"), {incrementalBackup: true, thisBackupName: "A"});
while (backupCursor.hasNext()) {
    backupCursor.next();
}
backupCursor.close();

RollbackOps(rollbackNode);

jsTest.log("Taking an incremental backup on the previous" +
           " full backup with operations that will be rolled back.");
backupCursor = openBackupCursor(rollbackNode.getDB("admin"),
                                {incrementalBackup: true, thisBackupName: "B", srcBackupName: "A"});
while (backupCursor.hasNext()) {
    backupCursor.next();
}
backupCursor.close();

// Wait for rollback to finish.
rollbackTest.transitionToSyncSourceOperationsBeforeRollback();
rollbackTest.transitionToSyncSourceOperationsDuringRollback();
rollbackTest.transitionToSteadyStateOperations();

// Verify that we can take an incremental backup on "B", which had rolled back operations.
jsTest.log("Taking an incremental backup on the previous one, which had rolled back operations.");
backupCursor = openBackupCursor(rollbackNode.getDB("admin"),
                                {incrementalBackup: true, thisBackupName: "C", srcBackupName: "B"});
while (backupCursor.hasNext()) {
    backupCursor.next();
}
backupCursor.close();

rollbackTest.stop();
