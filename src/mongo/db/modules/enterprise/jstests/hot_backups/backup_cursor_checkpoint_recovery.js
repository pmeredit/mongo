/**
 * This test makes sure it's possible to recover from a stable checkpoint that was created with an
 * open backup cursor.
 *
 * @tags: [
 *   requires_persistence,
 *   requires_replication,
 *   requires_wiredtiger,
 * ]
 */
(function() {
"use strict";

load("jstests/libs/backup_utils.js");

// We are intentionally crashing the server, the fast count may be inaccurate.
TestData.skipEnforceFastCountOnValidate = true;

const rst = new ReplSetTest({nodes: 1});
rst.startSet();
rst.initiate();

const dbName = "test";
const collName = "recovery";

const primary = rst.getPrimary();
let db = primary.getDB(dbName);

// Opening backup cursors can race with taking a checkpoint, so disable automatic checkpoints.
assert.commandWorked(
    primary.adminCommand({configureFailPoint: "pauseCheckpointThread", mode: "alwaysOn"}));

assert.commandWorked(db.getCollection(collName).insert({}));

const checkpoint = assert.commandWorked(db.adminCommand({fsync: 1}));
const lastStableRes =
    assert.commandWorked(db.adminCommand({replSetTest: 1, getLastStableRecoveryTimestamp: 1}));

assert(timestampCmp(checkpoint["$clusterTime"].clusterTime,
                    lastStableRes.lastStableRecoveryTimestamp) <= 0,
       "Checkpoint didn't increase last stable recovery timestamp");

const cursor = db.aggregate([{$backupCursor: {}}]);

assert.commandWorked(db.getCollection(collName).insert({}));

const backupCheckpoint = assert.commandWorked(db.adminCommand({fsync: 1}));
const backupLastStableRes =
    assert.commandWorked(db.adminCommand({replSetTest: 1, getLastStableRecoveryTimestamp: 1}));

assert(timestampCmp(checkpoint["$clusterTime"].clusterTime,
                    backupCheckpoint["$clusterTime"].clusterTime) <= 0,
       "Checkpoint didn't increase cluster time");
assert(timestampCmp(backupCheckpoint["$clusterTime"].clusterTime,
                    backupLastStableRes.lastStableRecoveryTimestamp) <= 0,
       "Checkpoint didn't increase last stable recovery timestamp");

rst.stop(0, 9, {allowedExitCode: MongoRunner.EXIT_SIGKILL}, {forRestart: true});
rst.restart(0);
rst.awaitReplication();

db = rst.getPrimary().getDB(dbName);

const recoveryLastStableRes =
    assert.commandWorked(db.adminCommand({replSetTest: 1, getLastStableRecoveryTimestamp: 1}));

assert(timestampCmp(backupCheckpoint["$clusterTime"].clusterTime,
                    recoveryLastStableRes.lastStableRecoveryTimestamp) <= 0,
       "We didn't recover from the checkpoint taken during backup");

rst.stopSet();
})();
