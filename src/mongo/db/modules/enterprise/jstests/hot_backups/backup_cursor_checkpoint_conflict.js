/**
 * Tests that opening a backup cursor fails with BackupCursorOpenConflictWithCheckpoint if it races
 * with a checkpoint being taken.
 *
 * @tags: [
 *   requires_persistence,
 * ]
 */

import {getBackupCursorDB} from "jstests/libs/backup_utils.js";
import {configureFailPoint} from "jstests/libs/fail_point_util.js";
import {funWithArgs} from "jstests/libs/parallel_shell_helpers.js";

const replTest = new ReplSetTest({nodes: 1});
replTest.startSet();
replTest.initiate();

const primary = replTest.getPrimary();
const db = primary.getDB(jsTestName());
const coll = db.coll;

assert.commandWorked(coll.insert({_id: 0}));

const fp = configureFailPoint(primary, "backupCursorHangAfterOpen");

const backupCursorDB = getBackupCursorDB(primary);
const awaitOpenBackupCursor =
    startParallelShell(funWithArgs(function(dbName) {
                           assert.commandFailedWithCode(
                               db.getSiblingDB(dbName).runCommand(
                                   {aggregate: 1, pipeline: [{$backupCursor: {}}], cursor: {}}),
                               ErrorCodes.BackupCursorOpenConflictWithCheckpoint);
                       }, backupCursorDB.getName()), primary.port);

fp.wait();
assert.commandWorked(db.adminCommand({fsync: 1}));
fp.off();

awaitOpenBackupCursor();

replTest.stopSet();
