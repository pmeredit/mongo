/**
 * Tests that file copy based initial sync will succeed even if a new checkpoint is taken after
 * opening backup cursor.
 * @tags: [
 *   requires_persistence,
 *   requires_wiredtiger,
 *   multiversion_incompatible
 * ]
 */
import {kDefaultWaitForFailPointTimeout} from "jstests/libs/fail_point_util.js";

TestData.skipEnforceFastCountOnValidate = true;
const testName = "fcbis_succeeds_with_backup_cursor_checkpoint_conflict";
const rst = new ReplSetTest({
    name: testName,
    nodes: [{
        setParameter: {
            'initialSyncMethod': "fileCopyBased",
        }
    }]
});
rst.startSet();
rst.initiate();
const primary = rst.getPrimary();
const primaryDb = primary.getDB("test");

// Add some data to be cloned.
assert.commandWorked(primaryDb.test.insert([{a: 1}, {b: 2}, {c: 3}]));
rst.awaitReplication();

jsTestLog("Adding the initial sync destination node to the replica set");
const initialSyncNode = rst.add({
    rsConfig: {priority: 0, votes: 0},
    setParameter: {
        'initialSyncMethod': 'fileCopyBased',
        'numInitialSyncAttempts': 1,
        'failpoint.backupCursorHangAfterOpen': tojson({mode: 'alwaysOn'}),
    }
});

jsTestLog("Attempting file copy based initial sync");
rst.reInitiate();

assert.commandWorked(initialSyncNode.adminCommand({
    waitForFailPoint: "backupCursorHangAfterOpen",
    timesEntered: 1,
    maxTimeMS: kDefaultWaitForFailPointTimeout
}));

jsTestLog("Backup cursor opened, fsync to invalidate the checkpoint of the backup cursor.");
assert.commandWorked(initialSyncNode.adminCommand({fsync: 1}));

jsTestLog("Turn off the FP, FCBIS should succeed after retrying on checkpoint conflict error.");
assert.adminCommandWorkedAllowingNetworkError(
    initialSyncNode, {configureFailPoint: "backupCursorHangAfterOpen", mode: "off"});

rst.awaitSecondaryNodes();
rst.awaitReplication();
checkLog.containsJson(initialSyncNode, 8191601);

rst.stopSet();
