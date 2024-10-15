/**
 * Tests that file copy based initial sync will survive network errors at various points.  We only
 * check network operations after the backup cursor is open, because, as with logical initial
 * syncer, earlier operations are allowed to be more sensitive to network errors.
 *
 * We do not check the exhaust operations (downloading of files) due to test infrastructure
 * limitations (mongobridge does not support exhaust).  We do have unit tests for these.
 * @tags: [requires_persistence, requires_wiredtiger]
 *
 */
import {configureFailPoint, kDefaultWaitForFailPointTimeout} from "jstests/libs/fail_point_util.js";
import {ReplSetTest} from "jstests/libs/replsettest.js";

TestData.skipEnforceFastCountOnValidate = true;
const testName = TestData.testName;
const rst = new ReplSetTest({
    name: testName,
    nodes: [{
        setParameter: {
            'initialSyncMethod': "fileCopyBased",
        }
    }],
    useBridge: 1
});
rst.startSet();
rst.initiate();

const nRetries = 3;
const primary = rst.getPrimary();
const primaryDb = primary.getDB("test");

// Add some data to be cloned.
assert.commandWorked(primaryDb.test.insert([{a: 1}, {b: 2}, {c: 3}]));

// Ensure there's an up-to-date stable checkpoint.
rst.awaitLastStableRecoveryTimestamp();
assert.commandWorked(primary.adminCommand({fsync: 1}));

jsTestLog("Adding the initial sync destination node to the replica set");
const initialSyncNode = rst.add({
    rsConfig: {priority: 0, votes: 0},
    setParameter: {
        'initialSyncMethod': 'fileCopyBased',
        'failpoint.fCBISHangAfterFileCloning': tojson({mode: 'alwaysOn'}),
        'failpoint.initialSyncBackupFileClonerDisableExhaust': tojson({mode: 'alwaysOn'}),
        'failpoint.fCBISForceExtendBackupCursor': tojson({mode: 'alwaysOn'}),
        'numInitialSyncAttempts': 1,
        'logComponentVerbosity': tojson({replication: {verbosity: 2}, storage: {verbosity: 2}}),
    }
});
rst.reInitiate();
rst.waitForState(initialSyncNode, ReplSetTest.State.STARTUP_2);

// Wait for the initial failpoint
assert.commandWorked(initialSyncNode.adminCommand({
    waitForFailPoint: "fCBISHangAfterFileCloning",
    timesEntered: 1,
    maxTimeMS: kDefaultWaitForFailPointTimeout
}));

jsTestLog("Disconnecting sync source from sync node to test getLastApplied failure handling");
primary.disconnect(initialSyncNode);

// Set a failpoint after getLastApplied fails nRetries times.
let getLastAppliedFailpoint = configureFailPoint(
    initialSyncNode, "fCBISHangAfterAttemptingGetLastApplied", {skip: nRetries - 1});

// Release the initial failpoint
assert.commandWorked(
    initialSyncNode.adminCommand({configureFailPoint: "fCBISHangAfterFileCloning", mode: "off"}));

// Wait for the failures.
getLastAppliedFailpoint.wait();

jsTestLog("Reconnecting sync source from sync node to test getLastApplied failure handling");
primary.reconnect(initialSyncNode);

let hangBeforeExtendBackupCursorFailpoint =
    configureFailPoint(initialSyncNode, "fCBISHangBeforeExtendBackupCursor");
getLastAppliedFailpoint.off();

hangBeforeExtendBackupCursorFailpoint.wait();
jsTestLog("Disconnecting sync source from sync node to test extendedBackupCursor failure handling");
let hangAfterAttemptingExtendBackupCursorFailpoint = configureFailPoint(
    initialSyncNode, "fCBISHangAfterAttemptingExtendBackupCursor", {skip: nRetries - 1});
primary.disconnect(initialSyncNode);
hangBeforeExtendBackupCursorFailpoint.off();

hangAfterAttemptingExtendBackupCursorFailpoint.wait();
// Release the force-extend failpoint so we don't loop forever
assert.commandWorked(initialSyncNode.adminCommand(
    {configureFailPoint: "fCBISForceExtendBackupCursor", mode: "off"}));
jsTestLog("Reconnecting sync source from sync node to test extendedBackupCursor failure handling");
primary.reconnect(initialSyncNode);
hangAfterAttemptingExtendBackupCursorFailpoint.off();

// The initial sync should complete successfully
rst.waitForState(initialSyncNode, ReplSetTest.State.SECONDARY);
rst.stopSet();
