/**
 * Tests that file copy based initial sync will fail if backup cursor is already open on the chosen
 * sync source.
 * @tags: [requires_persistence, requires_wiredtiger]
 */
import {openBackupCursor} from "jstests/libs/backup_utils.js";
import {kDefaultWaitForFailPointTimeout} from "jstests/libs/fail_point_util.js";
import {ReplSetTest} from "jstests/libs/replsettest.js";

TestData.skipEnforceFastCountOnValidate = true;
const testName = "fcbis_fails_if_backup_cursor_is_already_open_on_sync_source";
const rst = new ReplSetTest({
    name: testName,
    nodes: 1,
});
rst.startSet();
rst.initiate();
const primary = rst.getPrimary();
const primaryDb = primary.getDB("test");

// Add some data to be cloned.
assert.commandWorked(primaryDb.test.insert([{a: 1}, {b: 2}, {c: 3}]));

openBackupCursor(primaryDb);

jsTestLog("Adding the initial sync destination node to the replica set");
const initialSyncNode = rst.add({
    rsConfig: {priority: 0, votes: 0},
    setParameter: {
        'initialSyncMethod': 'fileCopyBased',
        'numInitialSyncConnectAttempts': 2,
        'numInitialSyncAttempts': 1,
        'logComponentVerbosity': tojson({replication: {verbosity: 1, initialSync: 2}}),
        'failpoint.fCBISHangBeforeFinish': tojson({mode: 'alwaysOn'}),
    }
});

jsTestLog("Attempting file copy based initial sync");
rst.reInitiate();

assert.commandWorked(initialSyncNode.adminCommand({
    waitForFailPoint: "fCBISHangBeforeFinish",
    timesEntered: 1,
    maxTimeMS: kDefaultWaitForFailPointTimeout
}));

// Check that file copy based initial sync could not open the backup cursor on the sync source.
checkLog.containsJson(initialSyncNode, 5973000);

assert.adminCommandWorkedAllowingNetworkError(
    initialSyncNode, {configureFailPoint: "fCBISHangBeforeFinish", mode: "off"});

jsTestLog("File copy based initial sync should fail");
assert.eq(MongoRunner.EXIT_ABRUPT, waitMongoProgram(initialSyncNode.port));

// We skip validation and dbhashes because the initial sync failed so the initial sync node is
// invalid and unreachable.
TestData.skipCheckDBHashes = true;
rst.stopSet(null, null, {skipValidation: true});
