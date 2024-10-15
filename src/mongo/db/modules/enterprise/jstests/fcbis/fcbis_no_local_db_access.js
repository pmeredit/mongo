/**
 * Tests that access to 'local' is denied during File Copy Based Initial Sync.
 * @tags: [requires_persistence, requires_wiredtiger]
 */
import {kDefaultWaitForFailPointTimeout} from "jstests/libs/fail_point_util.js";
import {ReplSetTest} from "jstests/libs/replsettest.js";

TestData.skipEnforceFastCountOnValidate = true;
const testName = TestData.testName;
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
        'failpoint.fCBISHangAfterFileCloning': tojson({mode: 'alwaysOn'}),
        'logComponentVerbosity': tojson({replication: {verbosity: 1, initialSync: 2}}),
    }
});
rst.reInitiate();
rst.waitForState(initialSyncNode, ReplSetTest.State.STARTUP_2);
let initialSyncNodeLocalDb = initialSyncNode.getDB("local");
assert.commandWorked(initialSyncNode.adminCommand({
    waitForFailPoint: "fCBISHangAfterFileCloning",
    timesEntered: 1,
    maxTimeMS: kDefaultWaitForFailPointTimeout
}));
jsTestLog("Trying to read a local collection expected to exist");
assert.commandFailedWithCode(initialSyncNodeLocalDb.runCommand({find: "startup_log"}),
                             ErrorCodes.NotPrimaryOrSecondary);
jsTestLog("Trying to read a local collection expected to not exist");
assert.commandFailedWithCode(initialSyncNodeLocalDb.runCommand({find: "does_not_exist"}),
                             ErrorCodes.NotPrimaryOrSecondary);
assert.commandWorked(
    initialSyncNode.adminCommand({configureFailPoint: "fCBISHangAfterFileCloning", mode: "off"}));
rst.waitForState(initialSyncNode, ReplSetTest.State.SECONDARY);
jsTestLog(
    "Ensuring we can read a local collection expected to exist after initial sync completes.");
let res = assert.commandWorked(initialSyncNodeLocalDb.runCommand({find: "startup_log"}));
assert.neq(res.cursor.firstBatch.length, 0);
jsTestLog(
    "Ensuring we can read a local collection expected to not exist after initial sync completes.");
res = assert.commandWorked(initialSyncNodeLocalDb.runCommand({find: "does_not_exist"}));
assert.docEq(res.cursor.firstBatch, []);
rst.stopSet();
