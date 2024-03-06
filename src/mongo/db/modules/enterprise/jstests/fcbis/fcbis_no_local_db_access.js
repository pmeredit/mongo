/**
 * Tests that access to 'local' is denied during File Copy Based Initial Sync.
 */
(function() {
"use strict";

load("jstests/replsets/rslib.js");
load("jstests/libs/fail_point_util.js");

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
rst.initiateWithHighElectionTimeout();
const primary = rst.getPrimary();
const primaryDb = primary.getDB("test");

const featureEnabled = assert
                           .commandWorked(primaryDb.adminCommand(
                               {getParameter: 1, featureFlagFileCopyBasedInitialSync: 1}))
                           .featureFlagFileCopyBasedInitialSync.value;
if (!featureEnabled) {
    jsTestLog("Skipping test because the file copy based initial sync feature flag is disabled");
    rst.stopSet();
    return;
}

// Add some data to be cloned.
assert.commandWorked(primaryDb.test.insert([{a: 1}, {b: 2}, {c: 3}]));
rst.awaitReplication();

jsTestLog("Adding the initial sync destination node to the replica set");
const initialSyncNode = rst.add({
    rsConfig: {priority: 0, votes: 0},
    setParameter: {
        'initialSyncMethod': 'fileCopyBased',
        'failpoint.fCBISHangAfterFileCloning': tojson({mode: 'alwaysOn'}),
        'logComponentVerbosity': tojson({replication: {verbosity: 1}}),
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
})();
