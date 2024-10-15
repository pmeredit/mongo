/**
 * Runs watchdog during FCBIS, and pauses FCBIS after deleting storage files. Watchdog
 * should skip checks during this time, and resume checks after FCBIS switches storage to correct
 * location.
 *
 * @tags: [requires_fcv_61, requires_persistence, requires_wiredtiger]
 */
import {kDefaultWaitForFailPointTimeout} from "jstests/libs/fail_point_util.js";
import {ReplSetTest} from "jstests/libs/replsettest.js";

// Get the current watchdog check generation
function getWatchDogCheckGeneration(node) {
    const result = node.adminCommand({"serverStatus": 1});
    assert.commandWorked(result);
    return result.watchdog.checkGeneration;
}

clearRawMongoProgramOutput();
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

// Ensure there's an up-to-date stable checkpoint for FCBIS to copy.
rst.awaitLastStableRecoveryTimestamp();
assert.commandWorked(primary.adminCommand({fsync: 1}));

jsTestLog("Adding the initial sync destination node to the replica set");
const initialSyncNode = rst.add({
    rsConfig: {priority: 0, votes: 0},
    setParameter: {
        'initialSyncMethod': 'fileCopyBased',
        'numInitialSyncAttempts': 1,
        'logComponentVerbosity': tojson({
            replication: {verbosity: 1, initialSync: 2},
            control: 1,
        }),
        'failpoint.fCBISHangAfterDeletingOldStorageFiles': tojson({mode: 'alwaysOn'}),
        'watchdogPeriodSeconds': 60
    }
});
// Set watchdogPeriodSeconds to a lower value (just higher than the checkPeriod of 10 seconds)
// so that this test runs faster.
assert.commandWorked(initialSyncNode.adminCommand({setParameter: 1, watchdogPeriodSeconds: 11}));
rst.reInitiate();

jsTestLog("Waiting to hit fCBISHangAfterDeletingOldStorageFiles failpoint");
assert.soonNoExcept(() => {
    let res = initialSyncNode.adminCommand({
        waitForFailPoint: "fCBISHangAfterDeletingOldStorageFiles",
        timesEntered: 1,
        maxTimeMS: kDefaultWaitForFailPointTimeout
    });
    return res.ok;
});

jsTestLog("Make sure watchdog has run but is skipping checks");
assert.soon(() => { return getWatchDogCheckGeneration(initialSyncNode) > 0; },
            "Watchdog should be running");

assert.soon(function() {
    return rawMongoProgramOutput().match(/\"id\":8350802.*Watchdog skipping running check/);
}, "Watchdog should skip checks");

jsTestLog("Turning fCBISHangAfterDeletingOldStorageFiles failpoint off");
assert.commandWorked(initialSyncNode.adminCommand(
    {configureFailPoint: "fCBISHangAfterDeletingOldStorageFiles", mode: "off"}));

jsTestLog("Watchdog should resume running checks");
assert.soon(function() {
    return rawMongoProgramOutput().match(/\"id\":8350803.*Watchdog test/);
}, "Watchdog should resume running checks after FCBIS is done but did not");

// We use this assert instead of waitForSecondary because we want errors due to the node
// crashing to fail, not timeout.
assert.soon(() => initialSyncNode.adminCommand({hello: 1}).secondary);
rst.stopSet();
