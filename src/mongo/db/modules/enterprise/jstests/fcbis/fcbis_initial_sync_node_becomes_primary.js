/**
 * Tests that a node that has undergone file copy based initial sync can become primary and
 * contribute to the majority of the set.
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

// Ensure there's an up-to-date stable checkpoint.
rst.awaitLastStableRecoveryTimestamp();
assert.commandWorked(primary.adminCommand({fsync: 1}));

jsTestLog("Adding the initial sync destination node to the replica set");
const initialSyncNode = rst.add({setParameter: {'initialSyncMethod': 'fileCopyBased'}});
rst.reInitiate();
rst.waitForState(initialSyncNode, ReplSetTest.State.SECONDARY);
const initialSyncNodeDb = initialSyncNode.getDB("test");

assert.eq(3, initialSyncNodeDb.test.find().itcount());

jsTestLog("Stepping up the initial sync node");
rst.stepUp(initialSyncNode);
jsTestLog("Writing with majority write concern");
assert.commandWorked(
    initialSyncNodeDb.test.insert([{x: 1}, {y: 2}, {x: 3}], {writeConcern: {w: "majority"}}));
// Should be replicated to both nodes.
assert.eq(6, initialSyncNodeDb.test.find().itcount());
assert.eq(6, rst.getSecondaries()[0].getDB("test").test.find().itcount());

rst.stopSet();
})();
