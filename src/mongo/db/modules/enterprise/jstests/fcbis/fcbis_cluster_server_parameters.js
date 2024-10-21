/**
 * Tests that cluster server parameters are propagated onto new nodes that use file-copy based
 * initial sync.
 *
 * @tags: [requires_fcv_60, requires_persistence, requires_wiredtiger]
 */

import {ReplSetTest} from "jstests/libs/replsettest.js";

TestData.skipEnforceFastCountOnValidate = true;
const testName = TestData.testName;
const rst = new ReplSetTest({
    name: testName,
    nodes: [{
        setParameter: {
            'initialSyncMethod': 'fileCopyBased',
        }
    }]
});
rst.startSet();
rst.initiate();
const primary = rst.getPrimary();

jsTestLog("Setting a cluster parameter on the replica set");
assert.commandWorked(
    primary.adminCommand({setClusterParameter: {testIntClusterParameter: {intData: 5}}}));
rst.awaitReplication();

jsTestLog("Ensuring there is an up-to-date stable checkpoint for FCBIS to copy from.");
rst.awaitLastStableRecoveryTimestamp();
assert.commandWorked(primary.adminCommand({fsync: 1}));

jsTestLog("Adding the initial sync destination node to the replica set.");
const initialSyncNode = rst.add({
    rsConfig: {priority: 0, votes: 0},
    setParameter: {
        'initialSyncMethod': 'fileCopyBased',
        'numInitialSyncAttempts': 1,
    }
});
rst.reInitiate();

// Wait for initial sync to complete.
rst.awaitReplication();

jsTestLog("Checking that the new node has the up-to-date cluster parameter value");
const clusterServerParameters = assert
                                    .commandWorked(initialSyncNode.adminCommand(
                                        {getClusterParameter: "testIntClusterParameter"}))
                                    .clusterParameters;
assert.eq(clusterServerParameters[0].intData, 5);

jsTestLog("Done with test.");
rst.stopSet();
