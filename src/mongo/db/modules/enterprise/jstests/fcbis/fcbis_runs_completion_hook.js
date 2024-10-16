/**
 * Tests that we will run the appropriate hook after FCBIS completes.
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
rst.initiateWithHighElectionTimeout();
const primary = rst.getPrimary();
const primaryDb = primary.getDB("test");

jsTestLog("Adding data to the replica set");
assert.commandWorked(primaryDb.test.insert([{a: 1}, {b: 2}, {c: 3}]));
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
        'logComponentVerbosity': tojson({sharding: {verbosity: 2}}),
    }
});
rst.reInitiate();

// We use this assert instead of waitForSecondary because we want errors due to the node
// crashing to fail, not timeout.
assert.soon(() => initialSyncNode.adminCommand({hello: 1}).secondary);

jsTestLog("Checking for message indicating sharding hook ran.");
checkLog.containsJson(initialSyncNode, 6351912);

jsTestLog("Done with test.");
rst.stopSet();
