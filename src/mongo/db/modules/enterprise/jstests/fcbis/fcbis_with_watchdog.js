/**
 * Tests that the storage watchdog does not shut down the node if its action is interrupted
 * by FCBIS swapping out storage.
 *
 * @tags: [requires_fcv_61, requires_persistence, requires_wiredtiger]
 */

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
        'watchdogPeriodSeconds': 60
    }
});
rst.reInitiate();
// We use this assert instead of waitForSecondary because we want errors due to the node
// crashing to fail, not timeout.
assert.soon(() => initialSyncNode.adminCommand({hello: 1}).secondary);
rst.stopSet();
