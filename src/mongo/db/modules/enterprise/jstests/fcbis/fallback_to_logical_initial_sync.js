/**
 * Tests that file copy based initial sync will fallback to logical initial sync if we cannot find
 * a valid sync source within numInitialSyncConnectAttempts.
 * @tags: [requires_persistence, requires_wiredtiger]
 */

import {ReplSetTest} from "jstests/libs/replsettest.js";

TestData.skipEnforceFastCountOnValidate = true;
const testName = "fallback_to_logical_initial_sync";
const rst = new ReplSetTest({
    name: testName,
    nodes: [{
        directoryperdb: "", /* set directoryperdb so the values won't match for FCBIS and we will
                               need to fallback to logical */
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
        'failpoint.initialSyncHangBeforeCopyingDatabases': tojson({mode: 'alwaysOn'}),
        'numInitialSyncConnectAttempts': 2,
        'numInitialSyncAttempts': 1,
        'logComponentVerbosity': tojson({replication: {verbosity: 1, initialSync: 2}}),
    }
});
rst.reInitiate();
rst.waitForState(initialSyncNode, ReplSetTest.State.STARTUP_2);
let initialSyncNodeDb = initialSyncNode.getDB("test");
assert.commandWorked(initialSyncNodeDb.adminCommand(
    {configureFailPoint: "initialSyncHangBeforeCopyingDatabases", mode: "off"}));

assert.soon(() => rawMongoProgramOutput("5780600").match('Falling back to logical initial sync'),
            'FCBIS should fall back to logical initial sync',
            ReplSetTest.kDefaultTimeoutMS);

jsTestLog("Logical initial sync should succeed");
rst.awaitSecondaryNodes(null, [initialSyncNode]);
checkLog.containsJson(initialSyncNode, 4853000);
rst.awaitReplication();
rst.awaitSecondaryNodes();

rst.checkOplogs("test");
rst.checkReplicatedDataHashes("test");

rst.stopSet();
