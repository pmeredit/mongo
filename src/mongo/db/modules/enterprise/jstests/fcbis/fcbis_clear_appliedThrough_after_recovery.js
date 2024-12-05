/**
 * Tests that appliedThrough is cleared after FCBIS.
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

// Ensure there's an up-to-date stable checkpoint.
rst.awaitLastStableRecoveryTimestamp();
assert.commandWorked(primary.adminCommand({fsync: 1}));

assert.commandWorked(
    primary.adminCommand({configureFailPoint: "disableSnapshotting", mode: "alwaysOn"}));
jsTestLog("Doing more writes after stopping stable checkpoints");
assert.commandWorked(primaryDb.runCommand(
    {insert: "test", documents: [{a: 4}, {a: 5}, {a: 6}], writeConcern: {w: 1}}));

jsTestLog("Adding the initial sync destination node to the replica set");
const initialSyncNode = rst.add({
    rsConfig: {priority: 0, votes: 0},
    setParameter: {
        'initialSyncMethod': 'fileCopyBased',
        'failpoint.fCBISForceExtendBackupCursor': tojson({mode: {times: 1}}),
        'numInitialSyncAttempts': 1,
        'logComponentVerbosity': tojson({replication: {verbosity: 1, initialSync: 2}})
    }
});
rst.reInitiate();

assert.commandWorked(initialSyncNode.adminCommand({
    waitForFailPoint: 'fCBISForceExtendBackupCursor',
    timesEntered: 1,
    maxTimeMS: kDefaultWaitForFailPointTimeout
}));
assert.commandWorked(
    primary.adminCommand({configureFailPoint: "disableSnapshotting", mode: "off"}));
// Do more writes to trigger a new stable checkpoint on the primary.
assert.commandWorked(primaryDb.test.insert([{a: 7}, {b: 8}, {c: 9}]));

rst.awaitSecondaryNodes(null, [initialSyncNode]);

const minValid = initialSyncNode.getCollection('local.replset.minvalid').findOne();
assert(!minValid.hasOwnProperty('begin'), tojson(minValid));

rst.stopSet();
