/**
 * Tests that calling 'replSetSyncFrom' on an file-copy based initial syncing node will cancel the
 * current syncing attempt and cause it to retry against the newly designated sync source.
 * @tags: [requires_persistence, requires_wiredtiger]
 */

import {ReplSetTest} from "jstests/libs/replsettest.js";

TestData.skipEnforceFastCountOnValidate = true;
const rst = new ReplSetTest({
    nodes: [{}, {rsConfig: {priority: 0}}],
});
rst.startSet();
rst.initiate();

const primary = rst.getPrimary();
const primaryDB = primary.getDB("testDB");
const secondary = rst.getSecondary();
const collName = "testColl";
const primaryColl = primaryDB[collName];

assert.commandWorked(primaryColl.insert({_id: "a"}, {writeConcern: {w: 2}}));
jsTestLog("Creating initial sync node.");

// Add a third node to the replica set, force it to sync from the primary, and have it hang in the
// middle of initial sync.
const initialSyncNode = rst.add({
    rsConfig: {priority: 0},
    setParameter: {
        'initialSyncMethod': 'fileCopyBased',
        'failpoint.fCBISHangAfterStartingFileClone': tojson({mode: 'alwaysOn'}),
        'logComponentVerbosity': tojson({replication: {verbosity: 1, initialSync: 2}}),
        'failpoint.forceSyncSourceCandidate':
            tojson({mode: 'alwaysOn', data: {hostAndPort: primary.name}})
    }
});
rst.reInitiate();
jsTestLog("Waiting for initial sync node to enter STARTUP_2.");
rst.waitForState(initialSyncNode, ReplSetTest.State.STARTUP_2);

// Wait for the initial syncing node to choose a sync source.
assert.soon(function() {
    const res = assert.commandWorked(initialSyncNode.adminCommand({"replSetGetStatus": 1}));
    return primary.name === res.syncSourceHost;
});
assert.commandWorked(
    initialSyncNode.adminCommand({configureFailPoint: "forceSyncSourceCandidate", mode: "off"}));

jsTestLog("Changing the initial sync node's sync source from primary to secondary.");
assert.commandWorked(initialSyncNode.adminCommand({replSetSyncFrom: secondary.name}));

assert.commandWorked(initialSyncNode.adminCommand(
    {configureFailPoint: 'fCBISHangAfterStartingFileClone', mode: 'off'}));

assert.soon(function() {
    const newSyncSource = initialSyncNode.adminCommand({"replSetGetStatus": 1});
    return newSyncSource.syncSourceHost === secondary.name;
});

rst.awaitSecondaryNodes();
rst.stopSet();
