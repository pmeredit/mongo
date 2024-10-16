/**
 * Tests restarting a file copy based initial sync during various points. We attempt to restart the
 * syncing node while it is cloning files, after it has deleted the old storage files, and after it
 * has moved files from the '.initialsync' directory to the dbpath.
 * @tags: [requires_persistence, requires_wiredtiger]
 */
import {kDefaultWaitForFailPointTimeout} from "jstests/libs/fail_point_util.js";
import {ReplSetTest} from "jstests/libs/replsettest.js";

TestData.skipEnforceFastCountOnValidate = true;
const rst = new ReplSetTest({
    nodes: [{}, {rsConfig: {priority: 0}}],
});
rst.startSet();
rst.initiateWithHighElectionTimeout();

const primary = rst.getPrimary();
const primaryDB = primary.getDB("testDB");
const collName = "testColl";
const primaryColl = primaryDB[collName];

assert.commandWorked(primaryColl.insert({_id: "a"}, {writeConcern: {w: 2}}));
// Ensure there's an up-to-date stable checkpoint for FCBIS to copy.
rst.awaitLastStableRecoveryTimestamp();
assert.commandWorked(primary.adminCommand({fsync: 1}));

jsTestLog("Creating initial sync node.");
let initialSyncNode = rst.add({
    rsConfig: {priority: 0},
    setParameter: {
        'initialSyncMethod': 'fileCopyBased',
        'failpoint.fCBISHangAfterStartingFileClone': tojson({mode: 'alwaysOn'}),
        'failpoint.forceSyncSourceCandidate':
            tojson({mode: 'alwaysOn', data: {hostAndPort: primary.name}}),
        'logComponentVerbosity': tojson({replication: {verbosity: 2}, storage: {verbosity: 2}}),
    }
});

rst.reInitiate();
jsTestLog("Waiting for initial sync node to start file cloning from the sync source.");

initialSyncNode.adminCommand({
    waitForFailPoint: "fCBISHangAfterStartingFileClone",
    timesEntered: 1,
    maxTimeMS: kDefaultWaitForFailPointTimeout
});

jsTestLog("Restarting node with 'fCBISHangAfterDeletingOldStorageFiles' enabled");
rst.restart(initialSyncNode, {
    setParameter: {
        'initialSyncMethod': 'fileCopyBased',
        'failpoint.fCBISHangAfterDeletingOldStorageFiles': tojson({mode: 'alwaysOn'}),
        'failpoint.forceSyncSourceCandidate':
            tojson({mode: 'alwaysOn', data: {hostAndPort: primary.name}}),
        'logComponentVerbosity': tojson({replication: {verbosity: 2}, storage: {verbosity: 2}}),

    }
});

jsTestLog("Waiting for initial sync node to finish deleting old storage files.");

assert.soonNoExcept(() => {
    let res = initialSyncNode.adminCommand({
        waitForFailPoint: "fCBISHangAfterDeletingOldStorageFiles",
        timesEntered: 1,
        maxTimeMS: kDefaultWaitForFailPointTimeout
    });
    return res.ok;
});

jsTestLog("Restarting node to finish initial sync");
rst.restart(initialSyncNode, {
    setParameter: {
        'initialSyncMethod': 'fileCopyBased',
        'failpoint.forceSyncSourceCandidate':
            tojson({mode: 'alwaysOn', data: {hostAndPort: primary.name}}),
        'logComponentVerbosity': tojson({replication: {verbosity: 2}, storage: {verbosity: 2}}),
    }
});

rst.waitForState(initialSyncNode, ReplSetTest.State.SECONDARY);
let initialSyncNodeDB = initialSyncNode.getDB("testDB");
assert.eq(1, initialSyncNodeDB.testColl.find().itcount());

assert.commandWorked(primaryColl.insert({_id: "b"}, {writeConcern: {w: 3}}));
// Ensure there's an up-to-date stable checkpoint for FCBIS to copy.
rst.awaitLastStableRecoveryTimestamp();
assert.commandWorked(primary.adminCommand({fsync: 1}));

jsTestLog("Restarting node cleanly to test second initial sync");
rst.restart(initialSyncNode, {
    startClean: true,
    setParameter: {
        'initialSyncMethod': 'fileCopyBased',
        'failpoint.fCBISHangAfterMovingTheNewFiles': tojson({mode: 'alwaysOn'}),
        'failpoint.forceSyncSourceCandidate':
            tojson({mode: 'alwaysOn', data: {hostAndPort: primary.name}}),
        'logComponentVerbosity': tojson({replication: {verbosity: 2}, storage: {verbosity: 2}}),
    }
});

jsTestLog("Waiting for initial sync node to finish moving new files " +
          "from the '.initialsync' directory to the original dbpath.");

assert.soonNoExcept(() => {
    let res = initialSyncNode.adminCommand({
        waitForFailPoint: "fCBISHangAfterMovingTheNewFiles",
        timesEntered: 1,
        maxTimeMS: kDefaultWaitForFailPointTimeout
    });
    return res.ok;
});

jsTestLog("Restarting node to finish initial sync");
rst.restart(initialSyncNode, {
    startClean: false,
    setParameter: {
        'initialSyncMethod': 'fileCopyBased',
        'failpoint.forceSyncSourceCandidate':
            tojson({mode: 'alwaysOn', data: {hostAndPort: primary.name}}),
        'logComponentVerbosity': tojson({replication: {verbosity: 2}, storage: {verbosity: 2}}),
    }
});

rst.waitForState(initialSyncNode, ReplSetTest.State.SECONDARY);
initialSyncNodeDB = initialSyncNode.getDB("testDB");
assert.eq(2, initialSyncNodeDB.testColl.find().itcount());

rst.stopSet();
