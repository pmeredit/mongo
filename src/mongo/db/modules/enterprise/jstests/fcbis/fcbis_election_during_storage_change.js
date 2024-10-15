/**
 * When an initial syncing node is being resynced, it can cast a vote in an election. This test
 * ensures that if FCBIS is in the middle of a storage change at the time, the node does not vote
 * "yes" and the FCBIS succeeds.
 * @tags: [requires_persistence, requires_wiredtiger]
 */

import {kDefaultWaitForFailPointTimeout} from "jstests/libs/fail_point_util.js";
import {Thread} from "jstests/libs/parallelTester.js";
import {ReplSetTest} from "jstests/libs/replsettest.js";
import {setLogVerbosity} from "jstests/replsets/rslib.js";

TestData.skipEnforceFastCountOnValidate = true;
const testName = jsTestName();
const dbName = "testdb";
const collName = "testcoll";

const rst = new ReplSetTest({
    name: testName,
    nodes: [{}, {}, {rsConfig: {priority: 0}}],
    settings: {chainingAllowed: false},
    useBridge: true
});
rst.startSet();
rst.initiate();

const primary = rst.getPrimary();
const primaryDb = primary.getDB(dbName);
const primaryColl = primaryDb.getCollection(collName);
const initialSyncNode = rst.nodes[2];

jsTestLog("Adding some data to clone");
assert.commandWorked(primaryColl.insert({"starting": "doc"}, {writeConcern: {w: 3}}));

jsTestLog("Ensuring there is an up-to-date stable checkpoint for FCBIS to copy from.");
rst.awaitLastStableRecoveryTimestamp();
assert.commandWorked(primary.adminCommand({fsync: 1}));

jsTestLog("Restarting a node so it will do an initial sync. Stopping it before switching storage");
rst.restart(initialSyncNode, {
    startClean: true,
    setParameter: {
        'initialSyncMethod': "fileCopyBased",
        'failpoint.fCBISHangBeforeSwitchingStorage': tojson({mode: 'alwaysOn'}),
        'numInitialSyncAttempts': 1
    }
});

assert.commandWorked(initialSyncNode.adminCommand({
    waitForFailPoint: "fCBISHangBeforeSwitchingStorage",
    timesEntered: 1,
    maxTimeMS: kDefaultWaitForFailPointTimeout
}));

// Make sure id:5972100 debug log is enabled.
setLogVerbosity([initialSyncNode], {"replication": {"verbosity": 1}});

// Trigger an election that requires the initial syncing node's vote.
rst.nodes[0].disconnect(rst.nodes[1]);
let election = new Thread(function(host, initialSyncHost) {
    const conn = new Mongo(host);
    const initialSyncConn = new Mongo(initialSyncHost);
    let sawYesVote = false;
    while (!sawYesVote) {
        // The election sometimes fails spuriously due to missing a hearbeat from node 1 and
        // not yet getting one from node 2.  Retry in that case.
        assert.commandFailedWithCode(conn.adminCommand({replSetStepUp: 1}),
                                     ErrorCodes.CommandFailed);
        try {
            sawYesVote = checkLog.checkContainsOnceJson(initialSyncConn, 5972100, {});
        } catch (e) {
            // If we get this errorcode, it means the main thread has seen the vote log, we should
            // mark it as success.
            if (e.code === ErrorCodes.InterruptedDueToStorageChange) {
                sawYesVote = true;
            } else {
                throw e;
            }
        }
        if (!sawYesVote) {
            jsTestLog("Retrying election in 1 second");
            sleep(1000);
        }
    }
}, rst.nodes[1].host, initialSyncNode.host);
election.start();

jsTestLog("Waiting for the initial sync node to try to vote yes");
checkLog.containsJson(initialSyncNode, 5972100, {});
jsTestLog("Allowing storage change to continue");
assert.commandWorked(initialSyncNode.adminCommand(
    {configureFailPoint: "fCBISHangBeforeSwitchingStorage", mode: "off"}));

jsTestLog("Waiting for election to fail");
election.join();

jsTestLog("Waiting for initial sync to complete");
rst.waitForState(initialSyncNode, ReplSetTest.State.SECONDARY);

jsTestLog("Election should succeed now");
assert.commandWorked(rst.nodes[1].adminCommand({replSetStepUp: 1}));

rst.nodes[1].reconnect(rst.nodes);

rst.stopSet();
