/**
 * Test that a node in FCBIS does not report replication progress. There are two routes
 * these kinds of updates take:
 *  - via spanning tree:
 *      initial-syncing nodes should send no replSetUpdatePosition commands upstream at all
 *  - via heartbeats:
 *      these nodes should include null lastApplied and lastDurable optimes in heartbeat responses
 * Neither of these should happen during FCBIS.
 *
 * @tags: [requires_persistence, requires_wiredtiger]
 */
import {kDefaultWaitForFailPointTimeout} from "jstests/libs/fail_point_util.js";
import {ReplSetTest} from "jstests/libs/replsettest.js";
import {checkWriteConcernTimedOut} from "jstests/libs/write_concern_util.js";
import {reconfig} from "jstests/replsets/rslib.js";

TestData.skipEnforceFastCountOnValidate = true;
const testName = jsTestName();
const rst = new ReplSetTest({name: testName, nodes: [{}, {rsConfig: {priority: 0}}]});
rst.startSet();
rst.initiateWithHighElectionTimeout();

const primary = rst.getPrimary();
const primaryDb = primary.getDB("test");
// The default WC is majority and this test cannot satisfy majority writes.
assert.commandWorked(primary.adminCommand(
    {setDefaultRWConcern: 1, defaultWriteConcern: {w: 1}, writeConcern: {w: "majority"}}));

assert.commandWorked(primaryDb.test.insert({"starting": "doc"}, {writeConcern: {w: 2}}));

jsTestLog("Ensuring there is an up-to-date stable checkpoint for FCBIS to copy from.");
rst.awaitLastStableRecoveryTimestamp();
assert.commandWorked(primary.adminCommand({fsync: 1}));

jsTestLog("Adding a new node to the replica set");

const secondary = rst.add({
    rsConfig: {priority: 0, votes: 0},
    setParameter: {
        'failpoint.forceSyncSourceCandidate':
            tojson({mode: 'alwaysOn', data: {"hostAndPort": primary.host}}),
        'initialSyncMethod': 'fileCopyBased',
        'failpoint.fCBISHangBeforeFinish': tojson({mode: 'alwaysOn'}),
    }
});
rst.reInitiate();
rst.waitForState(secondary, ReplSetTest.State.STARTUP_2);

// Add the new node with votes:0 and then give it votes:1 to avoid 'newlyAdded' and mimic a resync,
// where a node is in initial sync with 1 vote.
let nextConfig = rst.getReplSetConfigFromNode(0);
nextConfig.members[2].votes = 1;
reconfig(rst, nextConfig, false /* force */, true /* wait */);

// Shut down the steady-state secondary so that it cannot participate in the majority.
rst.stop(1);

jsTestLog("Waiting for FCBIS node to get to end of initial sync");

assert.soonNoExcept(() => {
    assert.commandWorked(secondary.adminCommand({
        waitForFailPoint: "fCBISHangBeforeFinish",
        timesEntered: 1,
        maxTimeMS: kDefaultWaitForFailPointTimeout
    }));
    return true;
});

jsTestLog("Checking write acknowledgment");

// Make sure the node does not participate in the acknowledgment of any writes.
const writeResW2 = primaryDb.runCommand({
    insert: "test",
    documents: [{"writeConcernTwo": "shouldfail"}],
    writeConcern: {w: 2, wtimeout: 4000}
});
assert.commandWorkedIgnoringWriteConcernErrors(writeResW2);
checkWriteConcernTimedOut(writeResW2);

const writeResWMaj = primaryDb.runCommand({
    insert: "test",
    documents: [{"writeConcernMajority": "shouldfail"}],
    writeConcern: {w: "majority", wtimeout: 4000}
});
assert.commandWorkedIgnoringWriteConcernErrors(writeResWMaj);
checkWriteConcernTimedOut(writeResWMaj);

// The FCBIS node should have not sent replSetUpdatePosition at any point.
sleep(4 * 1000);
const numUpdatePosition = assert.commandWorked(secondary.adminCommand({serverStatus: 1}))
                              .metrics.repl.network.replSetUpdatePosition.num;
assert.eq(0, numUpdatePosition);

const nullOpTime = {
    "ts": Timestamp(0, 0),
    "t": NumberLong(-1)
};
const nullWallTime = ISODate("1970-01-01T00:00:00Z");

// Make sure that even though the FCBIS node adjusts its lastApplied and lastDurable at the end...
const statusAfterWMaj = assert.commandWorked(secondary.adminCommand({replSetGetStatus: 1}));
const secondaryOpTimes = statusAfterWMaj.optimes;
assert.gte(
    bsonWoCompare(secondaryOpTimes.appliedOpTime, nullOpTime), 0, () => tojson(secondaryOpTimes));
assert.gte(
    bsonWoCompare(secondaryOpTimes.durableOpTime, nullOpTime), 0, () => tojson(secondaryOpTimes));
assert.neq(nullWallTime, secondaryOpTimes.optimeDate, () => tojson(secondaryOpTimes));
assert.neq(nullWallTime, secondaryOpTimes.optimeDurableDate, () => tojson(secondaryOpTimes));

// ...the primary thinks they're still null as they were null in the heartbeat responses.
const primaryStatusRes = assert.commandWorked(primary.adminCommand({replSetGetStatus: 1}));
const secondaryOpTimesAsSeenByPrimary = primaryStatusRes.members[2];
assert.docEq(secondaryOpTimesAsSeenByPrimary.optime,
             nullOpTime,
             () => tojson(secondaryOpTimesAsSeenByPrimary));
assert.docEq(secondaryOpTimesAsSeenByPrimary.optimeDurable,
             nullOpTime,
             () => tojson(secondaryOpTimesAsSeenByPrimary));
assert.eq(nullWallTime,
          secondaryOpTimesAsSeenByPrimary.optimeDate,
          () => tojson(secondaryOpTimesAsSeenByPrimary));
assert.eq(nullWallTime,
          secondaryOpTimesAsSeenByPrimary.optimeDurableDate,
          () => tojson(secondaryOpTimesAsSeenByPrimary));

jsTestLog("Waiting for FCBIS node to reach steady state");

// Turn off the failpoint and wait for the node to finish initial sync.
assert.commandWorked(
    secondary.adminCommand({configureFailPoint: "fCBISHangBeforeFinish", mode: "off"}));
rst.awaitSecondaryNodes(null, [secondary]);

jsTestLog("Doing new write that should be accepted");

// The set should now be able to satisfy {w:2} writes.
assert.commandWorked(
    primaryDb.runCommand({insert: "test", documents: [{"will": "succeed"}], writeConcern: {w: 2}}));

jsTestLog("Done with test");
rst.restart(1);
rst.stopSet();
