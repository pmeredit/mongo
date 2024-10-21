/**
 * Tests that in progress index builds are restarted on file copy based initial sync node.
 * There are four cases that need to be tested
 * 1) Index started before checkpoint, not committed before FCBIS is complete
 * 2) Index started before checkpoint, committed in oplog applied by FCBIS
 * 3) Index started in oplog applied by FCBIS, not committed before FCBIS is complete
 * 4) Index started in oplog applied by FCBIS, commited in oplog applied by FCBIS.
 * @tags: [requires_persistence, requires_wiredtiger]
 */
import {kDefaultWaitForFailPointTimeout} from "jstests/libs/fail_point_util.js";
import {ReplSetTest} from "jstests/libs/replsettest.js";
import {IndexBuildTest} from "jstests/noPassthrough/libs/index_build.js";

TestData.skipEnforceFastCountOnValidate = true;
const dbName = "test";
const collName = "coll";
const indexNames = ["_id_", "a_1", "b_1", "c_1", "d_1"];

function isIndexBuildInProgress(conn, indexName) {
    jsTestLog("Running collection stats on " + conn.host);
    const coll = conn.getDB(dbName)[collName];
    const stats = assert.commandWorked(coll.stats());
    const indexBuildStats = stats["indexBuilds"];
    assert(Array.isArray(indexBuildStats));
    return Array.contains(indexBuildStats, indexName);
}

const rst = new ReplSetTest({
    name: jsTestName(),
    // Three nodes are required to ensure we always have a majority even when one secondary can't
    // do index builds.  Chaining disallowed to make sure the second secondary isn't syncing off
    // the stopped node.
    nodes: [{}, {rsConfig: {priority: 0}}, {rsConfig: {priority: 0}}],
    settings: {chainingAllowed: false}
});
rst.startSet();
rst.initiate();
const primary = rst.getPrimary();
const secondary = rst.getSecondary();
const primaryDB = primary.getDB(dbName);
const primaryColl = primaryDB.getCollection(collName);

assert.commandWorked(primaryColl.insert([{a: 1}, {b: 1}, {c: 1}, {d: 1}]));

jsTestLog("Starting index builds on the primary, preventing them from completing");
IndexBuildTest.pauseIndexBuilds(secondary);
const awaitCreateIndex1 = IndexBuildTest.startIndexBuild(
    primary, primaryColl.getFullName(), {a: 1}, {}, [], "votingMembers");

const awaitCreateIndex2 = IndexBuildTest.startIndexBuild(
    primary, primaryColl.getFullName(), {b: 1}, {}, [], "votingMembers");

jsTestLog("Waiting for index build 1 to start");
IndexBuildTest.waitForIndexBuildToStart(primaryDB, primaryColl.getName(), indexNames[1]);
jsTestLog("Waiting for index build 2 to start");
IndexBuildTest.waitForIndexBuildToStart(primaryDB, primaryColl.getName(), indexNames[2]);

jsTestLog("Waiting for replication");
// FCBIS copies from a checkpoint so we need to make sure the stable optime on the primary is up to
// date, and force a checkpoint to happen so that the incomplete index builds are in the checkpoint.
rst.awaitLastStableRecoveryTimestamp();
assert.commandWorked(primary.adminCommand({fsync: 1}));

jsTestLog("Adding the initial sync destination node to the replica set");
const initialSyncNode = rst.add({
    rsConfig: {priority: 0, votes: 0},
    setParameter: {
        'initialSyncMethod': 'fileCopyBased',
        'initialSyncSourceReadPreference': 'primary',
        'failpoint.fCBISHangAfterFileCloning': tojson({mode: "alwaysOn"}),
        'failpoint.fCBISForceExtendBackupCursor': tojson({mode: {times: 1}}),
        'numInitialSyncAttempts': 1,
        // WiredTiger debug logging is off as it is very verbose and not relevant for this test.
        'logComponentVerbosity':
            tojson({replication: {verbosity: 1, initialSync: 2}, storage: {verbosity: 1, wt: 0}}),
    }
});

jsTestLog("Starting file copy based initial sync");
rst.reInitiate();

jsTestLog("Waiting for cloning of initial checkpoint to be complete");
assert.commandWorked(initialSyncNode.adminCommand({
    waitForFailPoint: "fCBISHangAfterFileCloning",
    timesEntered: 1,
    maxTimeMS: kDefaultWaitForFailPointTimeout
}));

jsTestLog(
    "Creating an index which will have both startIndexBuild and commitIndexBuild in the oplog");
assert.commandWorked(primaryColl.createIndex({d: 1}, {}, 2 /* quorum */));

jsTestLog("Reducing commit quorum for b_1 index so it completes in the oplog");
assert.commandWorked(primaryDB.runCommand(
    {setIndexCommitQuorum: collName, indexNames: [indexNames[2]], commitQuorum: 2}));
jsTestLog("Waiting for b_1 index");
awaitCreateIndex2();

jsTestLog("Creating an index which will have only startIndexBuild in the oplog");
const awaitCreateIndex3 = IndexBuildTest.startIndexBuild(
    primary, primaryColl.getFullName(), {c: 1}, {}, [], "votingMembers");
jsTestLog("Waiting for index build 3 to start");
IndexBuildTest.waitForIndexBuildToStart(primaryDB, primaryColl.getName(), indexNames[3]);

assert.commandWorked(
    initialSyncNode.adminCommand({configureFailPoint: "fCBISHangAfterFileCloning", mode: "off"}));

jsTestLog("Waiting for file copy based initial sync to complete");
rst.awaitSecondaryNodes(undefined, [initialSyncNode]);

jsTestLog("In progress index build is restarted on syncing node");
assert.eq(true, isIndexBuildInProgress(primary, indexNames[1]));
assert.eq(true, isIndexBuildInProgress(initialSyncNode, indexNames[1]));

jsTestLog("Completed index builds not in progress on syncing node");
assert.eq(false, isIndexBuildInProgress(initialSyncNode, indexNames[4]));

jsTestLog("Resuming index builds");
IndexBuildTest.resumeIndexBuilds(secondary);
awaitCreateIndex1();
awaitCreateIndex3();

rst.awaitReplication();
jsTestLog("In progress index builds are completed on FCBIS node");
IndexBuildTest.assertIndexes(primaryColl, indexNames.length, indexNames);
IndexBuildTest.assertIndexes(
    initialSyncNode.getDB(dbName).getCollection(collName), indexNames.length, indexNames);
rst.stopSet();
