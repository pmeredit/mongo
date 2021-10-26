/**
 * Tests that in progress index builds are restarted on file copy based initial sync node.
 */
(function() {
"use strict";

load("jstests/replsets/rslib.js");
load("jstests/libs/fail_point_util.js");
load('jstests/noPassthrough/libs/index_build.js');  // For IndexBuildTest

const dbName = "test";
const collName = "coll";
const indexName = "a_1";

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
    nodes: 1,
});
rst.startSet();
rst.initiate();
const primary = rst.getPrimary();
const primaryDB = primary.getDB(dbName);
const primaryColl = primaryDB.getCollection(collName);

const featureEnabled = assert
                           .commandWorked(primaryDB.adminCommand(
                               {getParameter: 1, featureFlagFileCopyBasedInitialSync: 1}))
                           .featureFlagFileCopyBasedInitialSync.value;
if (!featureEnabled) {
    jsTestLog("Skipping test because the file copy based initial sync feature flag is disabled");
    rst.stopSet();
    return;
}

// Add some data to be cloned.
assert.commandWorked(primaryDB.test.insert([{a: 1}, {b: 2}, {c: 3}]));
rst.awaitReplication();

jsTestLog("Starting and pausing an index build on the primary");
assert.commandWorked(primaryColl.insert({_id: 0, a: 0}));

IndexBuildTest.pauseIndexBuilds(primary);
const awaitCreateIndex =
    IndexBuildTest.startIndexBuild(primary, primaryColl.getFullName(), {a: 1}, {}, [], "majority");
IndexBuildTest.waitForIndexBuildToScanCollection(primaryDB, primaryColl.getName(), indexName);

// FCBIS copies from a checkpoint so we need to make sure the stable optime on the primary is up to
// date, and force a checkpoint to happen so that the incomplete index build is in the checkpoint.
rst.awaitLastStableRecoveryTimestamp();
assert.commandWorked(primary.adminCommand({fsync: 1}));

jsTestLog("Adding the initial sync destination node to the replica set");
const initialSyncNode = rst.add({
    rsConfig: {priority: 0, votes: 0},
    setParameter: {
        'initialSyncMethod': 'fileCopyBased',
        'numInitialSyncAttempts': 1,
        'logComponentVerbosity': tojson({replication: {verbosity: 1}, storage: {verbosity: 1}}),
    }
});

IndexBuildTest.pauseIndexBuilds(initialSyncNode);

jsTestLog("Attempting file copy based initial sync");
rst.reInitiate();
rst.awaitSecondaryNodes();

jsTestLog("In progress index build is restarted on syncing node");
assert.eq(true, isIndexBuildInProgress(primary, indexName));
assert.eq(true, isIndexBuildInProgress(initialSyncNode, indexName));

jsTestLog("Resuming index builds");
IndexBuildTest.resumeIndexBuilds(primary);
IndexBuildTest.resumeIndexBuilds(initialSyncNode);
awaitCreateIndex();

jsTestLog("In progress index builds are completed on FCBIS node");
IndexBuildTest.assertIndexes(primaryColl, 2, ['_id_', indexName]);
IndexBuildTest.assertIndexes(
    initialSyncNode.getDB(dbName).getCollection(collName), 2, ['_id_', indexName]);
rst.stopSet();
})();
