/**
 * Test replica set failover while waiting for votes for importCollection.
 *
 * @tags: [
 *   requires_majority_read_concern,
 *   requires_persistence,
 *   requires_replication,
 *   requires_wiredtiger,
 * ]
 */

(function() {
"use strict";

load("jstests/libs/fail_point_util.js");
load("jstests/libs/parallel_shell_helpers.js");
load("jstests/libs/write_concern_util.js");  // For stopReplicationOnSecondaries.
load("src/mongo/db/modules/enterprise/jstests/live_import/libs/export_import_helpers.js");

if (_isWindows()) {
    return;
}

const dbName = "test";
const collName = "foo";

const collectionProperties = exportCollection(dbName, collName);
jsTestLog("Testing with collectionProperties: " + tojson(collectionProperties));

jsTestLog("Starting a replica set");
const rst = new ReplSetTest({nodes: 3});
const nodes = rst.startSet({setParameter: "featureFlagLiveImportExport=true"});
rst.initiate();
const primary = rst.getPrimary();
const secondary = rst.getSecondary();

// Copy the exported files into the path of each replica set node.
nodes.forEach(node => copyFilesForExport(collectionProperties, rst.getDbPath(node)));

// Set a fail point to hang importCollection before waiting for votes and stop all replications.
let failPoint = configureFailPoint(primary, "hangBeforeWaitingForImportDryRunVotes");
stopReplicationOnSecondaries(rst);

// Run importCollection on primary in a parallel shell.
const importFn = function(dbName, collName, collectionProperties) {
    const testDB = db.getMongo().getDB(dbName);
    assert.commandFailedWithCode(testDB.runCommand({
        importCollection: collName,
        collectionProperties: collectionProperties,
    }),
                                 ErrorCodes.InterruptedDueToReplStateChange);
};
const waitForImportShell =
    startParallelShell(funWithArgs(importFn, dbName, collName, collectionProperties), primary.port);

// The dryRun of the import should be waiting for votes after turning off the fail point.
failPoint.wait();
failPoint.off();

// Step up a secondary node while the primary is waiting for votes for the import dryRun.
assert.commandWorked(secondary.adminCommand({replSetStepUp: 1}));

// Restart replications.
restartReplSetReplication(rst);

// The import should fail with InterruptedDueToReplStateChange.
waitForImportShell();

// Test that the collection doesn't exist after the failover.
nodes.forEach(node => assertCollectionNotFound(node.getDB(dbName), collName));

// We should pass the dbHash check as part of the stopSet()
rst.stopSet();
}());
