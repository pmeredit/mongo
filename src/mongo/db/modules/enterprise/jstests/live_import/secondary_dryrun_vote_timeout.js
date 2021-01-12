/**
 * Test that secondary oplog application is not blocked forever on running the
 * voteCommitImportCollection command against the primary even if the command hangs.
 *
 * @tags: [
 *   requires_majority_read_concern,
 *   requires_persistence,
 *   requires_replication,
 *   requires_wiredtiger,
 *   sbe_incompatible,
 * ]
 */

(function() {
"use strict";

load("jstests/libs/fail_point_util.js");
load("jstests/libs/parallel_shell_helpers.js");
load("src/mongo/db/modules/enterprise/jstests/live_import/libs/export_import_helpers.js");

const dbName = "test";
const collName = "foo";

const collectionProperties = exportCollection(dbName, collName);
jsTestLog("Testing with collectionProperties: " + tojson(collectionProperties));

jsTestLog("Starting a replica set");
const rst = new ReplSetTest({nodes: 2});
const nodes = rst.startSet({setParameter: "featureFlagLiveImportExport=true"});
rst.initiateWithHighElectionTimeout();
const primary = rst.getPrimary();

// Copy the exported files into the path of each replica set node.
nodes.forEach(node => copyFilesForExport(collectionProperties, rst.getDbPath(node)));

// Set a fail point on primary to hang the voteCommitImportCollection command.
let failPoint = configureFailPoint(primary, "hangVoteCommitImportCollectionCommand");

// Run importCollection on primary in a parallel shell.
const importFn = function(dbName, collectionProperties) {
    const testDB = db.getMongo().getDB(dbName);
    assert.commandFailedWithCode(
        testDB.runCommand({importCollection: collectionProperties, maxTimeMS: 10000}),
        ErrorCodes.MaxTimeMSExpired);
};
const waitForImportShell =
    startParallelShell(funWithArgs(importFn, dbName, collectionProperties), primary.port);

// The voteCommitImportCollection should have been blocked by now.
failPoint.wait();

// Test that replication is not blocked forever by doing a {w: 2} writes.
assert.commandWorked(primary.getDB(dbName).runCommand(
    {insert: "another_coll", documents: [{_id: 1}], writeConcern: {w: 2}}));

// Test that the import also times out eventually given the maxTimeMS.
waitForImportShell();

failPoint.off();

// Test that the collection doesn't exist after the timeout.
nodes.forEach(node => assertCollectionNotFound(node.getDB(dbName), collName));

// We should pass the dbHash check as part of the stopSet()
rst.stopSet();
}());
