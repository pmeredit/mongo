/**
 * Test that secondary oplog application is not blocked forever on running the
 * voteCommitImportCollection command against the primary even if the command hangs.
 *
 * @tags: [
 *   requires_majority_read_concern,
 *   requires_persistence,
 *   requires_replication,
 *   requires_wiredtiger,
 * ]
 */

import {configureFailPoint} from "jstests/libs/fail_point_util.js";
import {funWithArgs} from "jstests/libs/parallel_shell_helpers.js";
import {ReplSetTest} from "jstests/libs/replsettest.js";
import {
    assertCollectionNotFound,
    copyFilesForExport,
    exportCollection
} from "src/mongo/db/modules/enterprise/jstests/live_import/libs/export_import_helpers.js";

const dbName = "test";
const collName = "foo";

const collectionProperties = exportCollection(dbName, collName);
jsTestLog("Testing with collectionProperties: " + tojson(collectionProperties));

jsTestLog("Starting a replica set");
const rst = new ReplSetTest({nodes: 2});
const nodes = rst.startSet();
rst.initiate();
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

// Wait for the secondary to vote for the dry run.
jsTestLog('Waiting for the primary to receive a vote from the secondary for the dry run.');
checkLog.containsJson(primary, 5085500, {dryRunSuccess: true});

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