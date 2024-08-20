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

import {configureFailPoint} from "jstests/libs/fail_point_util.js";
import {funWithArgs} from "jstests/libs/parallel_shell_helpers.js";
import {ReplSetTest} from "jstests/libs/replsettest.js";
import {
    restartReplSetReplication,
    stopReplicationOnSecondaries
} from "jstests/libs/write_concern_util.js";
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
const rst = new ReplSetTest({nodes: 3});
const nodes = rst.startSet();
rst.initiate();
const primary = rst.getPrimary();
const secondary = rst.getSecondary();

// The default WC is majority and this test can't satisfy majority writes.
assert.commandWorked(primary.adminCommand(
    {setDefaultRWConcern: 1, defaultWriteConcern: {w: 1}, writeConcern: {w: "majority"}}));
rst.awaitReplication();

// Copy the exported files into the path of each replica set node.
nodes.forEach(node => copyFilesForExport(collectionProperties, rst.getDbPath(node)));

// Set a fail point to hang importCollection before waiting for votes and stop all replications.
let failPoint = configureFailPoint(primary, "hangBeforeWaitingForImportDryRunVotes");
stopReplicationOnSecondaries(rst);

// Run importCollection on primary in a parallel shell.
const importFn = function(dbName, collectionProperties) {
    const testDB = db.getMongo().getDB(dbName);
    assert.commandFailedWithCode(testDB.runCommand({importCollection: collectionProperties}),
                                 ErrorCodes.InterruptedDueToReplStateChange);
};
const waitForImportShell =
    startParallelShell(funWithArgs(importFn, dbName, collectionProperties), primary.port);

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

// Make sure the previous step up has completed before calling 'stopSet'.
rst.getPrimary();

// We should pass the dbHash check as part of the stopSet()
rst.stopSet();