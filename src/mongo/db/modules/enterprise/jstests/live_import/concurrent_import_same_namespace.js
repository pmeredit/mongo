/**
 * Test concurrent import on the same namespace.
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
    assertCollectionExists,
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
rst.initiateWithHighElectionTimeout();
const primary = rst.getPrimary();

// Copy the exported files into the path of each replica set node.
nodes.forEach(node => copyFilesForExport(collectionProperties, rst.getDbPath(node)));

// Set a fail point on primary hang the import after the dry run.
let failPoint = configureFailPoint(primary, "hangAfterImportDryRun");

// Function to run importCollection on primary in a parallel shell.
const importFn = function(dbName, collectionProperties) {
    const testDB = db.getMongo().getDB(dbName);
    // The import should either succeed or lose to the other thread.
    assert.commandWorkedOrFailedWithCode(testDB.runCommand({
        importCollection: collectionProperties,
        writeConcern: {w: 2},
    }),
                                         ErrorCodes.NamespaceExists);
};

// Run import in two parallel threads with the same namespace.
const importThread1 =
    startParallelShell(funWithArgs(importFn, dbName, collectionProperties), primary.port);
const importThread2 =
    startParallelShell(funWithArgs(importFn, dbName, collectionProperties), primary.port);

// Both threads should be able to succeed the dry run.
assert.commandWorked(primary.adminCommand(
    {waitForFailPoint: "hangAfterImportDryRun", timesEntered: 2, maxTimeMS: 60 * 1000}));
failPoint.off();

// Only one thread will win.
importThread1();
importThread2();

// Verify the imported collection exists.
nodes.forEach(node => assertCollectionExists(node.getDB(dbName), collName));

// We should pass the dbHash check as part of the stopSet()
rst.stopSet();