/**
 * Test rollback of importCollection.
 *
 * @tags: [
 *   requires_majority_read_concern,
 *   requires_persistence,
 *   requires_replication,
 *   requires_wiredtiger,
 *   requires_mongobridge,
 * ]
 */

import {ReplSetTest} from "jstests/libs/replsettest.js";
import {RollbackTest} from "jstests/replsets/libs/rollback_test.js";
import {
    assertCollectionExists,
    assertCollectionNotFound,
    copyFilesForExport,
    exportCollection,
} from "src/mongo/db/modules/enterprise/jstests/live_import/libs/export_import_helpers.js";

const dbName = "test";
const collName = "foo";

const collectionProperties = exportCollection(dbName, collName);

// Test rollback.
jsTestLog("Starting a rollback test");
const rst = new ReplSetTest({name: jsTestName(), nodes: 3, useBridge: true});
const nodes = rst.startSet();
let config = rst.getReplSetConfig();
config.members[2].priority = 0;
config.settings = {
    chainingAllowed: false
};
rst.initiate(config);

// Copy the exported files into the path of each replica set node.
nodes.forEach(node => copyFilesForExport(collectionProperties, rst.getDbPath(node)));

const rollbackTest = new RollbackTest(jsTestName(), rst);
const rollbackNode = rollbackTest.transitionToRollbackOperations();
const rollbackDB = rollbackNode.getDB(dbName);

jsTestLog("Importing collection to a live replica set, collectionProperties: " +
          tojson(collectionProperties));
// Use {force: true} for the import so that we don't block on the dryRun phase.
assert.commandWorked(rollbackDB.runCommand({importCollection: collectionProperties, force: true}));
assertCollectionExists(rollbackDB, collName);
assert.commandWorked(rollbackDB.runCommand({insert: collName, documents: [{x: 1}]}));
assert.eq(rollbackDB[collName].count(), 1);

// Start rollback.
rollbackTest.transitionToSyncSourceOperationsBeforeRollback();
rollbackTest.transitionToSyncSourceOperationsDuringRollback();

// Wait for rollback to finish.
rollbackTest.transitionToSteadyStateOperations();

// Make sure the imported collection has been rolled back on all nodes.
nodes.forEach(node => assertCollectionNotFound(node.getDB(dbName), collName));

// We should pass the dbHash check as part of the stopSet()
rollbackTest.stop();
