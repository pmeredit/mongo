/**
 * Test rollback of importCollection.
 *
 * @tags: [requires_persistence, requires_replication, requires_wiredtiger]
 */

(function() {
"use strict";

load("jstests/replsets/libs/rollback_test.js");
load("src/mongo/db/modules/enterprise/jstests/live_import/libs/export_helpers.js");

const dbName = "test";
const collName = "foo";

const collectionProperties = exportEmptyCollectionFromStandalone(dbName, collName);

// Test rollback.
jsTestLog("Starting a rollback test");
const rst = new ReplSetTest({name: jsTestName(), nodes: 3, useBridge: true});
const nodes = rst.startSet({setParameter: "featureFlagLiveImportExport=true"});
let config = rst.getReplSetConfig();
config.members[2].priority = 0;
config.settings = {
    chainingAllowed: false
};
rst.initiateWithHighElectionTimeout(config);

const rollbackTest = new RollbackTest(jsTestName(), rst);
const rollbackNode = rollbackTest.transitionToRollbackOperations();
const rollbackDB = rollbackNode.getDB(dbName);

jsTestLog("Importing collection to a live replica set, collectionProperties: " +
          tojson(collectionProperties));
assert.commandWorked(rollbackDB.runCommand(
    {importCollection: collName, collectionProperties: collectionProperties}));
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
}());
