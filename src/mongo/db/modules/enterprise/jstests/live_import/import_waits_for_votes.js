/**
 * Test dryRun of importCollection waits for votes from data-bearing voting members.
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

load("jstests/libs/write_concern_util.js");  // For stopReplicationOnSecondaries.
load("src/mongo/db/modules/enterprise/jstests/live_import/libs/export_helpers.js");

const dbName = "test";
const collName = "foo";

const collectionProperties = exportEmptyCollectionFromStandalone(dbName, collName);
jsTestLog("Testing with collectionProperties: " + tojson(collectionProperties));

jsTestLog("Starting a replica set");
const rst = new ReplSetTest({nodes: [{}, {}, {rsConfig: {priority: 0, votes: 0}}]});
const nodes = rst.startSet({setParameter: "featureFlagLiveImportExport=true"});
rst.initiateWithHighElectionTimeout();
const primary = rst.getPrimary();
const primaryDB = primary.getDB(dbName);

// Stop replication on a data-bearing voting member and the dryRun should timeout.
stopServerReplication(nodes[1]);
assert.commandFailedWithCode(primaryDB.runCommand({
    importCollection: collName,
    collectionProperties: collectionProperties,
    maxTimeMS: 5000,
}),
                             ErrorCodes.MaxTimeMSExpired);
restartServerReplication(nodes[1]);

// Test that the collection doesn't exist after the failure.
rst.awaitLastOpCommitted();
nodes.forEach(node => assertCollectionNotFound(node.getDB(dbName), collName));

// Test that if the whole dryRun fails if it fails on one node.
assert.commandWorked(nodes[1].getDB("admin").adminCommand(
    {configureFailPoint: "failImportCollectionApplication", mode: "alwaysOn"}));
assert.commandFailedWithCode(primaryDB.runCommand({
    importCollection: collName,
    collectionProperties: collectionProperties,
}),
                             ErrorCodes.OperationFailed);
assert.commandWorked(nodes[1].getDB("admin").adminCommand(
    {configureFailPoint: "failImportCollectionApplication", mode: "off"}));

// Test that the collection doesn't exist after the failure.
rst.awaitLastOpCommitted();
nodes.forEach(node => assertCollectionNotFound(node.getDB(dbName), collName));

// Stop replication only on non-voting member and the dryRun should still go through.
stopServerReplication(nodes[2]);
assert.commandWorked(primaryDB.runCommand({
    importCollection: collName,
    collectionProperties: collectionProperties,
}));
restartServerReplication(nodes[2]);

// Test that the collection exists after the import.
rst.awaitLastOpCommitted();
nodes.forEach(node => assertCollectionExists(node.getDB(dbName), collName));

// We should pass the dbHash check as part of the stopSet()
rst.stopSet();
}());
