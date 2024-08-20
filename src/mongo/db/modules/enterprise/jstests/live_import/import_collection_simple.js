/**
 * Test running importCollection in a live replica set and make sure the import operation is
 * replicated correctly.
 *
 * @tags: [
 *   requires_majority_read_concern,
 *   requires_persistence,
 *   requires_replication,
 *   requires_wiredtiger,
 * ]
 */

import {ReplSetTest} from "jstests/libs/replsettest.js";
import {
    assertCollectionExists,
    copyFilesForExport,
    exportCollection
} from "src/mongo/db/modules/enterprise/jstests/live_import/libs/export_import_helpers.js";

const dbName = "test";
const collName = "foo";

const collectionProperties = exportCollection(dbName, collName);

// Test replica set.
jsTestLog("Starting a replica set");
const rst = new ReplSetTest({nodes: 2});
const nodes = rst.startSet();
rst.initiateWithHighElectionTimeout();
const primary = rst.getPrimary();
const secondary = rst.getSecondary();
const primaryDB = primary.getDB(dbName);
const secondaryDB = secondary.getDB(dbName);

// Copy the exported files into the path of each replica set node.
nodes.forEach(node => copyFilesForExport(collectionProperties, rst.getDbPath(node)));

jsTestLog("Importing collection to a live replica set, collectionProperties: " +
          tojson(collectionProperties));
assert.commandWorked(
    primaryDB.runCommand({importCollection: collectionProperties, writeConcern: {w: 2}}));

// Test that the collection exists after the import>
assertCollectionExists(primaryDB, collName);
assertCollectionExists(secondaryDB, collName);

// Test that we can write to the imported collection.
const docs = [{_id: 1}];
assert.commandWorked(
    primaryDB.runCommand({insert: collName, documents: docs, writeConcern: {w: 2}}));
assert.eq(primaryDB[collName].find().toArray(), docs);
assert.eq(secondaryDB[collName].find().toArray(), docs);

// We should pass the dbHash check as part of the stopSet()
rst.stopSet();