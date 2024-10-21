/**
 * Basic end-to-end test that exports a non-empty collection which gets imported into a live replica
 * set.
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
    copyFilesForExport,
    exportCollection,
    validateImportCollection
} from "src/mongo/db/modules/enterprise/jstests/live_import/libs/export_import_helpers.js";

const dbName = "test";
const collName = "foo";

// Export the collection after applying the operations.
const collectionProperties =
    exportCollection(dbName, collName, ((coll) => {
                         assert.commandWorked(coll.insert({a: 1, b: 2, c: 3}));
                         assert.commandWorked(coll.createIndexes([{a: 1}, {b: 1, c: 1}]));
                     }));

// Setup a replica set that we'll import the exported collection to.
jsTestLog("Starting a replica set for import");
const rst = new ReplSetTest({nodes: 3});
const nodes = rst.startSet();
rst.initiate();
const primary = rst.getPrimary();
const primaryDB = primary.getDB(dbName);

// Copy the exported files into the path of each replica set node.
nodes.forEach(node => copyFilesForExport(collectionProperties, rst.getDbPath(node)));

// Import and validate the collection on the replica set.
assert.commandWorked(
    primaryDB.runCommand({importCollection: collectionProperties, writeConcern: {w: 3}}));

const collection = primaryDB.getCollection(collName);
validateImportCollection(collection, collectionProperties);

assert.eq({a: 1, b: 2, c: 3}, collection.findOne({}, {_id: 0}));
assert.commandWorked(collection.insert({e: 4}));

rst.stopSet();