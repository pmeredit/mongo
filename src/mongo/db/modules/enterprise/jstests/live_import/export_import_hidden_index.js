/**
 * Tests exporting and importing with a hidden index.
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
const collectionProperties = exportCollection(dbName, collName, ((coll) => {
                                                  assert.commandWorked(coll.insert({a: 1}));
                                                  assert.commandWorked(coll.createIndex({a: 1}));
                                                  assert.commandWorked(coll.hideIndex({a: 1}));
                                              }));

// Setup a replica set that we'll import the exported collection to.
jsTestLog("Starting a replica set for import");
const rst = new ReplSetTest({nodes: 3});
const nodes = rst.startSet();
rst.initiateWithHighElectionTimeout();
const primary = rst.getPrimary();
const primaryDB = primary.getDB(dbName);

// Copy the exported files into the path of each replica set node.
nodes.forEach(node => copyFilesForExport(collectionProperties, rst.getDbPath(node)));

// Import and validate the collection on the replica set.
assert.commandWorked(
    primaryDB.runCommand({importCollection: collectionProperties, writeConcern: {w: 3}}));

const collection = primaryDB.getCollection(collName);
validateImportCollection(collection, collectionProperties);

// Can't 'hint' a hidden index.
assert.commandFailedWithCode(collection.runCommand("find", {hint: {a: 1}}), ErrorCodes.BadValue);

assert.commandWorked(collection.unhideIndex({a: 1}));
assert.eq(1, collection.find().hint({a: 1}).toArray().length);

rst.stopSet();