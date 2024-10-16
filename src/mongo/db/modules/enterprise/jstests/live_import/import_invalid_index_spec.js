/**
 * Test that the server doesn't crash when importing a collection with invalid index spec.
 *
 *  @tags: [
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
                         assert.commandWorked(coll.insert({a: 1, b: 2}));
                         assert.commandWorked(coll.createIndexes([{a: 1}, {b: 1}]));
                     }));

// Setup a replica set that we'll import the exported collection to.
const rst = new ReplSetTest({nodes: 1});
const nodes = rst.startSet();
rst.initiateWithHighElectionTimeout();
const primary = rst.getPrimary();
const primaryDB = primary.getDB(dbName);

// Copy the exported files into the path of each replica set node.
nodes.forEach(node => copyFilesForExport(collectionProperties, rst.getDbPath(node)));

// Manually corrupt the index spec.

// Test importing non-ready index.
collectionProperties.metadata.md.indexes[1].ready = false;
assert.commandFailedWithCode(primaryDB.runCommand({importCollection: collectionProperties}),
                             ErrorCodes.BadValue);
collectionProperties.metadata.md.indexes[1].ready = true;

// Test malformed spec.
collectionProperties.metadata.md.indexes[1].spec.v = "bug";
assert.commandFailedWithCode(primaryDB.runCommand({importCollection: collectionProperties}),
                             ErrorCodes.BadValue);
collectionProperties.metadata.md.indexes[1].spec.v = 2;

// Test duplicating index names.
collectionProperties.metadata.md.indexes[1].spec.name = "b_1";
assert.commandFailedWithCode(primaryDB.runCommand({importCollection: collectionProperties}),
                             ErrorCodes.BadValue);
collectionProperties.metadata.md.indexes[1].spec.name = "a_1";

// Test index name with missing index ident.
collectionProperties.metadata.md.indexes[1].spec.name = "bug";
assert.commandFailedWithCode(primaryDB.runCommand({importCollection: collectionProperties}), 13111);
collectionProperties.metadata.md.indexes[1].spec.name = "a_1";

// Working case with valid collectionProperties.
assert.commandWorked(primaryDB.runCommand({importCollection: collectionProperties}));

// Validate the collection and test that we can read from and write to the imported collection.
const collection = primaryDB.getCollection(collName);
validateImportCollection(collection, collectionProperties);

assert.eq({a: 1, b: 2}, collection.findOne({}, {_id: 0}));
assert.commandWorked(collection.insert({a: 3, b: 4, c: 5}));
assert.commandWorked(collection.createIndex({c: 1}));

rst.stopSet();