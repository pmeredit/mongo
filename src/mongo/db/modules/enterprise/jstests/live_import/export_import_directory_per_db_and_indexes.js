/**
 * Tests the behaviour when the exporting and importing nodes are running under different settings
 * for 'directoryPerDB' and 'directoryForIndexes'. Both the exporting and importing nodes need to be
 * running under the same configuration for the import to succeed.
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
    exportCollection
} from "src/mongo/db/modules/enterprise/jstests/live_import/libs/export_import_helpers.js";

const dbName = "test";
const collName = "foo";

// Export the collection after applying the operations.
const collectionProperties =
    exportCollection(dbName, collName, ((coll) => {
                         assert.commandWorked(coll.insert({a: 1, b: 2, c: 3}));
                         assert.commandWorked(coll.createIndexes([{a: 1}, {b: 1, c: 1}]));
                     }));

function testImport(collectionProperties, mongodOptions) {
    // Set up a replica set that we'll import the exported collection to.
    jsTestLog("Starting a replica set for import with extra options: " + tojson(mongodOptions));
    const rst = new ReplSetTest({nodes: 1});
    const nodes = rst.startSet(mongodOptions);
    rst.initiateWithHighElectionTimeout();
    const primary = rst.getPrimary();
    const primaryDB = primary.getDB(dbName);

    // Copy the exported files into the path of each replica set node.
    nodes.forEach(node => copyFilesForExport(collectionProperties, rst.getDbPath(node)));

    const res = primaryDB.runCommand({importCollection: collectionProperties});

    rst.stopSet();
    return res;
}

assert.commandWorked(testImport(collectionProperties, {}));
assert.commandFailedWithCode(testImport(collectionProperties, {directoryperdb: ""}),
                             ErrorCodes.InvalidOptions);
assert.commandFailedWithCode(testImport(collectionProperties, {wiredTigerDirectoryForIndexes: ""}),
                             ErrorCodes.InvalidOptions);
assert.commandFailedWithCode(
    testImport(collectionProperties, {directoryperdb: "", wiredTigerDirectoryForIndexes: ""}),
    ErrorCodes.InvalidOptions);