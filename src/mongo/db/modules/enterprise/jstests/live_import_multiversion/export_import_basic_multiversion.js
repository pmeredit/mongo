/**
 * Tests that the export and import of a non-empty collection is compatible between the 'latest' and
 * 'lastLTS' mongod binaries.
 *
 * @tags: [
 *   requires_majority_read_concern,
 *   requires_persistence,
 *   requires_replication,
 *   requires_wiredtiger,
 * ]
 */
import "jstests/multiVersion/libs/verify_versions.js";
import {ReplSetTest} from "jstests/libs/replsettest.js";
import {
    copyFilesForExport,
    exportCollection,
    validateImportCollection
} from "src/mongo/db/modules/enterprise/jstests/live_import/libs/export_import_helpers.js";

const lastLTSVersion = {
    binVersion: "last-lts"
};
const latestVersion = {
    binVersion: "latest"
};

function runVersionedExportImportTest(exportVersion, importVersion) {
    jsTestLog(`Running test with export version: ${tojson(exportVersion)}, and import version: ${
        tojson(importVersion)}`);
    const dbName = "test";
    const collName = "foo";

    // Export the collection after applying the operations.
    const collectionProperties =
        exportCollection(dbName,
                         collName,
                         ((coll) => {
                             assert.commandWorked(coll.insert({a: 1, b: 2, c: 3}));
                             assert.commandWorked(coll.createIndexes([{a: 1}, {b: 1, c: 1}]));
                         }),
                         exportVersion);
    jsTestLog(`Collection properties exported from mongod on ${tojson(exportVersion)}: ${
        tojson(collectionProperties)}`);

    // Setup a replica set that we'll import the exported collection to.
    const rst = new ReplSetTest({nodes: [importVersion, importVersion, importVersion]});
    const nodes = rst.startSet();
    rst.initiate();
    const primary = rst.getPrimary();
    assert.binVersion(primary, importVersion.binVersion);

    // Copy the exported files into the path of each replica set node.
    nodes.forEach(node => copyFilesForExport(collectionProperties, rst.getDbPath(node)));

    // Import and validate the collection on the replica set.
    const primaryDB = primary.getDB(dbName);
    assert.commandWorked(
        primaryDB.runCommand({importCollection: collectionProperties, writeConcern: {w: 3}}));

    const collection = primaryDB.getCollection(collName);
    validateImportCollection(collection, collectionProperties);

    assert.eq({a: 1, b: 2, c: 3}, collection.findOne({}, {_id: 0}));
    assert.commandWorked(collection.insert({a: 10}));

    rst.stopSet();
}

runVersionedExportImportTest(lastLTSVersion, latestVersion);
runVersionedExportImportTest(latestVersion, lastLTSVersion);
