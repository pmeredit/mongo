/**
 * Tests running importCollection on a time-series collection in a live replica set. Makes sure that
 * the collection can be imported with a missing _id index.
 *
 * @tags: [
 *   requires_majority_read_concern,
 *   requires_persistence,
 *   requires_replication,
 *   requires_wiredtiger,
 * ]
 */

import {TimeseriesTest} from "jstests/core/timeseries/libs/timeseries.js";
import {ReplSetTest} from "jstests/libs/replsettest.js";
import {
    assertCollectionExists,
    copyFilesForExport
} from "src/mongo/db/modules/enterprise/jstests/live_import/libs/export_import_helpers.js";

const dbName = "test";
const collName = "import_collection_timeseries";

Random.setRandomSeed();

// Creates a time-series collection to be used for export.
function createTimeseriesCollection() {
    let conn = MongoRunner.runMongod();
    assert(conn);
    let db = conn.getDB(dbName);

    const timeFieldName = 'time';
    assert.commandWorked(db.createCollection(collName, {timeseries: {timeField: timeFieldName}}));

    let coll = db.getCollection(collName);

    const numHosts = 10;
    const hosts = TimeseriesTest.generateHosts(numHosts);

    for (let i = 0; i < 100; i++) {
        const host = TimeseriesTest.getRandomElem(hosts);
        TimeseriesTest.updateUsages(host.fields);

        assert.commandWorked(coll.insert({
            measurement: "cpu",
            time: ISODate(),
            fields: host.fields,
            tags: host.tags,
        }));
    }

    MongoRunner.stopMongod(conn);
    return conn.dbpath;
}

// Exports the time-series collection.
function exportTimeseriesCollection(dbpath) {
    let conn = MongoRunner.runMongod({
        dbpath: dbpath,
        noCleanData: true,
        queryableBackupMode: "",
    });
    assert(conn);

    let db = conn.getDB(dbName);

    // Need to export both the 'system.views' and buckets collection.
    let exportedCollections = [];
    let collectionProperties =
        assert.commandWorked(db.runCommand({exportCollection: "system.views"}));
    exportedCollections.push(collectionProperties);

    collectionProperties =
        assert.commandWorked(db.runCommand({exportCollection: "system.buckets." + collName}));
    exportedCollections.push(collectionProperties);

    MongoRunner.stopMongod(conn);
    return exportedCollections;
}

function importTimeseriesCollection(collectionsToImport) {
    const rst = new ReplSetTest({nodes: 2});
    const nodes = rst.startSet();
    rst.initiate();
    const primary = rst.getPrimary();
    const secondary = rst.getSecondary();
    const primaryDB = primary.getDB(dbName);
    const secondaryDB = secondary.getDB(dbName);

    for (const collectionProperties of collectionsToImport) {
        // Copy the exported files into the path of each replica set node.
        nodes.forEach(node => copyFilesForExport(collectionProperties, rst.getDbPath(node)));

        jsTestLog("Importing collection to a live replica set, collectionProperties: " +
                  tojson(collectionProperties));

        assert.commandWorked(
            primaryDB.runCommand({importCollection: collectionProperties, writeConcern: {w: 2}}));
    }

    let coll = primaryDB.getCollection(collName);

    // Verify that the time-series view exists.
    assert.eq(coll.getFullName(),
              primaryDB.getCollection("system.views").find({}).toArray()[0]["_id"]);
    assert.eq(coll.getFullName(),
              secondaryDB.getCollection("system.views").find({}).toArray()[0]["_id"]);

    // Verify that the time-series bucket collection exists.
    assertCollectionExists(primaryDB, "system.buckets." + collName);
    assertCollectionExists(secondaryDB, "system.buckets." + collName);

    // Verify that there are no indexes.
    assert.eq(
        0,
        assert.commandWorked(primaryDB.runCommand({listIndexes: "system.buckets." + collName}))
            .cursor.firstBatch.length);
    assert.eq(
        0,
        assert.commandWorked(secondaryDB.runCommand({listIndexes: "system.buckets." + collName}))
            .cursor.firstBatch.length);

    // Verify that the buckets collection is clustered.
    const primaryOptions =
        assert
            .commandWorked(primaryDB.runCommand(
                {listCollections: 1, filter: {name: "system.buckets." + collName}}))
            .cursor.firstBatch[0]
            .options;
    const secondaryOptions =
        assert
            .commandWorked(secondaryDB.runCommand(
                {listCollections: 1, filter: {name: "system.buckets." + collName}}))
            .cursor.firstBatch[0]
            .options;

    assert(primaryOptions.hasOwnProperty("clusteredIndex"));
    assert(secondaryOptions.hasOwnProperty("clusteredIndex"));

    // Test that we can write to the imported collection.
    const numHosts = 10;
    const hosts = TimeseriesTest.generateHosts(numHosts);

    for (let i = 0; i < 100; i++) {
        const host = TimeseriesTest.getRandomElem(hosts);
        TimeseriesTest.updateUsages(host.fields);

        assert.commandWorked(coll.insert({
            measurement: "cpu",
            time: ISODate(),
            fields: host.fields,
            tags: host.tags,
        }));
    }

    rst.stopSet();
}

const dbpath = createTimeseriesCollection();
const exportedCollections = exportTimeseriesCollection(dbpath);
importTimeseriesCollection(exportedCollections);