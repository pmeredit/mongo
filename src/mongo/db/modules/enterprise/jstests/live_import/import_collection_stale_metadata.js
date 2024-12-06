/**
 * Test running importCollection and having it fail due to stale import metadata caused by the issue
 * described in SERVER-97741
 *
 * Fail points are used to all the dry run importCollection to succeed, and the production run
 * will return a WT error while importing indexes. The retry will only succeed if MongoDB
 * rolls back any partially done work
 *
 * @tags: [
 *   requires_majority_read_concern,
 *   requires_persistence,
 *   requires_replication,
 *   requires_wiredtiger,
 * ]
 */

import {configureFailPoint} from "jstests/libs/fail_point_util.js";
import {funWithArgs} from "jstests/libs/parallel_shell_helpers.js";
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
rst.initiate();
const primary = rst.getPrimary();
const secondary = rst.getSecondary();
const primaryDB = primary.getDB(dbName);
const secondaryDB = secondary.getDB(dbName);

// Copy the exported files into the path of each replica set node.
nodes.forEach(node => copyFilesForExport(collectionProperties, rst.getDbPath(node)));

const hangBeforeImport = configureFailPoint(primaryDB, "hangBeforeProductionImport");

jsTestLog("Importing collection to a live replica set, collectionProperties: " +
          tojson(collectionProperties));
let awaitImport =
    startParallelShell(funWithArgs(function(collectionProperties) {
                           assert.commandWorked(db.getSiblingDB("admin").runCommand(
                               {importCollection: collectionProperties, writeConcern: {w: 2}}));
                       }, collectionProperties), primary.port);
hangBeforeImport.wait();
const failImport = configureFailPoint(primaryDB, "WTFailImportSortedDataInterface", {}, {times: 1});
hangBeforeImport.off();
awaitImport();

// Ensure that we are actually triggering the failure condition, instead of blindly succeeding
checkLog.containsJson(primary, 9616600);
failImport.off();

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
