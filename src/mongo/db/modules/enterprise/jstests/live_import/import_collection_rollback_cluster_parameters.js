/**
 * Tests the commit and abort cases of importing the cluster parameters collection.
 * @tags: [
 *   requires_fcv_71,
 *   requires_majority_read_concern,
 *   requires_persistence,
 *   requires_replication,
 *   requires_wiredtiger,
 * ]
 */

import {
    runGetClusterParameterReplicaSet,
    runSetClusterParameter
} from "jstests/libs/cluster_server_parameter_utils.js";
import {configureFailPoint} from "jstests/libs/fail_point_util.js";
import {ReplSetTest} from "jstests/libs/replsettest.js";
import {
    assertCollectionExists,
    copyFilesForExport,
    exportCollection
} from "src/mongo/db/modules/enterprise/jstests/live_import/libs/export_import_helpers.js";

// set up some test CPs to be imported
const dbName = "config";
const collName = "clusterParameters";
let importedIntParam = {_id: "testIntClusterParameter", intData: 456};
let importedStrParam = {_id: "testStrClusterParameter", strData: "def"};
const collectionProperties = exportCollection(dbName, collName, coll => {
    const mongod = coll.getMongo();
    runSetClusterParameter(mongod, importedIntParam);
    runSetClusterParameter(mongod, importedStrParam);
});

// Test replica set.
jsTestLog("Starting a replica set");
const rst = new ReplSetTest({nodes: 2});
const nodes = rst.startSet();
rst.initiateWithHighElectionTimeout();
const primary = rst.getPrimary();
const secondary = rst.getSecondary();
const primaryDB = primary.getDB(dbName);
const secondaryDB = secondary.getDB(dbName);

let defaultIntParam = {_id: "testIntClusterParameter", intData: 16};
let defaultStrParam = {_id: "testStrClusterParameter", strData: "off"};

// Copy the exported files into the path of each replica set node.
nodes.forEach(node => copyFilesForExport(collectionProperties, rst.getDbPath(node)));

// Cause import to be aborted after cluster parameter op observer is set.
const fp = configureFailPoint(primary, "abortImportAfterOpObserver");

assert.commandFailed(
    primaryDB.runCommand({importCollection: collectionProperties, writeConcern: {w: 2}}));

// Ensure cluster parameters weren't updated.
runGetClusterParameterReplicaSet(
    rst, [defaultIntParam._id, defaultStrParam._id], [defaultIntParam, defaultStrParam]);

// This time, disable the failpoint and allow the import collection to succeed. Cluster parameters
// should be correctly updated.
fp.off();
assert.commandWorked(
    primaryDB.runCommand({importCollection: collectionProperties, writeConcern: {w: 2}}));

// Test that the collection exists after the import.
assertCollectionExists(primaryDB, collName);
assertCollectionExists(secondaryDB, collName);

// Ensure cluster parameters were correctly updated.
runGetClusterParameterReplicaSet(
    rst, [defaultIntParam._id, defaultStrParam._id], [importedIntParam, importedStrParam]);

// We should pass the dbHash check as part of the stopSet()
rst.stopSet();
