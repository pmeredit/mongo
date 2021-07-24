/**
 * Test auditing importCollection command.
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

load('src/mongo/db/modules/enterprise/jstests/audit/lib/audit.js');
load("src/mongo/db/modules/enterprise/jstests/live_import/libs/export_import_helpers.js");

const replSetName = "rs";
const dbName = "test";
const collName = "foo";

const collectionProperties = exportCollection(dbName, collName);

// Start the node with the "replSet" command line option and enable the audit of CRUD ops.
const mongo = MongoRunner.runMongodAuditLogger({
    replSet: replSetName,
});
const audit = mongo.auditSpooler();
const testDB = mongo.getDB(dbName);

// Initiate the single node replset.
let config = {_id: replSetName, protocolVersion: 1};
config.members = [{_id: 0, host: mongo.host}];
assert.commandWorked(testDB.adminCommand({replSetInitiate: config}));

// Copy the exported files into the path of the replica set node.
copyFilesForExport(collectionProperties, mongo.dbpath);

// Wait until the single node becomes primary.
assert.soon(() => testDB.runCommand({hello: 1}).isWritablePrimary);

// Create a database. A database is implicitly created when a collection inside it is created.
assert.commandWorked(testDB.runCommand({create: "anotherCollection"}));

audit.fastForward();
assert.commandWorked(testDB.runCommand({importCollection: collectionProperties}));
assertCollectionExists(testDB, collName);

// Test that importCollection is in the audit log.
audit.assertEntry("importCollection", {ns: testDB[collName].getFullName()});

// Test that there is only one importCollection entry as the dry run should not count.
audit.assertNoNewEntries("importCollection");

MongoRunner.stopMongod(mongo);
}());
