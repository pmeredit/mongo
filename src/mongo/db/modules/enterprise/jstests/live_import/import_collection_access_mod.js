/**
 * Test that importCollection triggers user cache invalidation when admin.system.users is imported.
 *
 * @tags: [
 *   requires_majority_read_concern,
 *   requires_persistence,
 *   requires_replication,
 *   requires_wiredtiger,
 * ]
 */
import "src/mongo/db/modules/enterprise/jstests/audit/lib/audit.js";

import {
    copyFilesForExport,
    exportCollectionExtended
} from "src/mongo/db/modules/enterprise/jstests/live_import/libs/export_import_helpers.js";

const replSetName = "rs";
const kAdminDB = "admin";
const kUsersColl = "system.users";

const collectionProperties =
    exportCollectionExtended(kAdminDB, kUsersColl, "user1", "password1", m1 => {
        const db1 = m1.getDB(kAdminDB);
        assert.commandWorked(
            db1.runCommand({createUser: "user1", pwd: "password1", roles: ["root"]}));
    });

assert.gt(collectionProperties.numRecords, NumberLong(0));
assert.gt(collectionProperties.dataSize, NumberLong(0));

// Start the node with the "replSet" command line option and enable the audit of CRUD ops.
const mongo = MongoRunner.runMongodAuditLogger({replSet: replSetName});
const audit = mongo.auditSpooler();
const testDB = mongo.getDB(kAdminDB);

// Initiate the single node replset.
const config = {
    _id: replSetName,
    protocolVersion: 1,
    members: [{_id: 0, host: mongo.host}]
};
assert.commandWorked(testDB.adminCommand({replSetInitiate: config}));

// Copy the exported files into the path of the replica set node.
copyFilesForExport(collectionProperties, mongo.dbpath);

// Wait until the single node becomes primary.
assert.soon(() => testDB.runCommand({hello: 1}).isWritablePrimary);
audit.fastForward();

assert.commandWorked(testDB.adminCommand({setParameter: 1, logLevel: 5}));
assert.commandWorked(testDB.runCommand({importCollection: collectionProperties}));
assert.commandWorked(testDB.adminCommand({setParameter: 1, logLevel: 0}));

checkLog.contains(mongo, "Invalidating user cache");

audit.assertEntry("importCollection", {"ns": "admin.system.users"});
audit.assertEntryRelaxed("directAuthMutation", {"ns": "admin.system.users", "operation": "insert"});

MongoRunner.stopMongod(mongo);
