/**
 * Test that applying importCollection oplog entry after initial sync is idempotent when the
 * imported collection is already cloned.
 *
 * @tags: [
 *   requires_majority_read_concern,
 *   requires_persistence,
 *   requires_replication,
 *   requires_wiredtiger,
 * ]
 */

import {kDefaultWaitForFailPointTimeout} from "jstests/libs/fail_point_util.js";
import {ReplSetTest} from "jstests/libs/replsettest.js";
import {
    assertCollectionExists,
    copyFilesForExport,
    exportCollection
} from "src/mongo/db/modules/enterprise/jstests/live_import/libs/export_import_helpers.js";

const dbName = "test";
const collName = "foo";

const collectionProperties = exportCollection(dbName, collName);
jsTestLog("Testing with collectionProperties: " + tojson(collectionProperties));

jsTestLog("Starting a replica set");
const rst = new ReplSetTest({nodes: 2});
const nodes = rst.startSet();

// Disallow the secondary node from voting so that the dry run of an import does not block on the
// initial syncing node.
const replSetConfig = rst.getReplSetConfig();
replSetConfig.members[1].priority = 0;
replSetConfig.members[1].votes = 0;
rst.initiate(replSetConfig);

// Copy the exported files into the path of each replica set node.
nodes.forEach(node => copyFilesForExport(collectionProperties, rst.getDbPath(node)));

const primary = rst.getPrimary();
const primaryDB = primary.getDB(dbName);

// Create the database by creating a collection in it.
assert.commandWorked(primaryDB.runCommand({create: "anotherCollection"}));

jsTestLog("Restarting secondary for initial sync");
const failPointOptions = tojson({
    mode: 'alwaysOn',
    data: {cloner: "DatabaseCloner", stage: "listCollections", database: dbName}
});
rst.restart(1, {
    startClean: true,
    setParameter: {
        'failpoint.hangBeforeClonerStage': failPointOptions,
        numInitialSyncAttempts: 1,
    }
});
const secondary = rst.nodes[1];

// We then block the secondary before issuing 'listCollections' on the test database.
jsTestLog("Waiting for secondary hang before DatabaseCloner");
assert.commandWorked(secondary.adminCommand({
    waitForFailPoint: "hangBeforeClonerStage",
    timesEntered: 1,
    maxTimeMS: kDefaultWaitForFailPointTimeout
}));

// Initial sync is stopped right before 'listCollections' on the test database. We now run
// importCollection on the primary.
jsTestLog("Importing collection on primary");
assert.commandWorked(primaryDB.runCommand({importCollection: collectionProperties}));

// Resume the database cloner on the secondary. Initial sync is then able to clone the imported
// collection.
assert.commandWorked(
    secondary.adminCommand({configureFailPoint: 'hangBeforeClonerStage', mode: 'off'}));

// Let the initial sync finish, making sure the importCollection oplog entry is idempotent when the
// imported collection is already cloned.
jsTestLog('Wait for both nodes to be up-to-date');
rst.awaitSecondaryNodes();
rst.awaitReplication();

// Test that initial sync skips applying the dry run and the importCollection is only applied once.
checkLog.containsWithCount(secondary, "Finished applying importCollection", 1);

// Test that the collection exists on both nodes.
nodes.forEach(node => assertCollectionExists(node.getDB(dbName), collName));

// We should pass the dbHash check as part of the stopSet()
rst.stopSet();
