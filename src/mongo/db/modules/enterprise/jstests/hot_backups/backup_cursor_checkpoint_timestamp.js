/**
 * Tests that we return the correct backup checkpoint timestamp.
 *
 * @tags: [
 *   requires_persistence,
 *   requires_replication,
 *   requires_wiredtiger,
 * ]
 */
import {openBackupCursor} from "jstests/libs/backup_utils.js";
import {ReplSetTest} from "jstests/libs/replsettest.js";

const rst = new ReplSetTest({nodes: 1});
rst.startSet();
rst.initiate();

const dbName = "test";
const collName = "a";
const primary = rst.getPrimary();
let db = primary.getDB(dbName);

// Insert a document for the checkpoint.
assert.commandWorked(db.createCollection(collName));
assert.commandWorked(db.runCommand({insert: collName, documents: [{a: 10}]}));
const checkpoint = assert.commandWorked(db.adminCommand({fsync: 1}));

let checkpointTimestamp = checkpoint["$clusterTime"].clusterTime;

// Insert data to advance the stable timestamp.
assert.commandWorked(db.runCommand({insert: collName, documents: [{b: 12}]}));
assert.commandWorked(db.runCommand({insert: collName, documents: [{c: 20}]}));

// Opt out of taking the default checkpoint when we open the backup cursor as this test relies on
// reporting the checkpoint timestamp of the explicit checkpoint we took earlier.
let backupCursor = openBackupCursor(db, {takeCheckpoint: false});

// Assert that the metadata document exists.
assert(backupCursor.hasNext());
let doc = backupCursor.next();
// Get the backup timestamp to compare with the checkpoint stable.
let backupTimestamp = doc.metadata["checkpointTimestamp"];

// Ensure the backup cursor is open on the checkpoint we took earlier.
assert(timestampCmp(checkpointTimestamp, backupTimestamp) == 0);

backupCursor.close();
rst.stopSet();
