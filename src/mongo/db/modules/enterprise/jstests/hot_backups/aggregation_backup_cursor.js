/**
 * Test the basic operation of a `$backupCursor` aggregation stage.
 *
 * @tags: [requires_persistence, requires_wiredtiger]
 */
import {getBackupCursorDB, openBackupCursor} from "jstests/libs/backup_utils.js";
import {
    validateReplicaSetBackupCursor
} from "src/mongo/db/modules/enterprise/jstests/hot_backups/libs/backup_cursor_helpers.js";

let conn = MongoRunner.runMongod();
const db = getBackupCursorDB(conn);

// Cursor timeout only occurs outside of sessions. Disabling implicit session use to allow for
// cursor timeout testing.
TestData.disableImplicitSessions = true;

assert(db.serverStatus()["storageEngine"].hasOwnProperty("backupCursorOpen"));
assert(!db.serverStatus()["storageEngine"]["backupCursorOpen"]);

// $backupCursor cannot be specified in view pipeline.
assert.commandFailedWithCode(db.createView('a', 'b', [{$backupCursor: {}}]),
                             ErrorCodes.InvalidNamespace);

let backupCursor = openBackupCursor(db);
// There should be about 14 files in total, but being precise would be unnecessarily fragile.
assert(db.serverStatus()["storageEngine"]["backupCursorOpen"]);
assert.gt(backupCursor.itcount(), 6);
assert(!backupCursor.isExhausted());
// Consuming all of the files does not close the backup cursor.
assert(db.serverStatus()["storageEngine"]["backupCursorOpen"]);
backupCursor.close();

assert(!db.serverStatus()["storageEngine"]["backupCursorOpen"]);

// Open a backup cursor. Use a small batch size to ensure a getMore retrieves additional
// results.
let response = openBackupCursor(db, null, {cursor: {batchSize: 2}});
assert.eq(`${db.getName()}.$cmd.aggregate`, response._ns);
assert.eq(2, response._batchSize);
let cursorId = response._cursorid;

response = assert.commandWorked(db.runCommand({getMore: cursorId, collection: "$cmd.aggregate"}));
// Sanity check the results.
assert.neq(0, response.cursor.id);
assert.gt(response.cursor.nextBatch.length, 4);

// The $backupCursor is a tailable cursor. Even though we've exhausted the results, running a
// getMore should succeed.
response = assert.commandWorked(db.runCommand({getMore: cursorId, collection: "$cmd.aggregate"}));
assert.neq(0, response.cursor.id);
assert.eq(0, response.cursor.nextBatch.length);

// Because the backup cursor is still open, trying to open a second cursor should fail.
assert.commandFailed(db.runCommand({aggregate: 1, pipeline: [{$backupCursor: {}}], cursor: {}}));

// Kill the backup cursor.
response =
    assert.commandWorked(db.runCommand({killCursors: "$cmd.aggregate", cursors: [cursorId]}));
assert.eq(1, response.cursorsKilled.length);
assert.eq(cursorId, response.cursorsKilled[0]);

// Open another backup cursor with a batch size of 0. The underlying backup cursor should be
// created.
response = openBackupCursor(db, {}, {cursor: {batchSize: 0}});
assert.neq(0, response._cursorid);
assert.eq(0, response._batch.length);

// Attempt to open a second backup cursor to demonstrate the original underlying cursor was
// opened.
assert.commandFailed(db.runCommand({aggregate: 1, pipeline: [{$backupCursor: {}}], cursor: {}}));

// Close the cursor to reset state.
response = assert.commandWorked(
    db.runCommand({killCursors: "$cmd.aggregate", cursors: [response._cursorid]}));
assert.eq(1, response.cursorsKilled.length);

// Set a failpoint which will generate a uassert after the backup cursor is open.
assert.commandWorked(
    db.adminCommand({configureFailPoint: "backupCursorErrorAfterOpen", mode: "alwaysOn"}));
assert.commandFailed(db.runCommand({aggregate: 1, pipeline: [{$backupCursor: {}}], cursor: {}}));
assert.commandWorked(
    db.adminCommand({configureFailPoint: "backupCursorErrorAfterOpen", mode: "off"}));

// Demonstrate query cursor timeouts will kill backup cursors, closing the underlying resources.
openBackupCursor(db, {}, {cursor: {}});
assert.commandWorked(db.adminCommand({setParameter: 1, cursorTimeoutMillis: 1}));
openBackupCursor(db, {}, {cursor: {}});
TestData.disableImplicitSessions = false;

MongoRunner.stopMongod(conn);

// Run a replica set to verify the contents of the `metadata` document.
let rst = new ReplSetTest({name: "aggBackupCursor", nodes: 1});
rst.startSet();
rst.initiate();

validateReplicaSetBackupCursor(rst.getPrimary().getDB("test"));

rst.stopSet();