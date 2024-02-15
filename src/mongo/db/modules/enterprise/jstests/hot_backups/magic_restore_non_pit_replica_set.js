/*
 * Tests a non-PIT replica set restore with magic restore. The test does the following:
 *
 * Starts a replica set, inserts some initial data, and creates the backup data files
 * Writes a restore configuration object to a named pipe via the mongo shell
 * Starts a mongod with --magicRestore that parses the restore configuration and exits cleanly
 * Restart the initial replica set and assert the replica set config and data are what we expect.
 *
 * @tags: [
 *     requires_persistence,
 *     requires_wiredtiger
 * ]
 */

import {
    _copyFileHelper,
    _runMagicRestoreNode,
    _writeObjsToMagicRestorePipe,
    openBackupCursor
} from "jstests/libs/backup_utils.js";

// TODO SERVER-86034: Run on Windows machines once named pipe related failures are resolved.
if (_isWindows()) {
    jsTestLog("Temporarily skipping test for Windows variants. See SERVER-86034.");
    quit();
}

let rst = new ReplSetTest({nodes: 1});
rst.startSet();
rst.initiateWithHighElectionTimeout();

let primary = rst.getPrimary();
const dbName = "db";
const coll = "coll";

const db = primary.getDB(dbName);
// Insert some data to restore.
assert.commandWorked(db.getCollection(coll).insertMany([{a: 1}, {b: 2}, {c: 3}]));
const expectedDocs = db.getCollection(coll).find().toArray();

// Take the initial checkpoint.
assert.commandWorked(db.adminCommand({fsync: 1}));

const backupDbPath = primary.dbpath + "/backup";
resetDbpath(backupDbPath);
mkdir(backupDbPath + "/journal");

// TODO SERVER-82910: Insert additional data after the backup cursor is opened that will be
// truncated by magic restore. Open a backup cursor on the checkpoint.
const backupCursor = openBackupCursor(primary.getDB("admin"));

// Print the backup metadata document.
assert(backupCursor.hasNext());
jsTestLog(backupCursor.next());

while (backupCursor.hasNext()) {
    const doc = backupCursor.next();
    jsTestLog("Copying for backup: " + tojson(doc));
    _copyFileHelper(doc.filename, primary.dbpath, backupDbPath);
}
backupCursor.close();

let expectedConfig = assert.commandWorked(primary.adminCommand({replSetGetConfig: 1})).config;
rst.stopSet(/*signal=*/ null, /*forRestart=*/ true);

const objs = [{
    "nodeType": "replicaSet",
    "replicaSetConfig": expectedConfig,
    // TODO SERVER-82910: Set the 'maxCheckpointTs' to the checkpoint timestamp of the backup once a
    // full non-PIT replica set restore is implemented.
    "maxCheckpointTs": new Timestamp(1000, 0),
}];

_writeObjsToMagicRestorePipe(objs, MongoRunner.dataDir);
_runMagicRestoreNode(backupDbPath, MongoRunner.dataDir);

// Restart the original replica set.
rst.startSet({restart: true, dbpath: backupDbPath});

primary = rst.getPrimary();
const restoredConfig = assert.commandWorked(primary.adminCommand({replSetGetConfig: 1})).config;

// A new election occurred when the replica set restarted, so we must increment the term.
expectedConfig.term++;
assert.eq(expectedConfig, restoredConfig);

// TODO SERVER-82910: Ensure additional data inserted after the backup cursor was opened does not
// exist in restored replica set.
const restoredDocs = primary.getDB(dbName).getCollection(coll).find().toArray();
assert.eq(expectedDocs, restoredDocs);

rst.stopSet();
