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

import {_copyFileHelper, openBackupCursor} from "jstests/libs/backup_utils.js";

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

// TODO SERVER-85665: Move logic for writing objects into named pipe into backup_utils.js.
const pipeName = "magic_restore_named_pipe";
const pipePrefix = MongoRunner.dataDir + "/tmp/";
let pipePath;
if (_isWindows()) {
    // "//./pipe/" is the required path start of all named pipes on Windows.
    pipePath = "//./pipe/" + pipeName;
} else {
    assert(mkdir(pipePrefix).created);
    pipePath = pipePrefix + pipeName;
}

// On Windows, the 'pipePrefix' parameter is ignored.
_writeTestPipeObjects(pipeName, objs.length, objs, pipePrefix);
// Creating the named pipe is async, so we should wait until it exists.
assert.soon(() => fileExists(pipePath));

// Magic restore will exit the mongod process cleanly. 'runMongod' may acquire a connection to
// mongod before it exits, and so we wait for the process to exit in the 'assert.soon' below. If
// mongod exits before we acquire a connection, 'conn' will be null. In this case, if mongod exits
// with non-zero exit code, the runner will throw a StopError.
const conn = MongoRunner.runMongod(
    {dbpath: backupDbPath, noCleanData: true, magicRestore: "", env: {namedPipeInput: pipePath}});
if (conn) {
    assert.soon(() => {
        const res = checkProgram(conn.pid);
        return !res.alive && res.exitCode == MongoRunner.EXIT_CLEAN;
    }, "Expected magic restore to exit mongod cleanly");
}

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
