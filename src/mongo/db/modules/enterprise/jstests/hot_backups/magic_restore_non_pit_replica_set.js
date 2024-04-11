/*
 * Tests a non-PIT replica set restore with magic restore. The test does the following:
 *
 * - Starts a replica set, inserts some initial data, and creates the backup data files.
 * - Copies data files to the restore dbpath and closes the backup cursor.
 * - Writes a restore configuration object to a named pipe via the mongo shell.
 * - Starts a mongod with --magicRestore that parses the restore configuration and exits cleanly.
 * - Restarts the initial replica set and asserts the replica set config and data are what we
 *   expect.
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

function runTest(insertHigherTermOplogEntry) {
    let rst = new ReplSetTest({nodes: 1});
    rst.startSet();
    rst.initiateWithHighElectionTimeout();

    let primary = rst.getPrimary();
    const dbName = "db";
    const coll = "coll";

    const db = primary.getDB(dbName);
    // Insert some data to restore. This data will be reflected in the restored node.
    assert.commandWorked(db.getCollection(coll).insert({a: 1}));
    assert.commandWorked(db.getCollection(coll).insert({b: 2}));
    assert.commandWorked(db.getCollection(coll).insert({c: 3}));
    const expectedDocs = db.getCollection(coll).find().toArray();

    // Take the initial checkpoint.
    assert.commandWorked(db.adminCommand({fsync: 1}));

    const backupDbPath = primary.dbpath + "/backup";
    resetDbpath(backupDbPath);
    mkdir(backupDbPath + "/journal");

    // Open a backup cursor on the checkpoint.
    const backupCursor = openBackupCursor(primary.getDB("admin"));
    // Print the backup metadata document.
    assert(backupCursor.hasNext());
    const {metadata} = backupCursor.next();
    jsTestLog("Backup cursor metadata document: " + tojson(metadata));

    // These documents will be truncated by magic restore, since they were written after the backup
    // cursor was opened.
    assert.commandWorked(db.getCollection(coll).insert({e: 1}));
    assert.commandWorked(db.getCollection(coll).insert({f: 2}));
    assert.commandWorked(db.getCollection(coll).insert({g: 3}));
    assert.eq(db.getCollection(coll).find().toArray().length, 6);

    let oplog = primary.getDB("local").getCollection('oplog.rs');
    let entries = oplog.find({op: "i", ns: dbName + "." + coll}).sort({ts: -1}).toArray();
    assert.eq(entries.length, 6);
    // The most recent oplog entries will have timestamps strictly greater than the checkpoint
    // timestamp.
    entries.slice(0, 3).map((e) => assert(timestampCmp(e.ts, metadata.checkpointTimestamp) == 1));
    // The earlier oplog entries will have timestamps less than or equal to the checkpoint
    // timestamp.
    entries.slice(3, 6).map((e) => assert(timestampCmp(e.ts, metadata.checkpointTimestamp) <= 0));

    while (backupCursor.hasNext()) {
        const doc = backupCursor.next();
        jsTestLog("Copying for backup: " + tojson(doc));
        _copyFileHelper(doc.filename, primary.dbpath, backupDbPath);
    }
    backupCursor.close();

    let expectedConfig = assert.commandWorked(primary.adminCommand({replSetGetConfig: 1})).config;
    rst.stopSet(/*signal=*/ null, /*forRestart=*/ true);

    const restoreToHigherTermThan = 100;
    const objs = [{
        "nodeType": "replicaSet",
        "replicaSetConfig": expectedConfig,
        "maxCheckpointTs": metadata.checkpointTimestamp,
    }];

    if (insertHigherTermOplogEntry) {
        objs[0].restoreToHigherTermThan = NumberLong(restoreToHigherTermThan);
    }

    _writeObjsToMagicRestorePipe(objs, MongoRunner.dataDir);
    _runMagicRestoreNode(backupDbPath, MongoRunner.dataDir);

    // Restart the original replica set.
    rst.startSet({restart: true, dbpath: backupDbPath});

    primary = rst.getPrimary();
    const restoredConfig = assert.commandWorked(primary.adminCommand({replSetGetConfig: 1})).config;

    // If we passed in a value for the 'restoreToHigherTermThan' field in the restore config, a
    // no-op oplog entry was inserted in the oplog with that term value + 100. On startup, the
    // replica set node sets its term to this value. A new election occurred when the replica set
    // restarted, so we must also increment the term by 1 regardless of if we passed in a higher
    // term value.
    const expectedTerm =
        insertHigherTermOplogEntry ? restoreToHigherTermThan + 101 : expectedConfig.term + 1;
    expectedConfig.term = expectedTerm;
    assert.eq(expectedConfig, restoredConfig);

    const restoredDocs = primary.getDB(dbName).getCollection(coll).find().toArray();
    // The later 3 writes were truncated during magic restore.
    assert.eq(restoredDocs.length, 3);
    assert.eq(restoredDocs, expectedDocs);

    oplog = primary.getDB("local").getCollection('oplog.rs');
    entries = oplog.find({op: "i", ns: dbName + "." + coll}).sort({ts: -1}).toArray();
    assert.eq(entries.length, 3);

    let lastStableCheckpointTs = metadata.checkpointTimestamp;
    if (insertHigherTermOplogEntry) {
        const incrementTermEntry = oplog.findOne({op: "n", "o.msg": "restore incrementing term"});
        assert(incrementTermEntry);
        assert.eq(incrementTermEntry.t, restoreToHigherTermThan + 100);
        // If we've inserted a no-op oplog entry with a higher term during magic restore, we'll have
        // updated the stable timestamp.
        lastStableCheckpointTs = incrementTermEntry.ts;
    }

    // Ensure that the last stable checkpoint taken used the timestamp from the top of the oplog. As
    // the timestamp is greater than 0, this means the magic restore took a stable checkpoint on
    // shutdown.
    const {lastStableRecoveryTimestamp} =
        assert.commandWorked(primary.adminCommand({replSetGetStatus: 1}));
    assert(timestampCmp(lastStableRecoveryTimestamp, lastStableCheckpointTs) == 0);

    // A restored node will not preserve any history. The oldest timestamp should be set to the
    // stable timestamp at the end of a non-PIT restore.
    let res = primary.getDB("db").runCommand({
        find: "coll",
        readConcern: {
            level: "snapshot",
            atClusterTime: Timestamp(lastStableRecoveryTimestamp.getTime() - 1,
                                     lastStableRecoveryTimestamp.getInc())
        }
    });
    assert.commandFailedWithCode(res, ErrorCodes.SnapshotTooOld);

    // A snapshot read at the last stable timestamp should succeed.
    res = primary.getDB("db").runCommand({
        find: "coll",
        readConcern: {level: "snapshot", atClusterTime: lastStableRecoveryTimestamp}
    });
    assert.commandWorked(res);
    assert.eq(res.cursor.firstBatch.length, 3);

    rst.stopSet();
}

// Run non-PIT restore twice, with one run performing a no-op oplog entry insert with a higher term.
// This affects the stable timestamp on magic restore node shutdown.
runTest(false /* insertHigherTermOplogEntry */);
runTest(true /* insertHigherTermOplogEntry */);
