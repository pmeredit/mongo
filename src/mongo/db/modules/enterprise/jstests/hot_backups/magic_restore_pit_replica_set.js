/*
 * Tests a PIT replica set restore with magic restore. The test does the following:
 *
 * - Starts a replica set, inserts some initial data, and opens a backup cursor.
 * - Writes additional data that will be truncated by magic restore, since the writes occur after
 *   the checkpoint timestamp.
 * - Copies data files to the restore dbpath and closes the backup cursor.
 * - Writes a restore configuration object and the source oplog entries from after the checkpoint
 *   timestamp to a named pipe via the mongo shell.
 * - Starts a mongod with --magicRestore that parses the restore configuration, inserts and applies
 *   the additional oplog entries, and exits cleanly.
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

// TODO SERVER-87225: Enable fast count on validate when operations applied during a restore are
// counted correctly.
TestData.skipEnforceFastCountOnValidate = true;

function runTest(insertHigherTermOplogEntry) {
    const sourceCluster = new ReplSetTest({nodes: 1});
    sourceCluster.startSet();
    sourceCluster.initiateWithHighElectionTimeout();

    const sourcePrimary = sourceCluster.getPrimary();
    const dbName = "db";
    const coll = "coll";

    const sourceDb = sourcePrimary.getDB(dbName);
    // Insert some data to restore. This data will be reflected in the restored node.
    ['a', 'b', 'c'].forEach(
        key => { assert.commandWorked(sourceDb.getCollection(coll).insert({[key]: 1})); });

    // This timestamp will be used for a snapshot read.
    const snapshotTs = assert.commandWorked(sourcePrimary.adminCommand({replSetGetStatus: 1}))
                           .optimes.lastCommittedOpTime.ts;

    // Take the initial checkpoint.
    assert.commandWorked(sourcePrimary.adminCommand({fsync: 1}));

    const backupDbPath = MongoRunner.dataPath + "backup";
    resetDbpath(backupDbPath);
    mkdir(backupDbPath + "/journal");

    // Open a backup cursor on the checkpoint.
    const backupCursor = openBackupCursor(sourcePrimary.getDB("admin"));
    // Print the backup metadata document.
    assert(backupCursor.hasNext());
    const {metadata} = backupCursor.next();
    jsTestLog("Backup cursor metadata document: " + tojson(metadata));

    // These documents will be truncated by magic restore, since they were written after the backup
    // cursor was opened. We will pass these oplog entries to magic restore to perform a PIT
    // restore, so they will be reinserted and reflected in the final state of the data.
    ['e', 'f', 'g', 'h'].forEach(
        key => { assert.commandWorked(sourceDb.getCollection(coll).insert({[key]: 1})); });

    assert.eq(sourceDb.getCollection(coll).find().toArray().length, 7);

    let oplog = sourcePrimary.getDB("local").getCollection('oplog.rs');
    const entriesAfterBackup =
        oplog.find({ts: {$gt: metadata.checkpointTimestamp}}).sort({ts: 1}).toArray();
    assert.eq(entriesAfterBackup.length, 4);

    while (backupCursor.hasNext()) {
        const doc = backupCursor.next();
        jsTestLog("Copying for backup: " + tojson(doc));
        _copyFileHelper(doc.filename, sourcePrimary.dbpath, backupDbPath);
    }
    backupCursor.close();

    let expectedConfig =
        assert.commandWorked(sourcePrimary.adminCommand({replSetGetConfig: 1})).config;
    // The new node will be allocated a new port by the test fixture.
    expectedConfig.members[0].host = getHostName() + ":" + (Number(sourcePrimary.port) + 2);
    const restoreToHigherTermThan = 100;
    let lastOplogEntryTs = entriesAfterBackup[entriesAfterBackup.length - 1].ts;
    const objs = [{
        "nodeType": "replicaSet",
        "replicaSetConfig": expectedConfig,
        "maxCheckpointTs": metadata.checkpointTimestamp,
        // Restore to the timestamp of the last oplog entry on the source cluster.
        "pointInTimeTimestamp": lastOplogEntryTs
    }];
    if (insertHigherTermOplogEntry) {
        objs[0].restoreToHigherTermThan = NumberLong(restoreToHigherTermThan);
    }

    _writeObjsToMagicRestorePipe([...objs, ...entriesAfterBackup], MongoRunner.dataDir);
    _runMagicRestoreNode(backupDbPath, MongoRunner.dataDir);

    // Start a new replica set fixture on the dbpath.
    const destinationCluster = new ReplSetTest({nodes: 1});
    destinationCluster.startSet({dbpath: backupDbPath, noCleanData: true});

    const destPrimary = destinationCluster.getPrimary();
    const restoredConfig =
        assert.commandWorked(destPrimary.adminCommand({replSetGetConfig: 1})).config;
    // If we passed in a value for the 'restoreToHigherTermThan' field in the restore config, a
    // no-op oplog entry was inserted in the oplog with that term value + 100. On startup, the
    // replica set node sets its term to this value. A new election occurred when the replica set
    // restarted, so we must also increment the term by 1 regardless of if we passed in a higher
    // term value.
    const expectedTerm =
        insertHigherTermOplogEntry ? restoreToHigherTermThan + 101 : expectedConfig.term + 1;
    expectedConfig.term = expectedTerm;
    assert.eq(expectedConfig, restoredConfig);

    oplog = destPrimary.getDB("local").getCollection('oplog.rs');
    if (insertHigherTermOplogEntry) {
        const incrementTermEntry = oplog.findOne({op: "n", "o.msg": "restore incrementing term"});
        assert(incrementTermEntry);
        assert.eq(incrementTermEntry.t, restoreToHigherTermThan + 100);
        lastOplogEntryTs = incrementTermEntry.ts;
    }

    // Ensure that the last stable checkpoint taken used the timestamp from the top of the oplog. As
    // the timestamp is greater than 0, this means the magic restore took a stable checkpoint on
    // shutdown.
    const {lastStableRecoveryTimestamp} =
        assert.commandWorked(destPrimary.adminCommand({replSetGetStatus: 1}));
    assert(timestampCmp(lastStableRecoveryTimestamp, lastOplogEntryTs) == 0);

    const entries = oplog.find({op: "i", ns: dbName + "." + coll}).sort({ts: -1}).toArray();
    assert.eq(entries.length, 7);

    const minValid = destPrimary.getCollection('local.replset.minvalid').findOne();
    assert.eq(minValid, {_id: ObjectId("000000000000000000000000"), t: -1, ts: Timestamp(0, 1)});

    // The original node still maintains the history store, so point-in-time reads will succeed.
    let res = sourcePrimary.getDB("db").runCommand(
        {find: "coll", readConcern: {level: "snapshot", atClusterTime: snapshotTs}});
    assert.commandWorked(res);
    assert.eq(res.cursor.firstBatch.length, 3);

    // A restored node will not preserve any history. The oldest timestamp should be set to the top
    // of the oplog during recovery oplog application.
    res = destPrimary.getDB("db").runCommand(
        {find: "coll", readConcern: {level: "snapshot", atClusterTime: snapshotTs}});
    assert.commandFailedWithCode(res, ErrorCodes.SnapshotTooOld);

    // A snapshot read at the top of the oplog should succeed, since both the stable and oldest
    // timestamps are set to this value on magic restore shutdown.
    res = destPrimary.getDB("db").runCommand(
        {find: "coll", readConcern: {level: "snapshot", atClusterTime: lastOplogEntryTs}});
    assert.commandWorked(res);
    assert.eq(res.cursor.firstBatch.length, 7);

    let diff = DataConsistencyChecker.getDiff(
        sourcePrimary.getDB("db").getCollection("coll").find().sort({_id: 1}),
        destPrimary.getDB("db").getCollection("coll").find().sort({_id: 1}));

    assert.eq(diff,
              {docsWithDifferentContents: [], docsMissingOnFirst: [], docsMissingOnSecond: []});

    sourceCluster.stopSet();
    destinationCluster.stopSet();
}

// Run non-PIT restore twice, with one run performing a no-op oplog entry insert with a higher term.
// This affects the stable timestamp on magic restore node shutdown.
runTest(false /* insertHigherTermOplogEntry */);
runTest(true /* insertHigherTermOplogEntry */);
