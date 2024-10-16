/*
 * Tests a PIT replica set magic restore with transactions happening around restore time. Covers the
 * following test cases
 *
 * Can prepare a transaction on the source node and apply commitTransaction during PIT restore
 * Can commit an unprepared transaction during PIT restore
 *
 * @tags: [
 *     requires_persistence,
 *     requires_wiredtiger
 * ]
 */

import {PrepareHelpers} from "jstests/core/txns/libs/prepare_helpers.js";
import {MagicRestoreUtils} from "jstests/libs/backup_utils.js";
import {ReplSetTest} from "jstests/libs/replsettest.js";

// TODO SERVER-86034: Run on Windows machines once named pipe related failures are resolved.
if (_isWindows()) {
    jsTestLog("Temporarily skipping test for Windows variants. See SERVER-86034.");
    quit();
}

function runTest(insertHigherTermOplogEntry) {
    jsTestLog("Running PIT magic restore with insertHigherTermOplogEntry: " +
              insertHigherTermOplogEntry);
    let rst = new ReplSetTest({nodes: 1});
    rst.startSet();
    rst.initiateWithHighElectionTimeout();

    let primary = rst.getPrimary();
    const dbName = "db";
    const coll = "coll";

    let db = primary.getDB(dbName);

    // Insert a document to create the collection outside of the transaction.
    db.getCollection(coll).insert({['aa']: 1});

    // Create a transaction that will be prepared before performing the restore then committed
    // during PIT restore.
    let session1 = primary.startSession();
    const session1ID = session1.getSessionId();

    session1.startTransaction();

    let session1DB = session1.getDatabase(dbName);
    // Insert some data to restore. This data will be reflected in the restored node.
    ['a', 'b', 'c'].forEach(
        key => { assert.commandWorked(session1DB.getCollection(coll).insert({[key]: 1})); });

    const prepareTimestamp = PrepareHelpers.prepareTransaction(session1);

    // Create a second transaction which will not be prepared and will be committed during the PIT
    // restore.
    let session2 = primary.startSession();
    let session2DB = session2.getDatabase(dbName);
    session2.startTransaction();
    session2DB.getCollection("coll2").insert({['_id']: 1});

    // Create a third transaction that will not be prepared and will be committed before the PIT
    // restore.
    let session3 = primary.startSession();
    let session3DB = session3.getDatabase(dbName);
    session3.startTransaction();
    session3DB.getCollection("coll3").insert({['_id']: 1});
    session3.commitTransaction_forTesting();

    const magicRestoreUtils = new MagicRestoreUtils({
        backupSource: primary,
        pipeDir: MongoRunner.dataDir,
        insertHigherTermOplogEntry: insertHigherTermOplogEntry
    });

    magicRestoreUtils.takeCheckpointAndOpenBackup();

    ['e', 'f', 'g'].forEach(
        key => { assert.commandWorked(db.getCollection(coll).insert({[key]: 1})); });
    assert.eq(db.getCollection(coll).find().toArray().length, 4);

    // Commit the prepared transaction during the PIT window.
    PrepareHelpers.commitTransaction(session1, prepareTimestamp);

    // Commit the unprepared transaction during the PIT window.
    session2.commitTransaction_forTesting();

    magicRestoreUtils.assertOplogCountForNamespace(primary, dbName + "." + coll, 4, "i");
    let {lastOplogEntryTs, entriesAfterBackup} = magicRestoreUtils.getEntriesAfterBackup(primary);
    // 'e', 'f', 'g' inserts + commitTransaction + commitTransaction.
    assert.eq(entriesAfterBackup.length, 5);

    magicRestoreUtils.copyFilesAndCloseBackup();

    let expectedConfig = assert.commandWorked(primary.adminCommand({replSetGetConfig: 1})).config;
    // The new node will be allocated a new port by the test fixture.
    expectedConfig.members[0].host = getHostName() + ":" + (Number(primary.port) + 2);
    rst.stopSet(
        null /* signal */, false /* forRestart */, {'skipValidation': true, noCleanData: true});

    let restoreConfiguration = {
        "nodeType": "replicaSet",
        "replicaSetConfig": expectedConfig,
        "maxCheckpointTs": magicRestoreUtils.getCheckpointTimestamp(),
        // Restore to the timestamp of the last oplog entry on the source cluster.
        "pointInTimeTimestamp": lastOplogEntryTs
    };
    restoreConfiguration =
        magicRestoreUtils.appendRestoreToHigherTermThanIfNeeded(restoreConfiguration);

    magicRestoreUtils.writeObjsAndRunMagicRestore(
        restoreConfiguration, entriesAfterBackup, {"replSet": jsTestName()});

    // Restart the destination replica set.
    rst = new ReplSetTest({nodes: 1});
    rst.startSet({dbpath: magicRestoreUtils.getBackupDbPath(), noCleanData: true});

    primary = rst.getPrimary();
    db = primary.getDB(dbName);

    // Do a majority write to make sure the committed timestamp is up to date on the new node before
    // we commit the prepared transaction.
    assert.commandWorked(
        db.getCollection("update_timestamp").runCommand("insert", {documents: [{_id: 2}]}));

    // Make sure all 3 transactions show up in config transactions as committed.
    const txns = primary.getDB('config')['transactions'].find().toArray();
    assert.eq(txns.length, 3);
    txns.forEach(txn => assert.eq(txn.state, "committed"));

    const restoredDocs = primary.getDB(dbName).getCollection(coll).find().toArray();
    assert.eq(restoredDocs.length, 7);

    // Check the namespace where we had the uncommitted transaction.
    const committedDuringPITDocs = primary.getDB(dbName).getCollection("coll2").find().toArray();
    assert.eq(committedDuringPITDocs, [{_id: 1}]);

    // Check the namespace for the transaction committed before restore.
    const committedBeforeRestoreDocs =
        primary.getDB(dbName).getCollection("coll3").find().toArray();
    assert.eq(committedBeforeRestoreDocs, [{_id: 1}]);

    magicRestoreUtils.postRestoreChecks({
        node: primary,
        expectedConfig: expectedConfig,
        dbName: dbName,
        collName: coll,
        // The transaction entries are in an applyOps which does not get counted here.
        expectedOplogCountForNs: 4,
        opFilter: "i",
        expectedNumDocsSnapshot: 7,
    });

    rst.stopSet(null /* signal */, false /* forRestart */, {'skipValidation': true});
}

// insertHigherTermOplogEntry causes a no-op oplog entry insert with a higher term. This affects the
// stable timestamp on magic restore node shutdown.
for (const insertHigherTermOplogEntry of [false, true]) {
    runTest(insertHigherTermOplogEntry);
}
