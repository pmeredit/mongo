/*
 * Tests a non-PIT replica set magic restore with transactions happening around restore time. Covers
 * the following test cases
 *
 * Can prepare a transaction on the source node and commit it on the restored node
 * Can commit a transaction on the source node and the results show up on the restored node
 * Cannot recover an unprepared uncommitted transaction from the source node on the restored node
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
    jsTestLog("Running non-PIT magic restore with insertHigherTermOplogEntry: " +
              insertHigherTermOplogEntry);
    let rst = new ReplSetTest({nodes: 1});
    rst.startSet();
    rst.initiateWithHighElectionTimeout();

    let primary = rst.getPrimary();
    const dbName = "db";
    const coll = "coll";

    let db = primary.getDB(dbName);

    // Insert a document to create the collection outside of the transaction.
    db.getCollection(coll).insert({'aa': 1});

    // Create a transaction that will be prepared before performing the restore. We will commit the
    // transaction on the source after the checkpoint timestamp, and on the restore node after
    // restore.
    let session1 = primary.startSession();
    const session1ID = session1.getSessionId();

    session1.startTransaction();

    let session1DB = session1.getDatabase(dbName);
    // Insert some data to restore. This data will be reflected in the restored node.
    ['a', 'b', 'c'].forEach(
        key => { assert.commandWorked(session1DB.getCollection(coll).insert({[key]: 1})); });
    const expectedDocs = session1DB.getCollection(coll).find().toArray();

    const prepareTimestamp = PrepareHelpers.prepareTransaction(session1);

    // Create a second transaction which will not be prepared before doing magic restore. The
    // contents of this transaction will not exist on the restore node.
    let session2 = primary.startSession();
    let session2DB = session2.getDatabase(dbName);
    session2.startTransaction();
    session2DB.getCollection("coll2").insert({'a': 1});

    // Create a third transaction that will be committed before the non-PIT restore. We will make
    // sure the document inserted during the transaction is reflected on the restore node.
    let session3 = primary.startSession();
    let session3DB = session3.getDatabase(dbName);
    session3.startTransaction();
    session3DB.getCollection("coll3").insert({'_id': 1});
    session3.commitTransaction_forTesting();

    const magicRestoreUtils = new MagicRestoreUtils({
        backupSource: primary,
        pipeDir: MongoRunner.dataDir,
        insertHigherTermOplogEntry: insertHigherTermOplogEntry
    });
    magicRestoreUtils.takeCheckpointAndOpenBackup();

    // These documents will be truncated by magic restore, since they were written after the backup
    // cursor was opened.
    ['e', 'f', 'g'].forEach(
        key => { assert.commandWorked(db.getCollection(coll).insert({[key]: 1})); });
    assert.eq(db.getCollection(coll).find().toArray().length, 4);

    // Commit the prepared transaction which will be truncated by non-PIT restore.
    PrepareHelpers.commitTransaction(session1, prepareTimestamp);

    // Commit the unprepared transaction which will be truncated by non-PIT restore.
    session2.commitTransaction_forTesting();

    magicRestoreUtils.assertOplogCountForNamespace(primary, dbName + "." + coll, 4, "i");
    let {entriesAfterBackup} = magicRestoreUtils.getEntriesAfterBackup(primary);
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
    };
    restoreConfiguration =
        magicRestoreUtils.appendRestoreToHigherTermThanIfNeeded(restoreConfiguration);

    magicRestoreUtils.writeObjsAndRunMagicRestore(
        restoreConfiguration, [], {"replSet": jsTestName()});

    // Restart the destination replica set.
    rst = new ReplSetTest({nodes: 1});
    rst.startSet({dbpath: magicRestoreUtils.getBackupDbPath(), noCleanData: true});

    primary = rst.getPrimary();
    db = primary.getDB(dbName);

    // Do a majority write to make sure the committed timestamp is up to date on the new node before
    // we commit the prepared transaction.
    assert.commandWorked(
        db.getCollection("update_timestamp").runCommand("insert", {documents: [{_id: 2}]}));

    // Make sure there is still two transactions in the transactions table. This is because the
    // entry in the transactions table is made durable when a transaction is prepared or committed.
    // The uncommitted transaction should not exist since it was aborted.
    const txns = primary.getDB('config')['transactions'].find().sort({state: 1}).toArray();
    assert.eq(txns.length, 2);
    assert.eq(txns[0].state, "committed");
    assert.eq(txns[1].state, "prepared");

    // Make sure we can successfully commit the recovered prepared transaction.
    session1 = PrepareHelpers.createSessionWithGivenId(primary, session1ID);
    session1DB = session1.getDatabase(dbName);
    // The transaction on this session should have a txnNumber of 0. We explicitly set this
    // since createSessionWithGivenId does not restore the current txnNumber in the shell.
    session1.setTxnNumber_forTesting(0);
    const txnNumber = session1.getTxnNumber_forTesting();

    jsTestLog("Committing the prepared transaction");
    assert.commandWorked(session1DB.adminCommand({
        commitTransaction: 1,
        commitTimestamp: prepareTimestamp,
        txnNumber: NumberLong(txnNumber),
        autocommit: false,
    }));

    const restoredDocs = primary.getDB(dbName).getCollection(coll).find().toArray();
    // The later 3 writes were truncated during magic restore.
    assert.eq(restoredDocs.length, 4);
    assert.eq(restoredDocs, expectedDocs);

    // Check the namespace where we had the uncommitted transaction.
    const uncommittedTransactionDocs =
        primary.getDB(dbName).getCollection("coll2").find().toArray();
    assert.eq(uncommittedTransactionDocs, []);

    // Check the namespace for the transaction committed before restore.
    const committedTransactionDocs = primary.getDB(dbName).getCollection("coll3").find().toArray();
    assert.eq(committedTransactionDocs, [{_id: 1}]);

    magicRestoreUtils.postRestoreChecks({
        node: primary,
        expectedConfig: expectedConfig,
        dbName: dbName,
        collName: coll,
        // The transaction entries are in an applyOps which does not get counted here.
        expectedOplogCountForNs: 1,
        opFilter: "i",
        expectedNumDocsSnapshot: 4,
    });

    rst.stopSet(null /* signal */, false /* forRestart */, {'skipValidation': true});
}

// insertHigherTermOplogEntry causes a no-op oplog entry insert with a higher term. This affects the
// stable timestamp on magic restore node shutdown.
for (const insertHigherTermOplogEntry of [false, true]) {
    runTest(insertHigherTermOplogEntry);
}
