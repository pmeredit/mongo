/*
 * Tests a non-PIT replica set magic restore with a retryable write happening around restore time.
 * Tests that the write can be retried after the restore is complete without executing the command
 * again. We know the writes are being retried and not executed again because we check to make sure
 * we do not generate additional oplog entries (and the writes would fail since they are done with a
 * specific _id).
 *
 * @tags: [
 *     requires_persistence,
 *     requires_wiredtiger,
 *     incompatible_with_windows_tls
 * ]
 */

import {MagicRestoreUtils} from "jstests/libs/magic_restore_test.js";
import {ReplSetTest} from "jstests/libs/replsettest.js";

function runTest(insertHigherTermOplogEntry) {
    jsTestLog("Running non-PIT magic restore with insertHigherTermOplogEntry: " +
              insertHigherTermOplogEntry);
    let rst = new ReplSetTest({nodes: 1});
    rst.startSet();
    rst.initiate();

    let primary = rst.getPrimary();
    const dbName = "db";
    const coll = "coll";

    let db = primary.getDB(dbName);

    // Create a session to use for the retryable write.
    const session1 = primary.startSession();
    const session1ID = session1.getSessionId();

    // Retryable write insert command.
    const insertCommand =
        {insert: coll, lsid: session1ID, txnNumber: NumberLong(0), documents: [{_id: 1, x: 1}]};

    assert.commandWorked(db.runCommand(insertCommand));

    // Retry the same command.
    assert.commandWorked(db.runCommand(insertCommand));

    const magicRestoreUtils = new MagicRestoreUtils({
        rst: rst,
        pipeDir: MongoRunner.dataDir,
        insertHigherTermOplogEntry: insertHigherTermOplogEntry
    });
    magicRestoreUtils.takeCheckpointAndOpenBackup();

    // These documents will be truncated by magic restore, since they were written after the backup
    // cursor was opened.
    ['e', 'f', 'g'].forEach(
        key => { assert.commandWorked(db.getCollection(coll).insert({[key]: 1})); });
    assert.commandWorked(db.runCommand(insertCommand));
    // Retryable insert + 'e' + 'f' + 'g'. Even though we retried multiple times it doesn't add
    // additional documents to the collection.
    assert.eq(db.getCollection(coll).find().toArray().length, 4);

    magicRestoreUtils.assertOplogCountForNamespace(primary, {ns: dbName + "." + coll, op: "i"}, 4);
    let {entriesAfterBackup} = magicRestoreUtils.getEntriesAfterBackup(primary);
    // 'e', 'f', 'g' inserts.
    assert.eq(entriesAfterBackup.length, 3);

    magicRestoreUtils.copyFilesAndCloseBackup();

    let expectedConfig = magicRestoreUtils.getExpectedConfig();
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

    const configTxnsPreRetry = primary.getDB("config").getCollection("transactions").findOne();

    // Re-run the retryable write.
    assert.commandWorked(db.runCommand(insertCommand));

    const configTxnsPostRetry = primary.getDB("config").getCollection("transactions").findOne();
    assert.eq(configTxnsPreRetry, configTxnsPostRetry);
    assert.eq(configTxnsPostRetry._id.id, session1ID.id);
    assert.eq(configTxnsPostRetry.txnNum, NumberLong(0));

    const restoredDocs = primary.getDB(dbName).getCollection(coll).find().toArray();
    // The later 3 writes were truncated during magic restore.
    assert.eq(restoredDocs.length, 1);

    magicRestoreUtils.postRestoreChecks({
        node: primary,
        dbName: dbName,
        collName: coll,
        // When a retryable write is retried it does not create an oplog entry so we should only
        // have the original entry.
        expectedOplogCountForNs: 1,
        opFilter: "i",
        expectedNumDocsSnapshot: 1,
    });

    rst.stopSet(null /* signal */, false /* forRestart */, {'skipValidation': true});
}

// insertHigherTermOplogEntry causes a no-op oplog entry insert with a higher term. This affects the
// stable timestamp on magic restore node shutdown.
for (const insertHigherTermOplogEntry of [false, true]) {
    runTest(insertHigherTermOplogEntry);
}
