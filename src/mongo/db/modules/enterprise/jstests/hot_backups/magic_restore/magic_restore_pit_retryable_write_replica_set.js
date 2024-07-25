/*
 * Tests a PIT replica set magic restore with a retryable write happening around restore time. Tests
 * that the write can be retried after the restore is complete and during PIT restore without
 * executing the command again. We know the writes are being retried and not executed again because
 * we check to make sure we do not generate additional oplog entries (and the writes would fail
 * since they are done with a specific _id).
 *
 * @tags: [
 *     requires_persistence,
 *     requires_wiredtiger
 * ]
 */

import {MagicRestoreUtils} from "jstests/libs/magic_restore_test.js";
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

    ['e', 'f', 'g'].forEach(
        key => { assert.commandWorked(db.getCollection(coll).insert({[key]: 1})); });

    // Retry the command while the backup cursor is open, this will not generate an oplog entry for
    // PIT restore.
    assert.commandWorked(db.runCommand(insertCommand));

    // Original insert + 'e', 'f', 'g'.
    assert.eq(db.getCollection(coll).find().toArray().length, 4);

    magicRestoreUtils.assertOplogCountForNamespace(primary, {ns: dbName + "." + coll, op: "i"}, 4);
    let {lastOplogEntryTs, entriesAfterBackup} = magicRestoreUtils.getEntriesAfterBackup(primary);
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

    const configTxnsPreRetry = primary.getDB("config").getCollection("transactions").findOne();

    // Retry the insert after restore is completed.
    assert.commandWorked(db.runCommand(insertCommand));

    const configTxnsPostRetry = primary.getDB("config").getCollection("transactions").findOne();
    assert.eq(configTxnsPreRetry, configTxnsPostRetry);
    assert.eq(configTxnsPostRetry._id.id, session1ID.id);
    assert.eq(configTxnsPostRetry.txnNum, NumberLong(0));

    const restoredDocs = primary.getDB(dbName).getCollection(coll).find().toArray();
    assert.eq(restoredDocs.length, 4);

    magicRestoreUtils.postRestoreChecks({
        node: primary,
        dbName: dbName,
        collName: coll,
        // Retried retryable writes do not generate additional oplog entries.
        expectedOplogCountForNs: 4,
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
