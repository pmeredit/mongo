/**
 * Tests that mongodecrypt can successfully decrypt an encrypted rollback file.
 *
 * @tags: [
 *   uses_transactions,
 *   uses_prepare_transaction,
 *   requires_fcv_44,
 *   uses_pykmip,
 *   incompatible_with_s390x,
 * ]
 */

import {arrayEq} from "jstests/aggregation/extras/utils.js";
import {PrepareHelpers} from "jstests/core/txns/libs/prepare_helpers.js";
import {ReplSetTest} from "jstests/libs/replsettest.js";
import {getUUIDFromListCollections} from "jstests/libs/uuid_util.js";
import {checkRollbackFiles} from "jstests/replsets/libs/rollback_files.js";
import {RollbackTest} from "jstests/replsets/libs/rollback_test.js";
import {
    killPyKMIPServer,
    platformSupportsGCM
} from "src/mongo/db/modules/enterprise/jstests/encryptdb/libs/helpers.js";

const testDir = "src/mongo/db/modules/enterprise/jstests/encryptdb/";
const kmipServerPort = "1337";

const kmipServerPid = _startMongoProgram("python", testDir + "kmip_server.py", kmipServerPort);
// Assert here that PyKMIP is compatible with the default Python version
assert(checkProgram(kmipServerPid));
// wait for PyKMIP, a KMIP server framework, to start
assert.soon(function() {
    return rawMongoProgramOutput().search("Starting connection service") !== -1;
});

const kmipParams = {
    enableEncryption: "",
    kmipServerName: "127.0.0.1",
    kmipPort: kmipServerPort,
    kmipServerCAFile: "jstests/libs/trusted-ca.pem",
    kmipClientCertificateFile: "jstests/libs/trusted-client.pem",
};

function runTest(cipherMode, keyID) {
    // run a mongod to make the KMIP server generates key with ID keyID
    let testParams = Object.merge(kmipParams, {encryptionCipherMode: cipherMode});
    const md = MongoRunner.runMongod(testParams);
    MongoRunner.stopMongod(md);
    testParams = Object.merge(testParams, {kmipKeyIdentifier: keyID});

    const replSet = new ReplSetTest({
        name: "decryptToolTest",
        nodes: 3,
        useBridge: true,
        nodeOptions: testParams,
    });
    let conns = replSet.startSet();
    let config = replSet.getReplSetConfig();
    config.members[2].priority = 0;
    config.settings = {chainingAllowed: false};
    replSet.initiateWithHighElectionTimeout(config);
    jsTestLog("Starting RollbackTest");
    const rollbackTest = new RollbackTest("decryptToolRollbackTest", replSet);
    jsTestLog("Created RollbackTest");

    const rollbackNode = rollbackTest.getPrimary();
    const testDB = rollbackNode.getDB("test");
    const collName = "rollback_prepare_transaction";
    const testColl = testDB.getCollection(collName);

    // We perform some operations on the collection aside from starting and preparing a transaction
    // in order to cause the count diff computed by replication to be non-zero.
    assert.commandWorked(testColl.insert({_id: "a"}));

    // Start two separate sessions for running transactions. On 'session1', we will run a prepared
    // transaction whose commit operation gets rolled back, and on 'session2', we will run a
    // prepared transaction whose prepare operation gets rolled back.
    const session1 = rollbackNode.startSession();
    const session1DB = session1.getDatabase(testDB.getName());
    const session1Coll = session1DB.getCollection(collName);

    const session2 = rollbackNode.startSession();
    const session2DB = session2.getDatabase(testDB.getName());
    const session2Coll = session2DB.getCollection(collName);

    // Prepare a transaction whose commit operation will be rolled back.
    session1.startTransaction();
    assert.commandWorked(session1Coll.insert({_id: "t2_a"}));
    assert.commandWorked(session1Coll.insert({_id: "t2_b"}));
    assert.commandWorked(session1Coll.insert({_id: "t2_c"}));
    let prepareTs = PrepareHelpers.prepareTransaction(session1);

    rollbackTest.transitionToRollbackOperations();

    // The following operations will be rolled-back.
    assert.commandWorked(testColl.insert({_id: "b"}));

    session2.startTransaction();
    assert.commandWorked(session2Coll.insert({_id: "t1"}));

    // Use w: 1 to simulate a prepare that will not become majority-committed.
    PrepareHelpers.prepareTransaction(session2, {w: 1});

    // Commit the transaction that was prepared before the common point.
    PrepareHelpers.commitTransaction(session1, prepareTs);

    // This is not exactly correct, but characterizes the current behavior of fastcount, which
    // includes the prepared but uncommitted transaction in the collection count.
    assert.eq(6, testColl.count());

    // Check the visible documents.
    arrayEq([{_id: "a"}, {_id: "b"}, {_id: "t2_a"}, {_id: "t2_b"}, {_id: "t2_c"}],
            testColl.find().toArray());

    rollbackTest.transitionToSyncSourceOperationsBeforeRollback();
    rollbackTest.transitionToSyncSourceOperationsDuringRollback();
    // Skip consistency checks so they don't conflict with the prepared transaction.
    rollbackTest.transitionToSteadyStateOperations({skipDataConsistencyChecks: true});

    // Both the regular insert and prepared insert should be rolled-back.
    assert.sameMembers([{_id: "a"}], testColl.find().toArray());

    // Confirm that the rollback wrote deleted documents to a file.
    const replTest = rollbackTest.getTestFixture();
    const expectedDocs = [{_id: "b"}, {_id: "t2_a"}, {_id: "t2_b"}, {_id: "t2_c"}];

    const uuid = getUUIDFromListCollections(testDB, collName);
    checkRollbackFiles(
        replTest.getDbPath(rollbackNode), testColl.getFullName(), uuid, expectedDocs);

    let adminDB = rollbackTest.getPrimary().getDB("admin");

    // Since we rolled back the prepared transaction on session2, retrying the prepareTransaction
    // command on this session should fail with a NoSuchTransaction error.
    assert.commandFailedWithCode(adminDB.adminCommand({
        prepareTransaction: 1,
        lsid: session2.getSessionId(),
        txnNumber: session2.getTxnNumber_forTesting(),
        autocommit: false
    }),
                                 ErrorCodes.NoSuchTransaction);

    // Allow the test to complete by aborting the left over prepared transaction.
    jsTestLog("Aborting the prepared transaction on session " + tojson(session1.getSessionId()));
    assert.commandWorked(adminDB.adminCommand({
        abortTransaction: 1,
        lsid: session1.getSessionId(),
        txnNumber: session1.getTxnNumber_forTesting(),
        autocommit: false
    }));

    const rollbackDir = replTest.getDbPath(rollbackNode) + "/rollback/";
    let rollbackFile = listFiles(listFiles(rollbackDir)[0].name)[0].name;
    let outputFile = replSet.getDbPath(conns[0]) + "/mongodecrypt_test_decrypted" +
        Math.floor(Math.random() * 1000000) + ".bson";

    jsTestLog("Running mongodecrypt");
    jsTestLog("Decrypting " + rollbackFile);
    jsTestLog("Saving bson file to " + outputFile);
    let decryptPid = _startMongoProgram("mongodecrypt",
                                        "--noConfirm",
                                        "--inputPath",
                                        rollbackFile,
                                        "--outputPath",
                                        outputFile,
                                        "--kmipServerName",
                                        "127.0.0.1",
                                        "--kmipPort",
                                        kmipServerPort,
                                        "--kmipServerCAFile",
                                        "jstests/libs/trusted-ca.pem",
                                        "--kmipClientCertificateFile",
                                        "jstests/libs/trusted-client.pem",
                                        "--kmipKeyIdentifier",
                                        keyID,
                                        "--encryptionCipherMode",
                                        cipherMode);

    const exitCode = waitProgram(decryptPid);
    // assert that mongodecrypt exited happily
    assert.eq(0, exitCode);
    // assert that the output it gave is valid BSON
    assert.doesNotThrow(function() {
        const bsonDump = _readDumpFile(outputFile);
        jsTestLog("Decrypted rollback file as:");
        jsTestLog(bsonDump);
        assert.gt(bsonDump.length, 0);
    });

    rollbackTest.stop();
}

runTest("AES256-CBC", "1");
if (platformSupportsGCM) {
    runTest("AES256-GCM", "2");
}
killPyKMIPServer(kmipServerPid);
