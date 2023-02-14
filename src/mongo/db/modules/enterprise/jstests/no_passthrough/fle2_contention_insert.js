/**
 * Test encrypted insert works race under contention
 *
 * @tags: [
 * requires_fcv_60
 * ]
 */
load("jstests/fle2/libs/encrypted_client_util.js");
load("jstests/libs/fail_point_util.js");
load("jstests/libs/parallel_shell_helpers.js");

(function() {
'use strict';

function bgInsertFunc(doc) {
    load("jstests/fle2/libs/encrypted_client_util.js");
    let client = new EncryptedClient(db.getMongo(), "txn_contention_insert");
    while (true) {
        let res = client.getDB().basic.insert(doc);
        if (!res.hasWriteError()) {
            assert.writeOK(res);
            return;
        }
        assert.writeErrorWithCode(
            res, ErrorCodes.WriteConflict, "Unexpected error: " + tojson(res));
        print("insert(" + tojson(doc) + ") threw a WriteConflict error. Retrying...");
    }
}

function runTest(conn) {
    let dbName = 'txn_contention_insert';
    let db = conn.getDB(dbName);

    let client = new EncryptedClient(db.getMongo(), dbName);

    assert.commandWorked(client.createEncryptionCollection("basic", {
        encryptedFields: {
            "fields":
                [{"path": "first", "bsonType": "string", "queries": {"queryType": "equality"}}]
        }
    }));

    // Setup a failpoint that hangs in insert
    assert.commandWorked(
        db.adminCommand({configureFailPoint: "fleCrudHangInsert", mode: {times: 2}}));

    // TODO: SERVER-73303 remove once v2 is enabled by default
    let shellArgs = [];
    if (isFLE2ProtocolVersion2Enabled()) {
        shellArgs = ["--setShellParameter", "featureFlagFLE2ProtocolVersion2=true"];
    }

    // Start two inserts. One will wait for the other
    let insertOne = startParallelShell(
        funWithArgs(bgInsertFunc, {_id: 1, "first": "mark"}), conn.port, false, ...shellArgs);

    // Wait for the two parallel shells
    let insertTwo = startParallelShell(
        funWithArgs(bgInsertFunc, {_id: 2, "first": "mark"}), conn.port, false, ...shellArgs);

    // Wait for the two parallel shells
    insertOne();
    insertTwo();

    // Assert we hit the failpoint twice
    assert.eq(
        2,
        assert
            .commandWorked(db.adminCommand({configureFailPoint: "fleCrudHangInsert", mode: "off"}))
            .count);

    // Verify the data on disk
    client.assertEncryptedCollectionCounts("basic", 2, 2, 0, 2);

    client.assertOneEncryptedDocumentFields("basic", {"_id": 1}, {"first": "mark"});
    client.assertOneEncryptedDocumentFields("basic", {"_id": 2}, {"first": "mark"});

    client.assertEncryptedCollectionDocuments("basic", [
        {"_id": 1, "first": "mark"},
        {"_id": 2, "first": "mark"},
    ]);
}

jsTestLog("ReplicaSet: Testing fle2 contention on insert");
{
    const rst = new ReplSetTest({nodes: 1});
    rst.startSet();

    rst.initiate();
    rst.awaitReplication();
    runTest(rst.getPrimary());
    rst.stopSet();
}

jsTestLog("Sharding: Testing fle2 contention on insert");
{
    const st = new ShardingTest({shards: 1, mongos: 1, config: 1});

    runTest(st.s);

    st.stop();
}
}());
