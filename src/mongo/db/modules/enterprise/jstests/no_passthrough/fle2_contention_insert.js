/**
 * Test encrypted insert works race under contention
 *
 * @tags: [
 *  featureFlagFLE2,
 * ]
 */
load("jstests/fle2/libs/encrypted_client_util.js");
load("jstests/libs/fail_point_util.js");

(function() {
'use strict';

if (!isFLE2Enabled()) {
    return;
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

    // Start two inserts. One will wait for the other
    let insertOne = startParallelShell(function() {
        load("jstests/fle2/libs/encrypted_client_util.js");
        let client = new EncryptedClient(db.getMongo(), "txn_contention_insert");
        assert.commandWorked(client.getDB().basic.insert({_id: 1, "first": "mark"}));
    }, conn.port);

    // Wait for the two parallel shells
    let insertTwo = startParallelShell(function() {
        load("jstests/fle2/libs/encrypted_client_util.js");
        let client = new EncryptedClient(db.getMongo(), "txn_contention_insert");
        assert.commandWorked(client.getDB().basic.insert({_id: 2, "first": "mark"}));
    }, conn.port);

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
