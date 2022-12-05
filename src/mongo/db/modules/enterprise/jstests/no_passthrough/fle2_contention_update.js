/**
 * Test encrypted update works race under contention
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

function bgUpdateFunc(query, update) {
    load("jstests/fle2/libs/encrypted_client_util.js");
    let client = new EncryptedClient(db.getMongo(), "txn_contention_update");
    while (true) {
        try {
            client.getDB().basic.updateOne(query, update);
            return;
        } catch (e) {
            assert.eq(e.code, ErrorCodes.WriteConflict, "Unexpected error: " + tojson(e));
            print("updateOne(" + tojson(query) + ", " + tojson(update) +
                  ") threw a WriteConflict error. Retrying...");
        }
    }
}

function runTest(conn) {
    let dbName = 'txn_contention_update';
    let db = conn.getDB(dbName);

    let client = new EncryptedClient(db.getMongo(), dbName);

    assert.commandWorked(client.createEncryptionCollection("basic", {
        encryptedFields: {
            "fields":
                [{"path": "first", "bsonType": "string", "queries": {"queryType": "equality"}}]
        }
    }));

    const edb = client.getDB();
    assert.commandWorked(edb.basic.insert({_id: 1, "first": "mark", "last": "marco"}));
    assert.commandWorked(edb.basic.insert({_id: 2, "first": "Mark", "last": "Marcus"}));
    client.assertEncryptedCollectionCounts("basic", 2, 2, 0, 2);

    // Setup a failpoint that hangs in update
    assert.commandWorked(
        db.adminCommand({configureFailPoint: "fleCrudHangUpdate", mode: {times: 2}}));

    // Start two updates. One will wait for the other
    let insertOne = startParallelShell(
        funWithArgs(bgUpdateFunc, {"last": "Marcus"}, {$set: {"first": "matthew"}}), conn.port);

    let insertTwo = startParallelShell(
        funWithArgs(bgUpdateFunc, {"last": "marco"}, {$set: {"first": "matthew"}}), conn.port);

    // Wait for the two parallel shells
    insertOne();
    insertTwo();

    // Assert we hit the failpoint twice
    assert.gte(
        2,
        assert
            .commandWorked(db.adminCommand({configureFailPoint: "fleCrudHangUpdate", mode: "off"}))
            .count);

    // Verify the data on disk
    client.assertEncryptedCollectionCounts("basic", 2, 4, 2, 6);

    client.assertOneEncryptedDocumentFields("basic", {"_id": 1}, {"first": "matthew"});
    client.assertOneEncryptedDocumentFields("basic", {"_id": 2}, {"first": "matthew"});

    client.assertEncryptedCollectionDocuments("basic", [
        {"_id": 1, "first": "matthew", "last": "marco"},
        {"_id": 2, "first": "matthew", "last": "Marcus"},
    ]);
}

jsTestLog("ReplicaSet: Testing fle2 contention on update");
{
    const rst = new ReplSetTest({nodes: 1});
    rst.startSet();

    rst.initiate();
    rst.awaitReplication();
    runTest(rst.getPrimary());
    rst.stopSet();
}

jsTestLog("Sharding: Testing fle2 contention on update");
{
    const st = new ShardingTest({shards: 1, mongos: 1, config: 1});

    runTest(st.s);

    st.stop();
}
}());
