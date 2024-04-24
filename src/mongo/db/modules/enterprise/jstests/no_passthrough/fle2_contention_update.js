/**
 * Test encrypted update works race under contention
 *
 * @tags: [
 * requires_fcv_60
 * ]
 */
import {EncryptedClient} from "jstests/fle2/libs/encrypted_client_util.js";
import {funWithArgs} from "jstests/libs/parallel_shell_helpers.js";

async function bgUpdateFunc(query, update) {
    const {EncryptedClient} = await import("jstests/fle2/libs/encrypted_client_util.js");
    let client = new EncryptedClient(db.getMongo(), "txn_contention_update");
    while (true) {
        try {
            client.getDB().basic.eupdateOne(query, update);
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
    assert.commandWorked(edb.basic.einsert({_id: 1, "first": "mark", "last": "marco"}));
    assert.commandWorked(edb.basic.einsert({_id: 2, "first": "Mark", "last": "Marcus"}));
    client.assertEncryptedCollectionCounts("basic", 2, 2, 2);

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

    // Assert we hit the failpoint at least twice
    assert.gte(
        assert
            .commandWorked(db.adminCommand({configureFailPoint: "fleCrudHangUpdate", mode: "off"}))
            .count,
        2);

    // Verify the data on disk
    client.assertEncryptedCollectionCounts("basic", 2, 4, 4);

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
