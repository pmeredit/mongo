/**
 * Test encrypted delete works race under contention
 *
 * @tags: [
 * requires_fcv_60
 * ]
 */
import {EncryptedClient} from "jstests/fle2/libs/encrypted_client_util.js";
import {funWithArgs} from "jstests/libs/parallel_shell_helpers.js";

async function bgDeleteFunc(query) {
    const {EncryptedClient} = await import("jstests/fle2/libs/encrypted_client_util.js");
    let client = new EncryptedClient(db.getMongo(), "txn_contention_delete");
    while (true) {
        try {
            client.getDB().basic.edeleteOne(query);
            return;
        } catch (e) {
            assert.eq(e.code, ErrorCodes.WriteConflict, "Unexpected error: " + tojson(e));
            print("deleteOne(" + tojson(query) + ") threw a WriteConflict error. Retrying...");
        }
    }
}

function runTest(conn) {
    let dbName = 'txn_contention_delete';
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
    assert.commandWorked(edb.basic.einsert({_id: 2, "first": "mark", "last": "Marcus"}));
    client.assertEncryptedCollectionCounts("basic", 2, 2, 2);

    // Setup a failpoint that hangs in delete
    assert.commandWorked(
        db.adminCommand({configureFailPoint: "fleCrudHangDelete", mode: {times: 2}}));

    // Start two deletes. One will wait for the other
    let insertOne = startParallelShell(funWithArgs(bgDeleteFunc, {"last": "Marcus"}), conn.port);

    let insertTwo = startParallelShell(funWithArgs(bgDeleteFunc, {"last": "marco"}), conn.port);

    // Wait for the two parallel shells
    insertOne();
    insertTwo();

    // Assert we hit the failpoint at least twice
    assert.gte(
        assert
            .commandWorked(db.adminCommand({configureFailPoint: "fleCrudHangDelete", mode: "off"}))
            .count,
        2);

    // Verify the data on disk
    client.assertEncryptedCollectionCounts("basic", 0, 2, 2);
}

jsTestLog("ReplicaSet: Testing fle2 contention on delete");
{
    const rst = new ReplSetTest({nodes: 1});
    rst.startSet();

    rst.initiate();
    rst.awaitReplication();
    runTest(rst.getPrimary());
    rst.stopSet();
}

jsTestLog("Sharding: Testing fle2 contention on delete");
{
    const st = new ShardingTest({shards: 1, mongos: 1, config: 1});

    runTest(st.s);

    st.stop();
}
