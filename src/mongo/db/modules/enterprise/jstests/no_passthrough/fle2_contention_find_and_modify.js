/**
 * Test encrypted find and modify works race under contention
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

function bgFindAndModifyFunc(query, update) {
    load("jstests/fle2/libs/encrypted_client_util.js");
    let client = new EncryptedClient(db.getMongo(), "txn_contention_find_and_modify");
    while (true) {
        let res = client.getDB().runCommand({findAndModify: "basic", query: query, update: update});
        if (res.ok === 0 || (res.hasOwnProperty("writeErrors") && res.writeErrors.length > 0)) {
            assert.commandFailedWithCode(
                res, ErrorCodes.WriteConflict, "Unexpected error: " + tojson(res));
            print("findAndModify(" + tojson(query) + ", " + tojson(update) +
                  ") threw a WriteConflict error. Retrying...");
            continue;
        }
        assert.commandWorked(res);
        return;
    }
}

function runTest(conn) {
    let dbName = 'txn_contention_find_and_modify';
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

    // Setup a failpoint that hangs in findAndModify
    assert.commandWorked(
        db.adminCommand({configureFailPoint: "fleCrudHangFindAndModify", mode: {times: 2}}));

    // Start two findAndModify. One will wait for the other
    let insertOne = startParallelShell(
        funWithArgs(bgFindAndModifyFunc, {"last": "Marcus"}, {$set: {"first": "matthew"}}),
        conn.port);

    let insertTwo = startParallelShell(
        funWithArgs(bgFindAndModifyFunc, {"last": "marco"}, {$set: {"first": "matthew"}}),
        conn.port);

    // Wait for the two parallel shells
    insertOne();
    insertTwo();

    // Assert we hit the failpoint twice
    assert.eq(2,
              assert
                  .commandWorked(db.adminCommand(
                      {configureFailPoint: "fleCrudHangFindAndModify", mode: "off"}))
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

jsTestLog("ReplicaSet: Testing fle2 contention on findAndModify");
{
    const rst = new ReplSetTest({nodes: 1});
    rst.startSet();

    rst.initiate();
    rst.awaitReplication();
    runTest(rst.getPrimary());
    rst.stopSet();
}

jsTestLog("Sharding: Testing fle2 contention on findAndModify");
{
    const st = new ShardingTest({shards: 1, mongos: 1, config: 1});

    runTest(st.s);

    st.stop();
}
}());
