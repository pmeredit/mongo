/**
 * Test encrypted retryable works
 *
 * @tags: [
 * assumes_unsharded_collection,
 * requires_non_retryable_commands,
 * assumes_read_preference_unchanged,
 * requires_capped,
 * assumes_unsharded_collection,
 * exclude_from_large_txns,
 * ]
 */
import {EncryptedClient} from "jstests/fle2/libs/encrypted_client_util.js";

function countOplogEntries(primaryConn) {
    var oplog = primaryConn.getDB('local').oplog.rs;

    return oplog.find(({"ns": "admin.$cmd", "o.applyOps.ns": "multi_retry.basic", "op": 'c'}))
        .itcount();
}

// primaryConn = connection to primary of shard in mongos otherwise primaryConn = conn
function runTest(conn, primaryConn) {
    let dbName = 'multi_retry';
    let collName = 'basic';

    let client = new EncryptedClient(conn, dbName);

    assert.commandWorked(client.createEncryptionCollection(collName, {
        encryptedFields: {
            "fields": [
                {"path": "first", "bsonType": "string", "queries": {"queryType": "equality"}},
                {"path": "middle", "bsonType": "string"},
                {"path": "aka", "bsonType": "string", "queries": {"queryType": "equality"}},
            ]
        }
    }));

    let edb = client.getDB();

    const lsid = UUID();

    // Test retryable writes for insert
    //
    let result = assert.commandWorked(edb.erunCommand({
        "insert": collName,
        documents: [
            {
                "_id": 1,
                "first": "dwayne",
                "middle": "elizondo mountain dew herbert",
                "aka": "president camacho"
            },
            {"_id": 2, "first": "bob", "middle": "belcher", "aka": "bobs burgers"},
            {"_id": 3, "first": "linda", "middle": "belcher", "aka": "linda"}
        ],
        ordered: false,
        lsid: {id: lsid},
        txnNumber: NumberLong(31)
    }));

    print("Here is the initial result");
    print(tojson(result));

    client.assertEncryptedCollectionCounts(collName, 3, 6, 6);

    let oplogCount = countOplogEntries(primaryConn);
    assert.eq(oplogCount, 3);

    let retryResult = assert.commandWorked(edb.erunCommand({
        "insert": collName,
        documents: [
            {
                "_id": 1,
                "first": "dwayne",
                "middle": "elizondo mountain dew herbert",
                "aka": "president camacho"
            },
            {"_id": 2, "first": "bob", "middle": "belcher", "aka": "bobs burgers"},
            {"_id": 3, "first": "linda", "middle": "belcher", "aka": "linda"}
        ],
        ordered: false,
        lsid: {id: lsid},
        txnNumber: NumberLong(31)
    }));

    print(tojson(retryResult));
    assert.eq(result.ok, retryResult.ok);
    assert.eq(result.n, retryResult.n);
    assert.eq(result.writeErrors, retryResult.writeErrors);
    assert.eq(result.writeConcernErrors, retryResult.writeConcernErrors);
    // Assert we did not write a second time to the oplog
    assert.eq(oplogCount, countOplogEntries(primaryConn));

    client.assertEncryptedCollectionCounts(collName, 3, 6, 6);
}

jsTestLog("ReplicaSet: Testing fle2 retryable writes");
{
    const rst = new ReplSetTest({nodes: 1});
    rst.startSet();

    rst.initiate();
    rst.awaitReplication();
    runTest(rst.getPrimary(), rst.getPrimary());
    rst.stopSet();
}

jsTestLog("Sharding: Testing fle2 retryable writes");
{
    const st = new ShardingTest({shards: 1, mongos: 1, config: 1});

    runTest(st.s, st.shard0);

    st.stop();
}
