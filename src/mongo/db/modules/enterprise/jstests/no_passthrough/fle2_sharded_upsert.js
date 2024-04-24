/**
 * Tests sharded upserts using shard & non-shard keys in the query filter.
 *
 * @tags: [requires_sharding]
 */
import {EncryptedClient} from "jstests/fle2/libs/encrypted_client_util.js";

function runTest(conn) {
    let dbName = 'basic_update';
    let collName = 'basic';
    let client = new EncryptedClient(conn, dbName);
    let edb = client.getDB();
    let ecoll = edb.getCollection(collName);

    assert.commandWorked(client.createEncryptionCollection(collName, {
        encryptedFields: {
            "fields":
                [{"path": "first", "bsonType": "string", "queries": {"queryType": "equality"}}]
        }
    }));

    assert.commandWorked(edb.adminCommand({enableSharding: edb.getName()}));

    let shardCollCmd = {
        shardCollection: edb.getName() + "." + collName,
        key: {_id: "hashed"},
        collation: {locale: "simple"}
    };
    assert.commandWorked(edb.adminCommand(shardCollCmd));

    assert.commandWorked(ecoll.einsert({"_id": 1, "first": "mark", "last": "marco"}));
    assert.commandWorked(ecoll.einsert({"_id": 2, "first": "Mark", "last": "Marcus"}));

    jsTestLog("Testing sharded upsert with shard key in query");
    let res = assert.commandWorked(ecoll.erunCommand({
        update: ecoll.getName(),
        updates: [{q: {"_id": 3}, u: {"last": "Marco", "first": "Luke"}, upsert: true}]
    }));
    assert.eq(res.n, 1);
    assert.eq(res.nModified, 0);
    assert.eq(res.upserted.length, 1);
    assert.eq(res.upserted[0]._id, 3);
    client.assertOneEncryptedDocumentFields(ecoll.getName(), {_id: 3}, {first: "Luke"});

    jsTestLog("Testing sharded upsert with non-shard key in query");
    res = assert.commandWorked(edb.basic.erunCommand({
        update: edb.basic.getName(),
        updates: [{q: {"last": "Mario"}, u: {"last": "Mario", "first": "Lukas"}, upsert: true}]
    }));
    assert.eq(res.n, 1);
    assert.eq(res.nModified, 0);
    assert.eq(res.upserted.length, 1);
    client.assertOneEncryptedDocumentFields(ecoll.getName(), {last: "Mario"}, {first: "Lukas"});
}

jsTestLog("Sharding: Testing sharded upserts");
{
    const st = new ShardingTest({shards: 2, mongos: 1, config: 1});
    runTest(st.s);
    st.stop();
}