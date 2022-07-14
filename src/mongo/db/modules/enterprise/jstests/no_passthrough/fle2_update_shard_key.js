/**
 * Tests that moving a document during a findAndModify from
 * one shard to another works correctly.
 *
 * @tags: [requires_sharding]
 *
 */

load("jstests/fle2/libs/encrypted_client_util.js");
load("src/mongo/db/modules/enterprise/jstests/fle/lib/utils.js");

(function() {
"use strict";

const st = new ShardingTest({
    shards: 2,
    mongos: 1,
    config: 1,
});

const dbName = "fle_update_shard_key";
const client = new EncryptedClient(st.s, dbName);

const testDb = client.getDB();
const shardedColl = testDb.getCollection("basic");
const nss = shardedColl.getFullName();

assert.commandWorked(client.createEncryptionCollection("basic", {
    encryptedFields:
        {"fields": [{"path": "name", "bsonType": "string", "queries": {"queryType": "equality"}}]}
}));

assert.commandWorked(testDb.adminCommand({shardCollection: nss, key: {id: 1}}));

assert.commandWorked(st.s.adminCommand({split: nss, middle: {id: 3}}));

assert.commandWorked(testDb.basic.insert({name: "Shreyas", id: 0}));
assert.commandWorked(testDb.basic.insert({name: "Bob", id: 6}));

assert.commandWorked(testDb.adminCommand({moveChunk: nss, find: {id: 0}, to: st.shard0.shardName}));
assert.commandWorked(testDb.adminCommand({moveChunk: nss, find: {id: 6}, to: st.shard1.shardName}));

assert.soon(() => {
    let result = testDb.runCommand(
        {findAndModify: "basic", query: {id: 0}, update: {name: "Shreyas", id: 5}});

    return result.ok === 1;
});

assert.soon(() => {
    let result = testDb.runCommand({
        update: "basic",
        updates: [{
            q: {id: 6},
            u: {name: "Bob", id: 1},
        }]
    });

    return result.ok === 1;
});

st.stop();
})();