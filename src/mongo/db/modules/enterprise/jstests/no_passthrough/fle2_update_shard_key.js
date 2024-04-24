/**
 * Tests that moving a document during a findAndModify from
 * one shard to another works correctly.
 *
 * @tags: [requires_sharding, requires_fcv_80]
 *
 */

import {EncryptedClient} from "jstests/fle2/libs/encrypted_client_util.js";
import {FeatureFlagUtil} from "jstests/libs/feature_flag_util.js";

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

assert.commandWorked(testDb.basic.einsert({name: "Shreyas", id: 0}));
assert.commandWorked(testDb.basic.einsert({name: "Bob", id: 6}));
assert.commandWorked(testDb.basic.einsert({name: "Kaitlin", id: 2}));

assert.commandWorked(testDb.adminCommand({moveChunk: nss, find: {id: 0}, to: st.shard0.shardName}));
assert.commandWorked(testDb.adminCommand({moveChunk: nss, find: {id: 6}, to: st.shard1.shardName}));

assert.soon(() => {
    let result = testDb.erunCommand(
        {findAndModify: "basic", query: {id: 0}, update: {name: "Shreyas", id: 5}});

    return result.ok === 1;
});

assert.soon(() => {
    let result = testDb.erunCommand({
        update: "basic",
        updates: [{
            q: {id: 6},
            u: {name: "Bob", id: 1},
        }]
    });

    return result.ok === 1;
});

assert.soon(() => {
    let result = testDb.eadminCommand({
        bulkWrite: 1,
        ops: [{
            update: 0,
            filter: {id: 2},
            updateMods: {name: "Shreyas", id: 7},
        }],
        nsInfo: [{ns: nss}]
    });

    if (result.ok !== 1 || result.nErrors !== 0 || result.cursor.firstBatch.length != 1) {
        return false;
    }

    const firstResult = result.cursor.firstBatch[0];
    return firstResult.n === 1 && firstResult.nModified === 1;
});

st.stop();
