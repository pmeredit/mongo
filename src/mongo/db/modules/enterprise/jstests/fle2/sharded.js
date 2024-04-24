/**
 * Test CRUD operations work on sharded collections.
 *
 * @tags: [
 * assumes_unsharded_collection,
 * requires_fcv_70
 * ]
 */
import {isMongos} from "jstests/concurrency/fsm_workload_helpers/server_types.js";
import {EncryptedClient} from "jstests/fle2/libs/encrypted_client_util.js";

if (!isMongos(db)) {
    quit();
}

const dbName = 'sharded';
const dbTest = db.getSiblingDB(dbName);
dbTest.dropDatabase();

const client = new EncryptedClient(db.getMongo(), dbName);

assert.commandWorked(client.createEncryptionCollection("basic", {
    encryptedFields:
        {"fields": [{"path": "a", "bsonType": "string", "queries": {"queryType": "equality"}}]}
}));

const edb = client.getDB();

assert(isMongos(edb));
assert.commandWorked(db.adminCommand({shardCollection: 'sharded.basic', key: {b: 'hashed'}}));

// Insert 10 documents.
for (let i = 0; i < 10; i++) {
    assert.commandWorked(edb.basic.einsert({
        _id: i,
        a: i.toString(),
        b: ((i + 1) % 10).toString(),
    }));
}

client.assertEncryptedCollectionCounts("basic", 10, 10, 10);
client.assertEncryptedCollectionDocuments("basic", [
    {_id: 0, a: "0", b: "1"},
    {_id: 1, a: "1", b: "2"},
    {_id: 2, a: "2", b: "3"},
    {_id: 3, a: "3", b: "4"},
    {_id: 4, a: "4", b: "5"},
    {_id: 5, a: "5", b: "6"},
    {_id: 6, a: "6", b: "7"},
    {_id: 7, a: "7", b: "8"},
    {_id: 8, a: "8", b: "9"},
    {_id: 9, a: "9", b: "0"},
]);

// Update all 10 documents.
for (let i = 0; i < 10; i++) {
    assert.commandWorked(edb.basic.eupdateOne({b: i.toString()}, {$set: {a: i.toString()}}));
}
client.assertEncryptedCollectionCounts("basic", 10, 20, 20);
client.assertEncryptedCollectionDocuments("basic", [
    {_id: 0, a: "1", b: "1"},
    {_id: 1, a: "2", b: "2"},
    {_id: 2, a: "3", b: "3"},
    {_id: 3, a: "4", b: "4"},
    {_id: 4, a: "5", b: "5"},
    {_id: 5, a: "6", b: "6"},
    {_id: 6, a: "7", b: "7"},
    {_id: 7, a: "8", b: "8"},
    {_id: 8, a: "9", b: "9"},
    {_id: 9, a: "0", b: "0"},
]);

// Modify all 10 documents.
for (let i = 0; i < 10; i++) {
    assert.commandWorked(edb.erunCommand({
        findAndModify: "basic",
        query: {b: i.toString()},
        update: {$set: {a: ((i + 1) % 10).toString()}},
    }));
}
client.assertEncryptedCollectionCounts("basic", 10, 30, 30);
client.assertEncryptedCollectionDocuments("basic", [
    {_id: 0, a: "2", b: "1"},
    {_id: 1, a: "3", b: "2"},
    {_id: 2, a: "4", b: "3"},
    {_id: 3, a: "5", b: "4"},
    {_id: 4, a: "6", b: "5"},
    {_id: 5, a: "7", b: "6"},
    {_id: 6, a: "8", b: "7"},
    {_id: 7, a: "9", b: "8"},
    {_id: 8, a: "0", b: "9"},
    {_id: 9, a: "1", b: "0"},
]);

// Delete 5 documents.
for (let i = 0; i < 10; i += 2) {
    assert.commandWorked(edb.basic.deleteOne({b: i.toString()}));
}
client.assertEncryptedCollectionCounts("basic", 5, 30, 30);
client.assertEncryptedCollectionDocuments("basic", [
    {_id: 0, a: "2", b: "1"},
    {_id: 2, a: "4", b: "3"},
    {_id: 4, a: "6", b: "5"},
    {_id: 6, a: "8", b: "7"},
    {_id: 8, a: "0", b: "9"},
]);
