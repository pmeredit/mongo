/**
 * Test that crud functions on unindexed fields works correctly.
 *
 * Since query for sharded findAndModify must contain the shard key,
 * we need to disable this test from mongos.
 *
 * @tags: [ assumes_against_mongod_not_mongos, requires_fcv_71 ]
 */

import {EncryptedClient, kSafeContentField} from "jstests/fle2/libs/encrypted_client_util.js";

const dbName = 'basic_crud_unindexed';
const dbTest = db.getSiblingDB(dbName);
dbTest.dropDatabase();

const client = new EncryptedClient(db.getMongo(), dbName);

assert.commandWorked(client.createEncryptionCollection(
    "basic_unindexed", {encryptedFields: {"fields": [{"path": "field", "bsonType": "string"}]}}));

const edb = client.getDB();

assert.commandWorked(edb.basic_unindexed.einsert({"_id": 1, "field": "foo"}));

assert.commandWorked(edb.basic_unindexed.erunCommand(
    {findAndModify: edb.basic_unindexed.getName(), query: {}, update: {$set: {"field": "bar"}}}));

client.assertEncryptedCollectionDocuments("basic_unindexed", [{"_id": 1, "field": "bar"}]);

assert.commandWorked(edb.basic_unindexed.eupdateOne({}, {$set: {field: 'cool'}}));

client.assertEncryptedCollectionDocuments("basic_unindexed", [{"_id": 1, "field": "cool"}]);

assert.commandWorked(edb.basic_unindexed.edeleteOne({}));

client.assertEncryptedCollectionDocuments("basic_unindexed", []);

// Test that crud functions on normal documents in encrypted collections works correctly.
assert.commandWorked(client.createEncryptionCollection("basic", {
    encryptedFields: {
        "fields": [
            {"path": "age", "bsonType": "int", "queries": {"queryType": "equality"}},
            {
                path: "name",
                bsonType: "string",
            }
        ]
    }
}));

{
    assert.commandWorked(edb.basic.einsert({"_id": 1, "field": "foo"}));

    assert.eq(edb.basic.find({_id: 1}).toArray()[0][kSafeContentField], undefined);

    client.assertEncryptedCollectionDocuments("basic", [{"_id": 1, "field": "foo"}]);

    assert.commandWorked(edb.basic.eupdateOne({_id: 1}, {$set: {field: "bar"}}));

    assert.eq(edb.basic.find({_id: 1}).toArray()[0][kSafeContentField], undefined);

    client.assertEncryptedCollectionDocuments("basic", [{"_id": 1, "field": "bar"}]);

    assert.commandWorked(edb.basic.erunCommand(
        {findAndModify: edb.basic.getName(), query: {_id: 1}, update: {$set: {field: "rho"}}}));

    client.assertEncryptedCollectionDocuments("basic", [{"_id": 1, "field": "rho"}]);

    assert.commandWorked(edb.basic.deleteOne({}));

    client.assertEncryptedCollectionDocuments("basic", []);
}

// Test that modifying an unencrypted field in a document still works.
{
    assert.commandWorked(edb.basic.einsert({"_id": 1, "field": "foo", "age": NumberInt(12)}));

    client.assertEncryptedCollectionDocuments("basic",
                                              [{"_id": 1, "field": "foo", "age": NumberInt(12)}]);

    assert.commandWorked(edb.basic.eupdateOne({_id: 1}, {$set: {field: "bar"}}));

    client.assertEncryptedCollectionDocuments("basic",
                                              [{"_id": 1, "field": "bar", "age": NumberInt(12)}]);

    assert.commandWorked(edb.basic.deleteOne({}));

    client.assertEncryptedCollectionDocuments("basic", []);
}
