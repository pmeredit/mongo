/**
 * Test that crud functions on unindexed fields works correctly.
 *
 * Since query for sharded findAndModify must contain the shard key,
 * we need to disable this test from mongos.
 *
 * @tags: [ assumes_against_mongod_not_mongos, requires_fcv_70 ]
 */

import {EncryptedClient} from "jstests/fle2/libs/encrypted_client_util.js";

const dbName = 'basic_crud_unindexed';
const dbTest = db.getSiblingDB(dbName);
dbTest.dropDatabase();

const client = new EncryptedClient(db.getMongo(), dbName);

assert.commandWorked(client.createEncryptionCollection(
    "basic_unindexed", {encryptedFields: {"fields": [{"path": "field", "bsonType": "string"}]}}));

const edb = client.getDB();

assert.commandWorked(edb.basic_unindexed.insert({"_id": 1, "field": "foo"}));

assert.commandWorked(edb.basic_unindexed.runCommand(
    {findAndModify: edb.basic_unindexed.getName(), query: {}, update: {$set: {"field": "bar"}}}));

client.assertEncryptedCollectionDocuments("basic_unindexed", [{"_id": 1, "field": "bar"}]);

assert.commandWorked(edb.basic_unindexed.update({}, {$set: {field: 'cool'}}));

client.assertEncryptedCollectionDocuments("basic_unindexed", [{"_id": 1, "field": "cool"}]);

assert.commandWorked(edb.basic_unindexed.deleteOne({}));

client.assertEncryptedCollectionDocuments("basic_unindexed", []);
