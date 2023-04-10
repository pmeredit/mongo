/**
 * Test encrypted find and modify fails
 *
 * @tags: [
 *   requires_fcv_70
 * ]
 */
load("jstests/fle2/libs/encrypted_client_util.js");

(function() {
'use strict';

const dbName = 'find_and_modify_collation';
const dbTest = db.getSiblingDB(dbName);
dbTest.dropDatabase();

const client = new EncryptedClient(db.getMongo(), dbName);

assert.commandWorked(client.createEncryptionCollection("basic", {
    encryptedFields:
        {"fields": [{"path": "first", "bsonType": "string", "queries": {"queryType": "equality"}}]}
}));

const edb = client.getDB();
assert.commandWorked(edb.basic.insert({"_id": 1, "first": "mark", "last": "Markus"}));
assert.commandWorked(edb.basic.insert({"_id": 2, "first": "Mark", "last": "Marco"}));

print("EDC: " + tojson(dbTest.basic.find().toArray()));
client.assertEncryptedCollectionCounts("basic", 2, 2, 2);

client.assertEncryptedCollectionDocuments("basic", [
    {"_id": 1, "first": "mark", "last": "Markus"},
    {"_id": 2, "first": "Mark", "last": "Marco"},
]);

// Verify new: true is not allowed
assert.commandFailedWithCode(edb.basic.runCommand({
    findAndModify: edb.basic.getName(),
    new: true,
    query: {"last": "markus"},
    update: {$set: {"first": "Marky"}}
}),
                             6371402);

// Verify fields is not allowed
assert.commandFailedWithCode(edb.basic.runCommand({
    findAndModify: edb.basic.getName(),
    fields: {_id: 1},
    query: {"last": "markus"},
    update: {$set: {"first": "Marky"}}
}),
                             6371408);
}());
