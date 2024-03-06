/**
 * Test encrypted find and modify with remove works
 *
 * @tags: [
 *   requires_fcv_70
 * ]
 */
load("jstests/fle2/libs/encrypted_client_util.js");

(function() {
'use strict';

const dbName = 'basic_find_and_modify_remove';
const dbTest = db.getSiblingDB(dbName);
dbTest.dropDatabase();

const client = new EncryptedClient(db.getMongo(), dbName);

assert.commandWorked(client.createEncryptionCollection("basic", {
    encryptedFields:
        {"fields": [{"path": "first", "bsonType": "string", "queries": {"queryType": "equality"}}]}
}));

const edb = client.getDB();
assert.commandWorked(
    edb.basic.insert({"_id": 1, "first": "mark", "last": "marco", "middle": "markus"}));
assert.commandWorked(
    edb.basic.insert({"_id": 2, "first": "Mark", "last": "Marcus", "middle": "markus"}));

print("EDC: " + tojson(dbTest.basic.find().toArray()));
client.assertEncryptedCollectionCounts("basic", 2, 2, 0, 2);

client.assertOneEncryptedDocumentFields("basic", {"last": "marco"}, {"first": "mark"});

//////////////////////////////////////////////////////////////////////////////////////////////////////
// Remove a document
assert.commandWorked(edb.basic.runCommand(
    {findAndModify: edb.basic.getName(), query: {"last": "marco"}, remove: true}));

client.assertEncryptedCollectionCounts("basic", 1, 2, 1, 2);

client.assertEncryptedCollectionDocuments("basic", [
    {"_id": 2, "first": "Mark", "last": "Marcus", "middle": "markus"},
]);

if (!client.useImplicitSharding) {
    //////////////////////////////////////////////////////////////////////////////////////////////////////
    // Remove a document by collation
    assert.commandWorked(edb.basic.runCommand({
        findAndModify: edb.basic.getName(),
        query: {"last": "marcus"},
        remove: true,
        collation: {locale: 'en_US', strength: 2}
    }));

    client.assertEncryptedCollectionCounts("basic", 0, 2, 2, 2);

    //////////////////////////////////////////////////////////////////////////////////////////////////////
    // FAIL: Remove and update the encrypted field

    // The FLE sharding code throws this directly. In replica sets, the regular mongod code throws
    // this with a different error code
    if (isMongos(db)) {
        assert.commandFailedWithCode(edb.basic.runCommand({
            findAndModify: edb.basic.getName(),
            query: {"last": "marco"},
            remove: true,
            update: {$set: {"first": "luke"}}
        }),
                                     6371401);
    }
}
}());
