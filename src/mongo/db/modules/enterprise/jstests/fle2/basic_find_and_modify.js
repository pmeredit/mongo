/**
 * Test encrypted find and modify works
 *
 * @tags: [
 * assumes_unsharded_collection
 * ]
 */
load("jstests/fle2/libs/encrypted_client_util.js");

(function() {
'use strict';

const dbName = 'basic_find_and_modify';
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

// TODO: SERVER-72933 remove when v2 findAndModify works
if (isFLE2ProtocolVersion2Enabled()) {
    jsTest.log("Test skipped because featureFlagFLE2ProtocolVersion2 is enabled");
    return;
}

//////////////////////////////////////////////////////////////////////////////////////////////////////
// Update an encrypted field in a document
let res = assert.commandWorked(edb.basic.runCommand({
    findAndModify: edb.basic.getName(),
    query: {"last": "marco"},
    update: {$set: {"first": "matthew"}},
}));
print("RES:" + tojson(res));

client.assertEncryptedCollectionCounts("basic", 2, 3, 1, 4);

client.assertOneEncryptedDocumentFields("basic", {"last": "marco"}, {"first": "matthew"});

client.assertEncryptedCollectionDocuments("basic", [
    {"_id": 1, "first": "matthew", "last": "marco", "middle": "markus"},
    {"_id": 2, "first": "Mark", "last": "Marcus", "middle": "markus"},
]);

//////////////////////////////////////////////////////////////////////////////////////////////////////
// Remove an encrypted field
assert.commandWorked(edb.basic.runCommand({
    findAndModify: edb.basic.getName(),
    query: {"last": "marco"},
    update: {$unset: {"first": ""}}
}));
const rawDoc = dbTest.basic.find({"last": "marco"}).toArray()[0];
assert.eq(rawDoc[kSafeContentField], []);
assert(!rawDoc.hasOwnProperty("first"));

client.assertEncryptedCollectionCounts("basic", 2, 3, 2, 5);

client.assertEncryptedCollectionDocuments("basic", [
    {"_id": 1, "last": "marco", "middle": "markus"},
    {"_id": 2, "first": "Mark", "last": "Marcus", "middle": "markus"},
]);

//////////////////////////////////////////////////////////////////////////////////////////////////////
// Add the encrypted field
assert.commandWorked(edb.basic.runCommand({
    findAndModify: edb.basic.getName(),
    query: {"last": "marco"},
    update: {$set: {"first": "luke"}}
}));

client.assertOneEncryptedDocumentFields("basic", {"last": "marco"}, {"first": "luke"});

client.assertEncryptedCollectionCounts("basic", 2, 4, 2, 6);

client.assertEncryptedCollectionDocuments("basic", [
    {"_id": 1, "first": "luke", "last": "marco", "middle": "markus"},
    {"_id": 2, "first": "Mark", "last": "Marcus", "middle": "markus"},
]);

//////////////////////////////////////////////////////////////////////////////////////////////////////
// Update a document by case-insensitive collation
res = assert.commandWorked(edb.basic.runCommand({
    findAndModify: edb.basic.getName(),
    query: {"last": "marcus"},
    update: {$set: {"first": "john"}},
    collation: {locale: 'en_US', strength: 2}
}));

client.assertEncryptedCollectionCounts("basic", 2, 5, 3, 8);

client.assertOneEncryptedDocumentFields("basic", {"last": "Marcus"}, {"first": "john"});

client.assertEncryptedCollectionDocuments("basic", [
    {"_id": 1, "first": "luke", "last": "marco", "middle": "markus"},
    {"_id": 2, "first": "john", "last": "Marcus", "middle": "markus"},
]);

//////////////////////////////////////////////////////////////////////////////////////////////////////
// Update an unencrypted field in a document, expect no esc/ecc/ecoc changes
client.assertDocumentChanges("basic", [1], [0], () => {
    return assert.commandWorked(edb.basic.runCommand({
        findAndModify: edb.basic.getName(),
        query: {"last": "marco"},
        update: {$set: {"middle": "matthew"}}
    }));
});

client.assertEncryptedCollectionCounts("basic", 2, 5, 3, 8);

client.assertEncryptedCollectionDocuments("basic", [
    {"_id": 1, "first": "luke", "last": "marco", "middle": "matthew"},
    {"_id": 2, "first": "john", "last": "Marcus", "middle": "markus"},
]);

//////////////////////////////////////////////////////////////////////////////////////////////////////
// Remove an unencrypted field in a document, expect no esc/ecc/ecoc changes
client.assertDocumentChanges("basic", [1], [0], () => {
    return assert.commandWorked(edb.basic.runCommand({
        findAndModify: edb.basic.getName(),
        query: {"last": "marco"},
        update: {$unset: {"middle": ""}}
    }));
});

client.assertEncryptedCollectionCounts("basic", 2, 5, 3, 8);

client.assertEncryptedCollectionDocuments("basic", [
    {"_id": 1, "first": "luke", "last": "marco"},
    {"_id": 2, "first": "john", "last": "Marcus", "middle": "markus"},
]);

// Upsert to create a new document with an encrypted field.
client.assertDocumentChanges("basic", [0, 1], [2], () => {
    return assert.commandWorked(edb.basic.runCommand({
        findAndModify: edb.basic.getName(),
        query: {"_id": 3},
        update: {$set: {"first": "Mark", "middle": "Markus"}},
        upsert: true,
    }));
});
client.assertEncryptedCollectionDocuments("basic", [
    {"_id": 1, "first": "luke", "last": "marco"},
    {"_id": 2, "first": "john", "last": "Marcus", "middle": "markus"},
    {"_id": 3, "first": "Mark", "middle": "Markus"},
]);

//////////////////////////////////////////////////////////////////////////////////////////////////////
// Add a duplicate index entry
if (!client.useImplicitSharding) {
    assert.commandWorked(dbTest.basic.createIndex({"middle": 1}, {unique: true}));

    res = assert.commandFailed(edb.basic.runCommand({
        findAndModify: edb.basic.getName(),
        query: {"last": "marco"},
        update: {$set: {"middle": "markus"}}
    }));
    print(tojson(res));

    client.assertEncryptedCollectionCounts("basic", 3, 6, 3, 9);

    /////////////////////////////////////////////////////////////////////////////////////////////////////
    // Null Update
    res = assert.commandWorked(edb.basic.runCommand({
        findAndModify: edb.basic.getName(),
        query: {"last": "marky"},
        update: {$set: {"first": "matthew"}}
    }));

    client.assertEncryptedCollectionCounts("basic", 3, 7, 3, 10);
}
}());
