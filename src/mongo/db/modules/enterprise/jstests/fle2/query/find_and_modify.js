/**
 * Test encrypted find and modify correctly rewrites the filter portion.
 *
 * @tags: [
 *  featureFlagFLE2,
 * ]
 */
(function() {
'use strict';

load("jstests/fle2/libs/encrypted_client_util.js");

const dbName = 'query_find_and_modify';
const dbTest = db.getSiblingDB(dbName);
dbTest.dropDatabase();

const collName = "test";

const client = new EncryptedClient(db.getMongo(), dbName);

assert.commandWorked(client.createEncryptionCollection(collName, {
    encryptedFields: {
        "fields": [
            {"path": "secretString", "bsonType": "string", "queries": {"queryType": "equality"}},
            {"path": "nested.secretInt", "bsonType": "int", "queries": {"queryType": "equality"}}
        ]
    }
}));

const edb = client.getDB();
const coll = edb[collName];
assert.commandWorked(
    coll.insert({_id: 1, secretString: "1337", nested: {secretInt: NumberInt(1337)}}));
assert.commandWorked(coll.insert({_id: 2, secretString: "5", nested: {secretInt: NumberInt(5)}}));

// Querying on a top-level encrypted field.
assert.commandWorked(coll.runCommand({
    findAndModify: collName,
    query: {secretString: "1337"},
    update: {$set: {is1337: true}},
}));

// Querying on a nested encrypted field.
assert.commandWorked(coll.runCommand({
    findAndModify: collName,
    query: {'nested.secretInt': NumberInt(5)},
    update: {$set: {isNestedFive: true}}
}));

client.assertEncryptedCollectionDocuments(collName, [
    {_id: 1, secretString: "1337", nested: {secretInt: NumberInt(1337)}, is1337: true},
    {_id: 2, secretString: "5", nested: {secretInt: NumberInt(5)}, isNestedFive: true}
]);

// Query over both a top level and nested encrypted field.
assert.commandWorked(coll.runCommand({
    findAndModify: collName,
    query: {secretString: "5", 'nested.secretInt': NumberInt(5)},
    update: {$set: {bothFive: true}}
}));

// Query over an encrypted field which matches no documents.
assert.commandWorked(coll.runCommand({
    findAndModify: collName,
    query: {secretString: "6"},
    update: {_id: 3, secretString: "6", nested: {secretInt: NumberInt(6)}},
    upsert: true
}));

client.assertEncryptedCollectionDocuments(collName, [
    {_id: 1, secretString: "1337", nested: {secretInt: NumberInt(1337)}, is1337: true},
    {
        _id: 2,
        secretString: "5",
        nested: {secretInt: NumberInt(5)},
        isNestedFive: true,
        bothFive: true
    },
    {_id: 3, secretString: "6", nested: {secretInt: NumberInt(6)}}
]);

// Query over one encrypted field and $unset another.
assert.commandWorked(coll.runCommand({
    findAndModify: collName,
    query: {secretString: "5"},
    update: {$unset: {'nested.secretInt': 1}}
}));
assert.eq([{_id: 2, secretString: "5", nested: {}, isNestedFive: true, bothFive: true}],
          coll.find({_id: 2}, {[kSafeContentField]: 0}).toArray());

// Verify that a user can specify a writeConcern without failing within the internally-created
// transaction.
assert.commandWorked(coll.runCommand({
    findAndModify: collName,
    query: {secretString: "6"},
    update: {$set: {isSix: true}},
    writeConcern: {w: 2},
}));
}());
