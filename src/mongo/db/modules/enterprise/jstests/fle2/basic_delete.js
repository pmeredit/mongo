/**
 * Test encrypted delete works
 *
 * @tags: [
 * requires_fcv_60,
 * requires_non_retryable_writes,
 * ]
 */
load("jstests/fle2/libs/encrypted_client_util.js");

(function() {
'use strict';

let dbName = 'basic_delete';
let dbTest = db.getSiblingDB(dbName);
dbTest.dropDatabase();

let client = new EncryptedClient(db.getMongo(), dbName);

assert.commandWorked(client.createEncryptionCollection("basic", {
    encryptedFields:
        {"fields": [{"path": "first", "bsonType": "string", "queries": {"queryType": "equality"}}]}
}));

let edb = client.getDB();
assert.commandWorked(edb.basic.insert({"first": "mark", "last": "marco"}));
assert.commandWorked(edb.basic.insert({"first": "Mark", "last": "Marco"}));

print("EDC: " + tojson(dbTest.basic.find().toArray()));
client.assertEncryptedCollectionCounts("basic", 2, 2, 0, 2);

client.assertOneEncryptedDocumentFields("basic", {"last": "marco"}, {"first": "mark"});

// Delete a document
let res = assert.commandWorked(
    edb.runCommand({delete: "basic", deletes: [{"q": {"last": "marco"}, limit: 1}]}));
print(tojson(res));
assert.eq(res.n, 1);
client.assertWriteCommandReplyFields(res);

client.assertEncryptedCollectionCounts("basic", 1, 2, 1, 3);

// Delete nothing
res = assert.commandWorked(edb.basic.deleteOne({"last": "non-existent"}));
assert.eq(res.deletedCount, 0);

if (!client.useImplicitSharding) {
    // Delete a document by case-insensitive collation
    res = assert.commandWorked(
        edb.basic.deleteOne({"last": "marco"}, {collation: {locale: 'en_US', strength: 2}}));
    assert.eq(res.deletedCount, 1);

    client.assertEncryptedCollectionCounts("basic", 0, 2, 2, 4);
}

// Negative: Test bulk delete. Query analysis is throwing this error in the shell
res = assert.commandFailedWithCode(dbTest.basic.runCommand({
    delete: edb.basic.getName(),
    deletes: [
        {
            q: {"last": "marco"},
            limit: 1,
        },
        {
            q: {"last": "marco2"},
            limit: 1,
        },
    ],
    encryptionInformation: {schema: {}}
}),
                                   6371302);

// Negative: Delete many documents. Query analysis is throwing this error in the shell
res = assert.commandFailedWithCode(dbTest.basic.runCommand({
    delete: edb.basic.getName(),
    deletes: [
        {
            q: {"last": "marco2"},
            limit: 0,
        },
    ],
    encryptionInformation: {schema: {}}
}),
                                   6371303);
assert.throwsWithCode(() => {
    edb.basic.deleteMany({});
}, 6382800);

// Tests that will delete documents based on encrypted fields.
if (!client.useImplicitSharding) {
    const collName = "basic_query";
    assert.commandWorked(client.createEncryptionCollection(collName, {
        encryptedFields: {
            "fields":
                [{"path": "first", "bsonType": "string", "queries": {"queryType": "equality"}}]
        }
    }));
    const coll = edb[collName];
    assert.commandWorked(coll.insert({"first": "mark", "last": "marco"}));
    assert.commandWorked(coll.insert({"first": "Mark", "last": "Marco"}));
    client.assertEncryptedCollectionCounts(collName, 2, 2, 0, 2);

    // Delete a document by an encrypted field.
    res = assert.commandWorked(coll.deleteOne({"first": "mark"}));
    assert.eq(res.deletedCount, 1);
    client.assertEncryptedCollectionCounts(collName, 1, 2, 1, 3);

    // Try deleting a non-existent document.
    res = assert.commandWorked(coll.deleteOne({"first": "dev"}));
    assert.eq(res.deletedCount, 0);
    client.assertEncryptedCollectionCounts(collName, 1, 2, 1, 3);

    // Try deleting a non-existent combination of encrypted and non-encrypted fields, with a
    // case-insensitive collation.
    res =
        assert.commandWorked(coll.deleteOne({$and: [{"first": "Mark"}, {"last": "non-existent"}]}));
    assert.eq(res.deletedCount, 0);
    client.assertEncryptedCollectionCounts(collName, 1, 2, 1, 3);

    // Delete with a combination of encrypted and non-encrypted fields.
    res = assert.commandWorked(coll.deleteOne({$and: [{"first": "Mark"}, {"last": "Marco"}]}));
    assert.eq(res.deletedCount, 1);
    client.assertEncryptedCollectionCounts(collName, 0, 2, 2, 4);
}
}());
