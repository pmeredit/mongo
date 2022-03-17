/**
 * Test encrypted delete works
 *
 * @tags: [
 *  featureFlagFLE2,
 * ]
 */
load("jstests/fle2/libs/encrypted_client_util.js");

(function() {
'use strict';

if (!isFLE2Enabled()) {
    return;
}

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

// Delete a document by case-insensitive collation
res = assert.commandWorked(
    edb.basic.deleteOne({"last": "marco"}, {collation: {locale: 'en_US', strength: 2}}));
assert.eq(res.deletedCount, 1);

client.assertEncryptedCollectionCounts("basic", 0, 2, 2, 4);

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
}());
