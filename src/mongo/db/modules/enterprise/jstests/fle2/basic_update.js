/**
 * Test encrypted update works
 *
 * @tags: [
 *  featureFlagFLE2,
 * ]
 */
load("jstests/fle2/libs/encrypted_client_util.js");

(function() {
'use strict';

if (!isFLE2ShardingEnabled()) {
    return;
}

let dbName = 'basic_update';
let dbTest = db.getSiblingDB(dbName);
dbTest.dropDatabase();

let client = new EncryptedClient(db.getMongo(), dbName);

assert.commandWorked(client.createEncryptionCollection("basic", {
    encryptedFields:
        {"fields": [{"path": "first", "bsonType": "string", "queries": {"queryType": "equality"}}]}
}));

let edb = client.getDB();
assert.commandWorked(edb.basic.insert({"first": "mark", "last": "marco", "middle": "markus"}));
assert.commandWorked(edb.basic.insert({"first": "Mark", "last": "Marcus", "middle": "markus"}));

print("EDC: " + tojson(dbTest.basic.find().toArray()));
client.assertEncryptedCollectionCounts("basic", 2, 2, 0, 2);

let rawDoc = dbTest.basic.find().toArray()[0];
print(tojson(rawDoc));
client.assertOneEncryptedDocumentFields("basic", {"last": "marco"}, {"first": "mark"});

// Update an encrypted field in a document
let res =
    assert.commandWorked(edb.basic.updateOne({"last": "marco"}, {$set: {"first": "matthew"}}));
print(tojson(res));
assert.eq(res.matchedCount, 1);
assert.eq(res.modifiedCount, 1);

client.assertEncryptedCollectionCounts("basic", 2, 3, 1, 4);

client.assertOneEncryptedDocumentFields("basic", {"last": "marco"}, {"first": "matthew"});

// Remove the encrypted field
res = assert.commandWorked(edb.basic.updateOne({"last": "marco"}, {$unset: {"first": ""}}));
assert.eq(res.modifiedCount, 1);
rawDoc = dbTest.basic.find({"last": "marco"}).toArray()[0];
assert.eq(rawDoc["__safeContent__"], []);
assert(!rawDoc.hasOwnProperty("first"));

client.assertEncryptedCollectionCounts("basic", 2, 3, 2, 5);

// Add the encrypted field
res = assert.commandWorked(edb.basic.updateOne({"last": "marco"}, {$set: {"first": "luke"}}));
assert.eq(res.modifiedCount, 1);
client.assertOneEncryptedDocumentFields("basic", {"last": "marco"}, {"first": "luke"});

client.assertEncryptedCollectionCounts("basic", 2, 4, 2, 6);

// Update  a document by case-insensitive collation
res = assert.commandWorked(edb.basic.updateOne(
    {"last": "marcus"}, {$set: {"first": "john"}}, {collation: {locale: 'en_US', strength: 2}}));
assert.eq(res.modifiedCount, 1);

client.assertEncryptedCollectionCounts("basic", 2, 5, 3, 8);

// Update an unencrypted field in a document, expect no esc/ecc/ecoc changes
res = assert.commandWorked(edb.basic.updateOne({"last": "marco"}, {$set: {"middle": "matthew"}}));
print(tojson(res));
assert.eq(res.matchedCount, 1);
assert.eq(res.modifiedCount, 1);

client.assertEncryptedCollectionCounts("basic", 2, 5, 3, 8);

// Remove an unencrypted field in a document, expect no esc/ecc/ecoc changes
res = assert.commandWorked(edb.basic.updateOne({"last": "marco"}, {$unset: {"middle": ""}}));
print(tojson(res));
assert.eq(res.matchedCount, 1);
assert.eq(res.modifiedCount, 1);
client.assertEncryptedCollectionCounts("basic", 2, 5, 3, 8);

//  Negative: Test bulk update
assert.throwsWithCode(() => {
    edb.basic.bulkWrite([
        {updateOne: {"filter": {"char": "Eldon"}, "update": {$set: {"status": "Critical Injury"}}}},
        {
            updateOne:
                {"filter": {"char2": "Eldon"}, "update": {$set: {"status": "Critical Injury"}}}
        },

    ]);
}, 6371502);

// Negative: Update many documents. Query analysis is throwing this error in the shell
assert.throwsWithCode(() => {
    edb.basic.updateMany({x: 1}, {$set: {y: 1}});
}, 6329900);
}());
