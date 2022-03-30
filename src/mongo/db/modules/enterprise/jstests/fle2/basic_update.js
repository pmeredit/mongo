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

if (!isFLE2Enabled()) {
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
client.assertOneEncryptedDocumentFields("basic", {"last": "Marcus"}, {"first": "john"});

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

// Update an unencrypted field in a document, but match no documents
// expect writes to esc,ecoc
res = assert.commandWorked(edb.basic.updateOne({"last": "marky"}, {$set: {"first": "matthew"}}));
print(tojson(res));
assert.eq(res.matchedCount, 0);
assert.eq(res.modifiedCount, 0);

client.assertEncryptedCollectionCounts("basic", 2, 6, 3, 9);

//  Negative: Test bulk update. Send raw unencrypted commands to bypass query analysis
res = assert.commandFailedWithCode(dbTest.basic.runCommand({
    update: edb.basic.getName(),
    updates: [
        {q: {"last": "marco"}, u: {$set: {status: "Modified", comments: ["$misc1", "$misc2"]}}},
        {q: {"last": "marco2"}, u: {$set: {status: "Modified", comments: ["$misc3", "$misc2"]}}}
    ],
    encryptionInformation: {schema: {}}
}),
                                   6371502);

// Negative: Update many documents. Send raw unencrypted commands to bypass query analysis
res = assert.commandFailedWithCode(dbTest.basic.runCommand({
    update: edb.basic.getName(),
    updates: [
        {
            q: {"last": "marco"},
            u: {$set: {status: "Modified", comments: ["$misc1", "$misc2"]}},
            multi: true,
        },
    ],
    encryptionInformation: {schema: {}}
}),
                                   6371503);

// TODO - NULL Update

// Test updates with encrypted fields in the query portion.

const collName = "basic_query";
assert.commandWorked(client.createEncryptionCollection(collName, {
    encryptedFields:
        {"fields": [{"path": "first", "bsonType": "string", "queries": {"queryType": "equality"}}]}
}));

edb = client.getDB();
const coll = edb[collName];
assert.commandWorked(coll.insert({"first": "mark", "last": "marco", "middle": "markus"}));
assert.commandWorked(coll.insert({"first": "Mark", "last": "Marcus", "middle": "markus"}));

print("EDC: " + tojson(dbTest[collName].find().toArray()));
client.assertEncryptedCollectionCounts(collName, 2, 2, 0, 2);

// Update an encrypted field in a document
res = assert.commandWorked(coll.updateOne({"first": "mark"}, {$set: {"first": "matthew"}}));
print(tojson(res));
assert.eq(res.matchedCount, 1);
assert.eq(res.modifiedCount, 1);

client.assertEncryptedCollectionCounts(collName, 2, 3, 1, 4);

client.assertOneEncryptedDocumentFields(collName, {"last": "marco"}, {"first": "matthew"});

// Remove the encrypted field
res = assert.commandWorked(coll.updateOne({"first": "matthew"}, {$unset: {"first": ""}}));
assert.eq(res.modifiedCount, 1);
rawDoc = dbTest[collName].find({"last": "marco"}).toArray()[0];
assert.eq(rawDoc["__safeContent__"], []);
assert(!rawDoc.hasOwnProperty("first"));

client.assertEncryptedCollectionCounts(collName, 2, 3, 2, 5);

// Add the encrypted field
res = assert.commandWorked(coll.updateOne({"last": "marco"}, {$set: {"first": "luke"}}));
assert.eq(res.modifiedCount, 1);
client.assertOneEncryptedDocumentFields(collName, {"last": "marco"}, {"first": "luke"});

client.assertEncryptedCollectionCounts(collName, 2, 4, 2, 6);

// Update an unencrypted field in a document, expect no esc/ecc/ecoc changes
res = assert.commandWorked(coll.updateOne({"first": "luke"}, {$set: {"middle": "matthew"}}));
print(tojson(res));
assert.eq(res.matchedCount, 1);
assert.eq(res.modifiedCount, 1);

client.assertEncryptedCollectionCounts(collName, 2, 4, 2, 6);

// Remove an unencrypted field in a document, expect no esc/ecc/ecoc changes
res = assert.commandWorked(coll.updateOne({"first": "luke"}, {$unset: {"middle": ""}}));
print(tojson(res));
assert.eq(res.matchedCount, 1);
assert.eq(res.modifiedCount, 1);
client.assertEncryptedCollectionCounts(collName, 2, 4, 2, 6);
}());
