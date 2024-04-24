/**
 * Test encrypted update works
 *
 * @tags: [
 * requires_non_retryable_writes,
 * assumes_read_preference_unchanged,
 * requires_fcv_70
 * ]
 */

import {EncryptedClient, kSafeContentField} from "jstests/fle2/libs/encrypted_client_util.js";

let dbName = 'basic_update';
let dbTest = db.getSiblingDB(dbName);
dbTest.dropDatabase();

let client = new EncryptedClient(db.getMongo(), dbName);

assert.commandWorked(client.createEncryptionCollection("basic", {
    encryptedFields:
        {"fields": [{"path": "first", "bsonType": "string", "queries": {"queryType": "equality"}}]}
}));

let edb = client.getDB();

assert.commandWorked(edb.basic.einsert({"first": "mark", "last": "marco", "middle": "markus"}));
assert.commandWorked(edb.basic.einsert({"first": "Mark", "last": "Marcus", "middle": "markus"}));

print("EDC: " + tojson(dbTest.basic.find().toArray()));
client.assertEncryptedCollectionCounts("basic", 2, 2, 2);

let rawDoc = dbTest.basic.find().toArray()[0];
print(tojson(rawDoc));
client.assertOneEncryptedDocumentFields("basic", {"last": "marco"}, {"first": "mark"});

// Update an encrypted field in a document
let res =
    assert.commandWorked(edb.basic.eupdateOne({"last": "marco"}, {$set: {"first": "matthew"}}));
print(tojson(res));
assert.eq(res.matchedCount, 1);
assert.eq(res.modifiedCount, 1);

client.assertEncryptedCollectionCounts("basic", 2, 3, 3);

client.assertOneEncryptedDocumentFields("basic", {"last": "marco"}, {"first": "matthew"});

// Remove the encrypted field
res = assert.commandWorked(edb.basic.eupdateOne({"last": "marco"}, {$unset: {"first": ""}}));
assert.eq(res.matchedCount, 1);
assert.eq(res.modifiedCount, 1);
rawDoc = dbTest.basic.find({"last": "marco"}).toArray()[0];
assert.eq(rawDoc[kSafeContentField], []);
assert(!rawDoc.hasOwnProperty("first"));

client.assertEncryptedCollectionCounts("basic", 2, 3, 3);

// Add the encrypted field
res = assert.commandWorked(edb.basic.eupdateOne({"last": "marco"}, {$set: {"first": "luke"}}));
assert.eq(res.matchedCount, 1);
assert.eq(res.modifiedCount, 1);
client.assertOneEncryptedDocumentFields("basic", {"last": "marco"}, {"first": "luke"});

client.assertEncryptedCollectionCounts("basic", 2, 4, 4);

// Update an unencrypted field in a document, expect no esc/ecoc changes
res = assert.commandWorked(edb.basic.eupdateOne({"last": "marco"}, {$set: {"middle": "matthew"}}));
print(tojson(res));
assert.eq(res.matchedCount, 1);
assert.eq(res.modifiedCount, 1);

client.assertEncryptedCollectionCounts("basic", 2, 4, 4);

// Remove an unencrypted field in a document, expect no esc/ecoc changes
res = assert.commandWorked(edb.basic.eupdateOne({"last": "marco"}, {$unset: {"middle": ""}}));
print(tojson(res));
assert.eq(res.matchedCount, 1);
assert.eq(res.modifiedCount, 1);
client.assertEncryptedCollectionCounts("basic", 2, 4, 4);

// Update an unencrypted field in a document, but match no documents
// expect writes to esc,ecoc
res = assert.commandWorked(edb.basic.eupdateOne({"last": "marky"}, {$set: {"first": "matthew"}}));
print(tojson(res));
assert.eq(res.matchedCount, 0);
assert.eq(res.modifiedCount, 0);

client.assertEncryptedCollectionCounts("basic", 2, 5, 5);

// Update an unencrypted field in a document with the same value as before
// NOTE: In non-FLE2 updates, if a single update results in no change to the document,
//       then nModified is set to 0. This is NOT the case in FLE2 updates.
res = assert.commandWorked(edb.basic.eupdateOne({"last": "marco"}, {$set: {"last": "marco"}}));
print(tojson(res));
assert.eq(res.matchedCount, 1);
assert.eq(res.modifiedCount, 1);

client.assertEncryptedCollectionCounts("basic", 2, 5, 5);

// Update an encrypted field in a document with the same value as before, expect esc/ecoc changes
client.assertOneEncryptedDocumentFields("basic", {"last": "marco"}, {"first": "luke"});
res = assert.commandWorked(edb.basic.eupdateOne({"last": "marco"}, {$set: {"first": "luke"}}));
print(tojson(res));
assert.eq(res.matchedCount, 1);
assert.eq(res.modifiedCount, 1);

client.assertEncryptedCollectionCounts("basic", 2, 6, 6);

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
assert.commandWorked(coll.einsert({"first": "mark", "last": "marco", "middle": "markus"}));
assert.commandWorked(coll.einsert({"first": "Mark", "last": "Marcus", "middle": "markus"}));

print("EDC: " + tojson(dbTest[collName].find().toArray()));
client.assertEncryptedCollectionCounts(collName, 2, 2, 2);

if (!client.useImplicitSharding) {
    // Update a document by case-insensitive collation
    res = assert.commandWorked(edb.basic.eupdateOne({"last": "marcus"},
                                                    {$set: {"first": "john"}},
                                                    {collation: {locale: 'en_US', strength: 2}}));
    assert.eq(res.modifiedCount, 1);

    client.assertEncryptedCollectionCounts("basic", 2, 7, 7);
    client.assertOneEncryptedDocumentFields("basic", {"last": "Marcus"}, {"first": "john"});

    // Update an encrypted field in a document
    res = assert.commandWorked(coll.eupdateOne({"first": "mark"}, {$set: {"first": "matthew"}}));
    print(tojson(res));
    assert.eq(res.matchedCount, 1);
    assert.eq(res.modifiedCount, 1);

    client.assertEncryptedCollectionCounts(collName, 2, 3, 3);

    client.assertOneEncryptedDocumentFields(collName, {"last": "marco"}, {"first": "matthew"});

    // Remove the encrypted field
    res = assert.commandWorked(coll.eupdateOne({"first": "matthew"}, {$unset: {"first": ""}}));
    assert.eq(res.modifiedCount, 1);
    rawDoc = dbTest[collName].find({"last": "marco"}).toArray()[0];
    assert.eq(rawDoc["__safeContent__"], []);
    assert(!rawDoc.hasOwnProperty("first"));

    client.assertEncryptedCollectionCounts(collName, 2, 3, 3);

    // Add the encrypted field
    res = assert.commandWorked(coll.eupdateOne({"last": "marco"}, {$set: {"first": "luke"}}));
    assert.eq(res.modifiedCount, 1);
    client.assertOneEncryptedDocumentFields(collName, {"last": "marco"}, {"first": "luke"});

    client.assertEncryptedCollectionCounts(collName, 2, 4, 4);

    // Update an unencrypted field in a document, expect no esc/ecoc changes
    res = assert.commandWorked(coll.eupdateOne({"first": "luke"}, {$set: {"middle": "matthew"}}));
    print(tojson(res));
    assert.eq(res.matchedCount, 1);
    assert.eq(res.modifiedCount, 1);

    client.assertEncryptedCollectionCounts(collName, 2, 4, 4);

    // Remove an unencrypted field in a document, expect no esc/ecoc changes
    res = assert.commandWorked(coll.eupdateOne({"first": "luke"}, {$unset: {"middle": ""}}));
    assert.eq(res.matchedCount, 1);
    assert.eq(res.modifiedCount, 1);
    client.assertEncryptedCollectionCounts(collName, 2, 4, 4);

    // Update an unencrypted field in a document with upsert, but match no documents
    // expect writes to esc,ecoc
    res = assert.commandWorked(
        edb.basic.eupdateOne({"last": "marky"}, {$set: {"first": "matthew"}}, {upsert: true}));
    print(tojson(res));
    assert.eq(res.matchedCount, 0);
    assert.eq(res.modifiedCount, 0);
    assert(res.upsertedId !== undefined);

    client.assertEncryptedCollectionCounts("basic", 3, 8, 8);
}
