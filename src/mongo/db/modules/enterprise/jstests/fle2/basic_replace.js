/**
 * Test encrypted update works with replace and pipeline
 *
 * @tags: [
 * requires_fcv_70,
 * assumes_read_preference_unchanged,
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
assert.commandWorked(edb.basic.insert({"_id": 1, "first": "mark", "last": "marco"}));
assert.commandWorked(edb.basic.insert({"_id": 2, "first": "Mark", "last": "Marcus"}));

print("EDC: " + tojson(dbTest.basic.find().toArray()));
client.assertEncryptedCollectionCounts("basic", 2, 2, 2);

// Replace an encrypted field with a new document
let res = assert.commandWorked(
    edb.basic.replaceOne({"last": "marco"}, {"last": "marco", "first": "matthew"}));
print(tojson(res));
assert.eq(res.matchedCount, 1);
assert.eq(res.modifiedCount, 1);

client.assertEncryptedCollectionCounts("basic", 2, 3, 3);

client.assertOneEncryptedDocumentFields("basic", {"last": "marco"}, {"first": "matthew"});
client.assertEncryptedCollectionDocuments("basic", [
    {"_id": 1, "first": "matthew", "last": "marco"},
    {
        "_id": 2,
        "first": "Mark",
        "last": "Marcus",
    },
]);

// Remove the encrypted field via replace
res = assert.commandWorked(edb.basic.replaceOne({"last": "marco"}, {"last": "marco"}));
assert.eq(res.modifiedCount, 1);
let rawDoc = dbTest.basic.find({"last": "marco"}).toArray()[0];
assert.eq(rawDoc[kSafeContentField], [], tojson(rawDoc));
assert(!rawDoc.hasOwnProperty("first"));

client.assertEncryptedCollectionCounts("basic", 2, 3, 3);
client.assertEncryptedCollectionDocuments("basic", [
    {"_id": 1, "last": "marco"},
    {
        "_id": 2,
        "first": "Mark",
        "last": "Marcus",
    },
]);

// Add the encrypted field
res = assert.commandWorked(
    edb.basic.replaceOne({"last": "marco"}, {"last": "marco", "first": "luke"}));
assert.eq(res.modifiedCount, 1);
client.assertOneEncryptedDocumentFields("basic", {"last": "marco"}, {"first": "luke"});

client.assertEncryptedCollectionCounts("basic", 2, 4, 4);
client.assertEncryptedCollectionDocuments("basic", [
    {"_id": 1, "last": "marco", "first": "luke"},
    {
        "_id": 2,
        "first": "Mark",
        "last": "Marcus",
    },
]);

// Fail with update pipeline by sending in a regular client
assert.commandFailedWithCode(dbTest.basic.runCommand({
    update: edb.basic.getName(),
    updates: [{
        q: {"last": "marco"},
        u: [
            {$set: {status: "Modified", comments: ["$misc1", "$misc2"]}},
            {$unset: ["misc1", "misc2"]}
        ]
    }],
    encryptionInformation: {schema: {}}
}),
                             6371517);

client.assertEncryptedCollectionCounts("basic", 2, 4, 4);

// Add a document via upsert
res = assert.commandWorked(edb.basic.runCommand({
    update: edb.basic.getName(),
    updates: [{q: {"last": "Marco"}, u: {"last": "Marco", "first": "Luke"}, upsert: true}]
}));
print(tojson(res));
assert.eq(res.nModified, 0);
assert.eq(res.n, 1);
assert(res.hasOwnProperty("upserted"));
assert.eq(res.upserted.length, 1);
assert(res.upserted[0].hasOwnProperty("_id"));

client.assertOneEncryptedDocumentFields("basic", {"last": "Marco"}, {"first": "Luke"});
client.assertEncryptedCollectionCounts("basic", 3, 5, 5);
