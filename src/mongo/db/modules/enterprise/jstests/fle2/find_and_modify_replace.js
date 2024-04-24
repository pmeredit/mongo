/**
 * Test encrypted find and modify with replacement style update and upsert
 *
 * @tags: [ requires_fcv_70 ]
 */
import {EncryptedClient, kSafeContentField} from "jstests/fle2/libs/encrypted_client_util.js";

const dbName = 'find_and_modify_replace';
const dbTest = db.getSiblingDB(dbName);
dbTest.dropDatabase();

const client = new EncryptedClient(db.getMongo(), dbName);

assert.commandWorked(client.createEncryptionCollection("basic", {
    encryptedFields:
        {"fields": [{"path": "first", "bsonType": "string", "queries": {"queryType": "equality"}}]}
}));

const edb = client.getDB();
assert.commandWorked(edb.basic.einsert({"_id": 1, "first": "mark", "last": "Markus"}));
assert.commandWorked(edb.basic.einsert({"_id": 2, "first": "Mark", "last": "Marco"}));

print("EDC: " + tojson(dbTest.basic.find().toArray()));
client.assertEncryptedCollectionCounts("basic", 2, 2, 2);

client.assertEncryptedCollectionDocuments("basic", [
    {"_id": 1, "first": "mark", "last": "Markus"},
    {"_id": 2, "first": "Mark", "last": "Marco"},
]);

// Replace an encrypted field with a new document
let res = assert.commandWorked(edb.basic.erunCommand({
    findAndModify: edb.basic.getName(),
    query: {"last": "Marco"},
    update: {"last": "marco", "first": "matthew"}
}));
print(tojson(res));
assert.eq(res.lastErrorObject.n, 1);

client.assertEncryptedCollectionCounts("basic", 2, 3, 3);

client.assertOneEncryptedDocumentFields("basic", {"last": "marco"}, {"first": "matthew"});
client.assertEncryptedCollectionDocuments("basic", [
    {"_id": 1, "first": "mark", "last": "Markus"},
    {"_id": 2, "first": "matthew", "last": "marco"},
]);

// Remove the encrypted field via replace
assert.commandWorked(edb.basic.erunCommand(
    {findAndModify: edb.basic.getName(), query: {"last": "markus"}, update: {"last": "marco"}}));

res = assert.commandWorked(edb.basic.ereplaceOne({"last": "marco"}, {"last": "marco"}));
assert.eq(res.modifiedCount, 1);
let rawDoc = dbTest.basic.find({"last": "marco"}).toArray()[0];
assert.eq(rawDoc[kSafeContentField], []);
assert(!rawDoc.hasOwnProperty("first"));

client.assertEncryptedCollectionCounts("basic", 2, 3, 3);
client.assertEncryptedCollectionDocuments("basic", [
    {"_id": 1, "first": "mark", "last": "Markus"},
    {"_id": 2, "last": "marco"},
]);

// Add the encrypted field
assert.commandWorked(edb.basic.erunCommand({
    findAndModify: edb.basic.getName(),
    query: {"last": "marco"},
    update: {"last": "marco", "first": "luke"}
}));

assert.eq(res.modifiedCount, 1);

client.assertEncryptedCollectionCounts("basic", 2, 4, 4);
client.assertEncryptedCollectionDocuments("basic", [
    {"_id": 1, "first": "mark", "last": "Markus"},
    {"_id": 2, "last": "marco", "first": "luke"},
]);

// Fail with update pipeline by sending in a regular client
res = assert.commandFailedWithCode(dbTest.basic.runCommand({
    findAndModify: edb.basic.getName(),
    query: {"last": "markus"},
    update: [{
        q: {"last": "marco"},
        u: [
            {$set: {status: "Modified", comments: ["$misc1", "$misc2"]}},
            {$unset: ["misc1", "misc2"]}
        ]
    }],
    encryptionInformation: {
        schema: {
            "find_and_modify_replace.basic":
                {escCollection: "foo", ecocCollection: "foo", fields: []}
        }
    }
}),
                                   6439901);

client.assertEncryptedCollectionCounts("basic", 2, 4, 4);

// Add a document via upsert
res = assert.commandWorked(edb.basic.erunCommand({
    findAndModify: edb.basic.getName(),
    query: {"last": "Marco"},
    update: {"last": "Marco", "first": "Luke"},
    upsert: true,
}));
print(tojson(res));
print("EDC: " + tojson(dbTest.basic.find().toArray()));
assert(res.hasOwnProperty("lastErrorObject"));
assert(res.lastErrorObject.hasOwnProperty("upserted"));
assert(res.lastErrorObject.hasOwnProperty("updatedExisting"));
assert.eq(res.lastErrorObject.n, 1);
assert.eq(res.lastErrorObject.updatedExisting, false);

client.assertOneEncryptedDocumentFields("basic", {"last": "Marco"}, {"first": "Luke"});
client.assertEncryptedCollectionCounts("basic", 3, 5, 5);
