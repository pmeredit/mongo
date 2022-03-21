/**
 * Test encrypted insert works
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

let dbName = 'basic_insert';
let dbTest = db.getSiblingDB(dbName);
dbTest.dropDatabase();

let client = new EncryptedClient(db.getMongo(), dbName);

assert.commandWorked(client.createEncryptionCollection("basic", {
    encryptedFields:
        {"fields": [{"path": "first", "bsonType": "string", "queries": {"queryType": "equality"}}]}
}));

let edb = client.getDB();

// Insert a document with a field that gets encrypted

// Verify we can insert two documents in a txn

// Insert a document with a field that gets encrypted
let res = assert.commandWorked(
    edb.runCommand({"insert": "basic", documents: [{"_id": 1, "first": "mark"}]}));
print(tojson(res));
assert.eq(res.n, 1);
client.assertWriteCommandReplyFields(res);

client.assertEncryptedCollectionCounts("basic", 1, 1, 0, 1);

// Verify it is encrypted with an unencrypted client
let rawDoc = dbTest.basic.find().toArray()[0];
print(tojson(rawDoc));
assertIsIndexedEncryptedField(rawDoc["first"]);
assert(rawDoc["__safeContent__"] !== undefined);

// Verify we decrypt it clean with an encrypted client.
let doc = edb.basic.find().toArray()[0];
print(tojson(doc));
assert.eq(doc["first"], "mark");
assert(doc["__safeContent__"] !== undefined);

client.assertOneEncryptedDocumentFields("basic", {}, {"first": "mark"});

// Make an insert with no encrypted fields
assert.commandWorked(edb.basic.insert({"last": "marco"}));

rawDoc = dbTest.basic.find({"last": "marco"}).toArray()[0];
print(tojson(rawDoc));
assert.eq(rawDoc["last"], "marco");
assert(rawDoc["__safeContent__"] === undefined);

client.assertEncryptedCollectionCounts("basic", 2, 1, 0, 1);

// Trigger a duplicate key exception and validate the response
res = assert.commandFailed(
    edb.runCommand({"insert": "basic", documents: [{"_id": 1, "first": "marco"}]}));
print(tojson(res));

assert.eq(res.n, 0);
client.assertWriteCommandReplyFields(res);
}());
