/**
 * Test encrypted insert works
 *
 * @tags: [
 * assumes_read_preference_unchanged,
 * requires_fcv_70
 * ]
 */
import {
    assertIsIndexedEncryptedField,
    assertIsUnindexedEncryptedField,
    EncryptedClient,
    kSafeContentField
} from "jstests/fle2/libs/encrypted_client_util.js";

let dbName = 'basic_insert';
let dbTest = db.getSiblingDB(dbName);
dbTest.dropDatabase();

let client = new EncryptedClient(db.getMongo(), dbName);

assert.commandWorked(client.createEncryptionCollection("basic", {
    encryptedFields: {
        "fields": [
            {"path": "first", "bsonType": "string", "queries": {"queryType": "equality"}},
            {"path": "middle", "bsonType": "string"},
            {"path": "aka", "bsonType": "string", "queries": {"queryType": "equality"}},
        ]
    }
}));

let edb = client.getDB();

// Insert a document with a field that gets encrypted
let res = assert.commandWorked(edb.runCommand({
    "insert": "basic",
    documents: [{
        "_id": 1,
        "first": "dwayne",
        "middle": "elizondo mountain dew herbert",
        "aka": "president camacho"
    }]
}));
print(tojson(res));
assert.eq(res.n, 1);
client.assertWriteCommandReplyFields(res);

client.assertEncryptedCollectionCounts("basic", 1, 2, 2);

// Verify it is encrypted with an unencrypted client
let rawDoc = dbTest.basic.find().toArray()[0];
print(tojson(rawDoc));
assertIsIndexedEncryptedField(rawDoc["first"]);
assertIsUnindexedEncryptedField(rawDoc["middle"]);
assertIsIndexedEncryptedField(rawDoc["aka"]);
assert(rawDoc[kSafeContentField] !== undefined);

// Verify we decrypt it clean with an encrypted client.
let doc = edb.basic.find().toArray()[0];
print(tojson(doc));
assert.eq(doc["first"], "dwayne");
assert.eq(doc["middle"], "elizondo mountain dew herbert");
assert.eq(doc["aka"], "president camacho");
assert(doc[kSafeContentField] !== undefined);

client.assertOneEncryptedDocumentFields("basic", {}, {"first": "dwayne"});

// Make an insert with no encrypted fields
assert.commandWorked(edb.basic.insert({"last": "camacho"}));

rawDoc = dbTest.basic.find({"last": "camacho"}).toArray()[0];
print(tojson(rawDoc));
assert.eq(rawDoc["last"], "camacho");
assert(rawDoc[kSafeContentField] === undefined);

client.assertEncryptedCollectionCounts("basic", 2, 2, 2);

// Trigger a duplicate key exception and validate the response
res = assert.commandFailed(
    edb.runCommand({"insert": "basic", documents: [{"_id": 1, "first": "camacho"}]}));
print(tojson(res));

assert.eq(res.n, 0);
client.assertWriteCommandReplyFields(res);

// Inserting a document with encrypted data at a path that is marked for encryption, throws an
// error.
assert.throwsWithCode(
    () => edb.basic.runCommand({"insert": "basic", documents: [{"first": BinData(6, "data")}]}),
    31041);
