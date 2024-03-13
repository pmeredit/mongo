/**
 * Test encrypted insert works
 *
 * @tags: [
 *   requires_fcv_80,
 * ]
 */
import {
    assertIsEqualityIndexedEncryptedField,
    assertIsRangeIndexedEncryptedField,
    EncryptedClient,
    kSafeContentField
} from "jstests/fle2/libs/encrypted_client_util.js";

let dbName = 'basic_insert_range';
let dbTest = db.getSiblingDB(dbName);
dbTest.dropDatabase();

let client = new EncryptedClient(db.getMongo(), dbName);

jsTest.log("createEncryptionCollection");
assert.commandWorked(client.createEncryptionCollection("basic", {
    encryptedFields: {
        "fields": [
            {
                "path": "issueDate",
                "bsonType": "date",
                "queries": {
                    "queryType": "range",
                    "min": new Date("1900-01-01"),
                    "max": new Date("2600-01-01"),
                    "sparsity": 1
                }
            },
            {
                "path": "hair",
                "bsonType": "string",
                "queries": {"queryType": "equality", "contention": NumberLong(1)}
            },
            {
                "path": "eyes",
                "bsonType": "string",
                "queries": {"queryType": "equality", "contention": NumberLong(2)}
            },
            {
                "path": "height.ft",
                "bsonType": "long",
                "queries": {
                    "queryType": "range",
                    "min": NumberLong(0),
                    "max": NumberLong(7),
                    "sparsity": 1,
                }
            },
            {
                "path": "height.in",
                "bsonType": "int",
                "queries": {
                    "queryType": "range",
                    "min": NumberInt(0),
                    "max": NumberInt(12),
                    "sparsity": 2,
                }
            },
            {
                "path": "weight",
                "bsonType": "double",
                "queries": {"queryType": "range", "sparsity": 4}
            }
        ]
    }
}));

const kExpectEdgesIssueDate = 46;
const kExpectEdgesHair = 1;
const kExpectEdgesEyes = 1;
const kExpectEdgesHeightFt = 4;
const kExpectEdgesHeightIn = 3;
const kExpectEdgesWeight = 17;

let edb = client.getDB();

// Insert a document with fields which get encrypted
jsTest.log("insert: basic");
let res = assert.commandWorked(edb.runCommand({
    "insert": "basic",
    documents: [{
        "_id": 355647829019,
        "issueDate": new Date("2505-03-03"),
        "firstName": "Not",
        "lastName": "Sure",
        "hair": "yes",
        "eyes": "yes",
        "height": {"ft": NumberLong(6), "in": NumberInt(0)},
        "weight": 178.0
    }]
}));
print(tojson(res));
assert.eq(res.n, 1);
client.assertWriteCommandReplyFields(res);

const kExpectEdges = kExpectEdgesIssueDate + kExpectEdgesHair + kExpectEdgesEyes +
    kExpectEdgesHeightFt + kExpectEdgesHeightIn + kExpectEdgesWeight;

client.assertEncryptedCollectionCounts("basic", 1, kExpectEdges, kExpectEdges);
// Verify it is encrypted with an unencrypted client
let rawDoc = dbTest.basic.find().toArray()[0];
jsTest.log("rawdoc");
print(tojson(rawDoc));
assertIsRangeIndexedEncryptedField(rawDoc["issueDate"]);
assertIsEqualityIndexedEncryptedField(rawDoc["hair"]);
assertIsEqualityIndexedEncryptedField(rawDoc["eyes"]);
assertIsRangeIndexedEncryptedField(rawDoc["height"]["ft"]);
assertIsRangeIndexedEncryptedField(rawDoc["height"]["in"]);
assertIsRangeIndexedEncryptedField(rawDoc["weight"]);
assert(rawDoc[kSafeContentField] !== undefined);

// Verify we decrypt it clean with an encrypted client.
let doc = edb.basic.find().toArray()[0];
jsTest.log("cleandoc");
print(tojson(doc));
assert.eq(doc["issueDate"], new Date("2505-03-03"));
assert.eq(doc["firstName"], "Not");
assert.eq(doc["lastName"], "Sure");
assert.eq(doc["hair"], "yes");
assert.eq(doc["eyes"], "yes");
assert.eq(doc["height"]["ft"], 6);
assert.eq(doc["height"]["in"], 0);
assert.eq(doc["weight"], 178.0);

assert(doc[kSafeContentField] !== undefined);

// Make an insert with no encrypted fields
assert.commandWorked(edb.basic.insert({"firstName": "Frito"}));
rawDoc = dbTest.basic.find({"firstName": "Frito"}).toArray()[0];
print(tojson(rawDoc));
assert.eq(rawDoc["firstName"], "Frito");
assert(rawDoc[kSafeContentField] === undefined);

client.assertEncryptedCollectionCounts("basic", 2, kExpectEdges, kExpectEdges);

// Trigger a duplicate key exception and validate the response
res = assert.commandFailed(
    edb.runCommand({"insert": "basic", documents: [{"_id": 355647829019, "lastName": "camacho"}]}));
print(tojson(res));

assert.eq(res.n, 0);
client.assertWriteCommandReplyFields(res);

// Inserting a document with encrypted data at a path that is marked for encryption, throws an
// error.
assert.throwsWithCode(
    () => edb.basic.runCommand({"insert": "basic", documents: [{"weight": BinData(6, "data")}]}),
    31041);
