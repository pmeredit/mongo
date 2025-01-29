/**
 * Test insert of text search indexed encrypted field works
 *
 * @tags: [
 *   featureFlagQETextSearchPreview,
 * ]
 */
import {
    assertIsTextIndexedEncryptedField,
    EncryptedClient,
    kSafeContentField
} from "jstests/fle2/libs/encrypted_client_util.js";

const dbName = 'basic_insert_text';
const dbTest = db.getSiblingDB(dbName);
dbTest.dropDatabase();
const client = new EncryptedClient(db.getMongo(), dbName);

jsTest.log("createEncryptionCollection");
assert.commandWorked(client.createEncryptionCollection("basic", {
    encryptedFields: {
        "fields": [
            {
                path: "firstName",
                bsonType: "string",
                queries: {
                    queryType: "substringPreview",
                    contention: NumberLong(1),
                    strMaxLength: NumberLong(1000),
                    strMaxQueryLength: NumberLong(100),
                    strMinQueryLength: NumberLong(10),
                    caseSensitive: false,
                    diacriticSensitive: false
                }
            },
            {
                path: "lastName",
                bsonType: "string",
                queries: {
                    queryType: "prefixPreview",
                    contention: NumberLong(1),
                    strMinQueryLength: NumberLong(2),
                    strMaxQueryLength: NumberLong(20),
                    caseSensitive: false,
                    diacriticSensitive: true,
                }
            },
            {
                path: "location",
                bsonType: "string",
                queries: {
                    queryType: "suffixPreview",
                    contention: NumberLong(1),
                    strMinQueryLength: NumberLong(2),
                    strMaxQueryLength: NumberLong(20),
                    caseSensitive: true,
                    diacriticSensitive: false,
                }
            },
            {
                path: "ssn",
                bsonType: "string",
                queries: [
                    {
                        queryType: "suffixPreview",
                        contention: NumberLong(1),
                        strMinQueryLength: NumberLong(3),
                        strMaxQueryLength: NumberLong(30),
                        caseSensitive: true,
                        diacriticSensitive: true,
                    },
                    {
                        queryType: "prefixPreview",
                        contention: NumberLong(1),
                        strMinQueryLength: NumberLong(2),
                        strMaxQueryLength: NumberLong(20),
                        caseSensitive: true,
                        diacriticSensitive: true,
                    },
                ]
            },
        ]
    }
}));

const edb = client.getDB();

// Insert a document with fields which get encrypted
jsTestLog("doing encrypted insert");
let res = assert.commandWorked(edb.erunCommand({
    insert: "basic",
    documents: [{
        "_id": 1,
        "firstName": "Linda",
        "lastName": "Belcher",
        "ssn": "997-23-2222",
        "location": "New Jersey",
    }]
}));
print(tojson(res));
assert.eq(res.n, 1);

const rawDoc = dbTest.basic.find().toArray()[0];
jsTest.log("rawdoc");
print(tojson(rawDoc));
assertIsTextIndexedEncryptedField(rawDoc["firstName"]);
assertIsTextIndexedEncryptedField(rawDoc["lastName"]);
assertIsTextIndexedEncryptedField(rawDoc["ssn"]);
assertIsTextIndexedEncryptedField(rawDoc["location"]);
assert(rawDoc[kSafeContentField] !== undefined);

// Verify we decrypt it clean with an encrypted client.
let doc = edb.basic.efindOne();
jsTest.log("cleandoc");
print(tojson(doc));

// TODO: SERVER-99771 uncomment once shell supports decryption of text indexed encrypted fields
// assert.eq(doc["firstName"], "Linda");
// assert.eq(doc["lastName"], "Belcher");
// assert.eq(doc["ssn"], "997-23-2222");
// assert.eq(doc["location"], "New Jersey");

assert(doc[kSafeContentField] !== undefined);
