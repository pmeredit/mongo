/**
 * Test that batch insert works with FLE2.
 *
 * @tags: [
 *   requires_fcv_70,
 *   assumes_read_preference_unchanged,
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
    validator: {$jsonSchema: {required: ["first", "aka"]}},
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
let res = assert.commandWorked(edb.erunCommand({
    "insert": "basic",
    documents: [
        {
            "_id": 1,
            "first": "dwayne",
            "middle": "elizondo mountain dew herbert",
            "aka": "president camacho"
        },
        {"_id": 2, "first": "bob", "middle": "belcher", "aka": "bobs burgers"},
        {"_id": 3, "first": "linda", "middle": "belcher", "aka": "linda"}
    ]
}));

print(tojson(res));
assert.eq(res.n, 3);
client.assertWriteCommandReplyFields(res);

client.assertEncryptedCollectionCounts("basic", 3, 6, 6);

// Verify it is encrypted with an unencrypted client
print("Testing that all documents are encrypted");
{
    let rawDoc = dbTest.basic.find({"_id": 1}).toArray()[0];
    print(tojson(rawDoc));
    assertIsIndexedEncryptedField(rawDoc["first"]);
    assertIsUnindexedEncryptedField(rawDoc["middle"]);
    assertIsIndexedEncryptedField(rawDoc["aka"]);
    assert(rawDoc[kSafeContentField] !== undefined);

    rawDoc = dbTest.basic.find({"_id": 2}).toArray()[0];
    print(tojson(rawDoc));
    assertIsIndexedEncryptedField(rawDoc["first"]);
    assertIsUnindexedEncryptedField(rawDoc["middle"]);
    assertIsIndexedEncryptedField(rawDoc["aka"]);
    assert(rawDoc[kSafeContentField] !== undefined);

    rawDoc = dbTest.basic.find({"_id": 3}).toArray()[0];
    print(tojson(rawDoc));
    assertIsIndexedEncryptedField(rawDoc["first"]);
    assertIsUnindexedEncryptedField(rawDoc["middle"]);
    assertIsIndexedEncryptedField(rawDoc["aka"]);
    assert(rawDoc[kSafeContentField] !== undefined);
}

// Verify we decrypt it clean with an encrypted client.
print("Testing that all documents can be decrypted by an encrypted client");
{
    let doc = edb.basic.efindOne({"_id": 1});
    print(tojson(doc));
    assert.eq(doc["first"], "dwayne");
    assert.eq(doc["middle"], "elizondo mountain dew herbert");
    assert.eq(doc["aka"], "president camacho");
    assert(doc[kSafeContentField] !== undefined);

    doc = edb.basic.efindOne({"_id": 2});
    print(tojson(doc));
    assert.eq(doc["first"], "bob");
    assert.eq(doc["middle"], "belcher");
    assert.eq(doc["aka"], "bobs burgers");
    assert(doc[kSafeContentField] !== undefined);

    doc = edb.basic.efindOne({"_id": 3});
    print(tojson(doc));
    assert.eq(doc["first"], "linda");
    assert.eq(doc["middle"], "belcher");
    assert.eq(doc["aka"], "linda");
    assert(doc[kSafeContentField] !== undefined);
}

// Insert documents with ordered = true
{
    let commandResult = edb.erunCommand({
        "insert": "basic",
        documents: [
            {"_id": 4, "first": "tina", "aka": "belcher"},
            {
                "_id": 5,
                "first": "louise",
            },
            {"_id": 6, "first": "gene", "aka": "belcher"}
        ]
    });

    assert.eq(commandResult.ok, 1);
    assert.eq(commandResult.n, 1);

    const writeErrors = commandResult.writeErrors;

    assert.eq(writeErrors.length, 1);

    assert.eq(writeErrors[0].errmsg, "Document failed validation");

    assert.eq(writeErrors[0].index, 1);

    client.assertEncryptedCollectionCounts("basic", 4, 8, 8);

    let rawDoc = dbTest.basic.find({"_id": 4}).toArray()[0];
    print(tojson(rawDoc));
    assertIsIndexedEncryptedField(rawDoc["first"]);
    assertIsIndexedEncryptedField(rawDoc["aka"]);
    assert(rawDoc[kSafeContentField] !== undefined);

    let decryptedDoc = edb.basic.efindOne({"_id": 4});
    print(tojson(decryptedDoc));
    assert.eq(decryptedDoc["first"], "tina");
    assert.eq(decryptedDoc["aka"], "belcher");
    assert(decryptedDoc[kSafeContentField] !== undefined);

    let non_existant = dbTest.basic.find({"_id": 5}).toArray();
    assert.eq(non_existant.length, 0);

    non_existant = dbTest.basic.find({"_id": 6}).toArray();
    assert.eq(non_existant.length, 0);
}

// Insert docs with ordered = false to skip over errors
{
    let commandResult = edb.erunCommand({
        "insert": "basic",
        documents: [
            {"_id": 7, "first": "teddy", "aka": "handyman"},
            {
                "_id": 8,
                "first": "edith",
            },
            {
                "_id": 9,
                "aka": "mr. frond",
            },
            {"_id": 10, "first": "mort", "aka": "mortician"}
        ],
        ordered: false
    });

    assert.eq(commandResult.ok, 1);
    assert.eq(commandResult.n, 2);

    let writeErrors = commandResult.writeErrors;
    assert.eq(writeErrors.length, 2);

    assert.eq(writeErrors[0].errmsg, "Document failed validation");
    assert.eq(writeErrors[1].errmsg, "Document failed validation");

    assert.eq(writeErrors[0].index, 1);
    assert.eq(writeErrors[1].index, 2);

    client.assertEncryptedCollectionCounts("basic", 6, 12, 12);

    let rawDoc = dbTest.basic.find({"_id": 7}).toArray()[0];
    print(tojson(rawDoc));
    assertIsIndexedEncryptedField(rawDoc["first"]);
    assertIsIndexedEncryptedField(rawDoc["aka"]);
    assert(rawDoc[kSafeContentField] !== undefined);

    let decryptedDoc = edb.basic.efindOne({"_id": 7});
    print(tojson(decryptedDoc));
    assert.eq(decryptedDoc["first"], "teddy");
    assert.eq(decryptedDoc["aka"], "handyman");
    assert(decryptedDoc[kSafeContentField] !== undefined);

    rawDoc = dbTest.basic.find({"_id": 10}).toArray()[0];
    print(tojson(rawDoc));
    assertIsIndexedEncryptedField(rawDoc["first"]);
    assertIsIndexedEncryptedField(rawDoc["aka"]);
    assert(rawDoc[kSafeContentField] !== undefined);

    decryptedDoc = edb.basic.efindOne({"_id": 10});
    print(tojson(decryptedDoc));
    assert.eq(decryptedDoc["first"], "mort");
    assert.eq(decryptedDoc["aka"], "mortician");
    assert(decryptedDoc[kSafeContentField] !== undefined);

    let non_existant = dbTest.basic.find({"_id": 8}).toArray();
    assert.eq(non_existant.length, 0);

    non_existant = dbTest.basic.find({"_id": 9}).toArray();
    assert.eq(non_existant.length, 0);
}
