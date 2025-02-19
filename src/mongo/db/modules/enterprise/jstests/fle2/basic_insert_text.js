/**
 * Test insert of text search indexed encrypted field works
 *
 * @tags: [
 *   featureFlagQETextSearchPreview,
 * ]
 */
import {
    assertIsTextIndexedEncryptedField,
    codeFailsInClientWithError,
    EncryptedClient,
    kSafeContentField
} from "jstests/fle2/libs/encrypted_client_util.js";
import {
    PrefixField,
    SubstringField,
    SuffixAndPrefixField,
    SuffixField
} from "jstests/fle2/libs/qe_text_search_util.js";

const dbName = 'basic_insert_text';
const dbTest = db.getSiblingDB(dbName);
dbTest.dropDatabase();
const client = new EncryptedClient(db.getMongo(), dbName);

const firstNameField = new SubstringField(1000, 10, 100, false, false, 1);
const lastNameField = new PrefixField(2, 48, false, true, 1);
const locationField = new SuffixField(2, 48, true, false, 1);
const ssnField = new SuffixAndPrefixField(3, 30, 2, 20, true, true, 1);

jsTest.log("createEncryptionCollection");
assert.commandWorked(client.createEncryptionCollection("basic", {
    encryptedFields: {
        "fields": [
            {
                path: "firstName",
                bsonType: "string",
                queries: firstNameField.createQueryTypeDescriptor()
            },
            {
                path: "lastName",
                bsonType: "string",
                queries: lastNameField.createQueryTypeDescriptor()
            },
            {
                path: "location",
                bsonType: "string",
                queries: locationField.createQueryTypeDescriptor()
            },
            {path: "ssn", bsonType: "string", queries: ssnField.createQueryTypeDescriptor()},
        ]
    }
}));

const edb = client.getDB();

// Insert a document with fields which get encrypted
jsTestLog("doing single encrypted substring field insert");
let testStr = "Linda";
assert.commandWorked(edb.basic.einsert({"_id": 1, "firstName": testStr}));
let edcCount = 1;
let tagCount = firstNameField.calculateExpectedTagCount(testStr.length);
client.assertEncryptedCollectionCounts("basic", edcCount, tagCount, tagCount);

jsTestLog("doing single encrypted suffix field insert");
testStr = "New Jersey";
assert.commandWorked(edb.basic.einsert({"_id": 2, "location": testStr}));
tagCount += locationField.calculateExpectedTagCount(testStr.length);
edcCount++;
client.assertEncryptedCollectionCounts("basic", edcCount, tagCount, tagCount);

jsTestLog("doing single encrypted prefix field insert");
testStr = "Belcher";
assert.commandWorked(edb.basic.einsert({"_id": 3, "lastName": "Belcher"}));
tagCount += lastNameField.calculateExpectedTagCount("Belcher".length);
edcCount++;
client.assertEncryptedCollectionCounts("basic", edcCount, tagCount, tagCount);

jsTestLog("doing single encrypted prefix+suffix field insert");
testStr = "997-23-2222";
assert.commandWorked(edb.basic.einsert({"_id": 4, "ssn": testStr}));
tagCount += ssnField.calculateExpectedTagCount(testStr.length);
edcCount++;
client.assertEncryptedCollectionCounts("basic", edcCount, tagCount, tagCount);

jsTestLog("doing multiple encrypted fields insert");
assert.commandWorked(edb.basic.einsert({
    "_id": 5,
    "firstName": "Linda",
    "lastName": "Belcher",
    "ssn": "997-23-2222",
    "location": "New Jersey",
}));
tagCount *= 2;  // x2 because we just inserted the same values as we did individually above.
edcCount++;
client.assertEncryptedCollectionCounts("basic", edcCount, tagCount, tagCount);

const rawDoc = dbTest.basic.find({_id: 5}).toArray()[0];
jsTest.log("rawdoc");
print(tojson(rawDoc));
assertIsTextIndexedEncryptedField(rawDoc["firstName"]);
assertIsTextIndexedEncryptedField(rawDoc["lastName"]);
assertIsTextIndexedEncryptedField(rawDoc["ssn"]);
assertIsTextIndexedEncryptedField(rawDoc["location"]);
assert(rawDoc[kSafeContentField] !== undefined);

// Verify we decrypt it clean with an encrypted client.
let doc = edb.basic.efindOne({_id: 5});
jsTest.log("cleandoc");
print(tojson(doc));
assert.eq(doc["firstName"], "Linda");
assert.eq(doc["lastName"], "Belcher");
assert.eq(doc["ssn"], "997-23-2222");
assert.eq(doc["location"], "New Jersey");
assert(doc[kSafeContentField] !== undefined);

// Test various string lengths for a substring field
for (let len of [0, 8 /* < lb */, 13, 16, 18, 48, 101, 160]) {
    const expectTagCt = firstNameField.calculateExpectedTagCount(len);
    jsTestLog(
        `inserting substring encrypted field value with length: ${len}, tag_ct: ${expectTagCt}`);
    if (expectTagCt > 255) {  // Too many tags

        assert.commandFailedWithCode(edb.basic.einsert({firstName: "a".repeat(len)}), 9784104);
    } else {
        assert.commandWorked(edb.basic.einsert({firstName: "a".repeat(len)}));
        edcCount++;
        tagCount += expectTagCt;
    }
    client.assertEncryptedCollectionCounts("basic", edcCount, tagCount, tagCount);
}

// Test various string lengths for suffix field
for (let len of [0, 1 /* < lb */, 13, 16, 18, 48, 101, 160]) {
    const expectTagCt = locationField.calculateExpectedTagCount(len);
    jsTestLog(`inserting suffix encrypted field value with length: ${len}, tag_ct: ${expectTagCt}`);
    if (expectTagCt > 255) {  // Too many tags
        assert.commandFailedWithCode(edb.basic.einsert({location: "a".repeat(len)}), 9784104);
    } else {
        assert.commandWorked(edb.basic.einsert({location: "a".repeat(len)}));
        edcCount++;
        tagCount += expectTagCt;
    }
    client.assertEncryptedCollectionCounts("basic", edcCount, tagCount, tagCount);
}

// Test various string lengths for prefix field
for (let len of [0, 1 /* < lb */, 13, 16, 18, 48, 101, 160]) {
    const expectTagCt = lastNameField.calculateExpectedTagCount(len);
    jsTestLog(`inserting prefix encrypted field value with length: ${len}, tag_ct: ${expectTagCt}`);
    if (expectTagCt > 255) {  // Too many tags
        assert.commandFailedWithCode(edb.basic.einsert({lastName: "a".repeat(len)}), 9784104);
    } else {
        assert.commandWorked(edb.basic.einsert({lastName: "a".repeat(len)}));
        edcCount++;
        tagCount += expectTagCt;
    }
    client.assertEncryptedCollectionCounts("basic", edcCount, tagCount, tagCount);
}

// Test input for substring encrypted field is longer than mlen
jsTestLog("doing substring field insert: length > mlen");
assert(codeFailsInClientWithError(
    () => { edb.basic.einsert({"firstName": "a".repeat(1001)}); },
    "String passed in was longer than the maximum length for substring indexing"));
