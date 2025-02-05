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
                    strMaxQueryLength: NumberLong(48),
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
                    strMaxQueryLength: NumberLong(48),
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

function getSubstringFieldTagCount(cplen, mlen, lb, ub) {
    assert.gte(cplen, 0);
    assert.gt(lb, 0);
    assert.gte(ub, lb);
    assert.gte(mlen, ub);
    const beta = cplen == 0 ? 1 : cplen;
    const cbclen = Math.ceil(beta / 16) * 16;
    if (beta > mlen || lb > cbclen) {
        return 0;
    }
    const hi = Math.min(ub, cbclen);
    const range = hi - lb + 1;
    const hisum = (hi * (hi + 1)) / 2;  // sum of [1..hi]
    const losum = (lb * (lb - 1)) / 2;  // sum of [1..lb)
    const maxkgram1 = (mlen * range) - (hisum - losum) + range;
    const maxkgram2 = (cbclen * range) - (hisum - losum) + range;
    return Math.min(maxkgram1, maxkgram2);
}

function getSuffixPrefixFieldTagCount(cplen, lb, ub) {
    assert.gt(lb, 0);
    assert.gte(ub, lb);
    assert.gte(cplen, 0);
    const beta = cplen == 0 ? 1 : cplen;
    const cbclen = Math.ceil(beta / 16) * 16;
    if (lb > cbclen) {
        return 0;
    }
    return Math.min(ub, cbclen) - lb + 1;
}

function firstNameTags(cplen) {
    return getSubstringFieldTagCount(cplen, 1000, 10, 100) + 1;  // +1 for exact match tag
}

function locationTags(cplen) {
    return getSuffixPrefixFieldTagCount(cplen, 2, 48) + 1;  // +1 for exact match tag
}

function lastNameTags(cplen) {
    return getSuffixPrefixFieldTagCount(cplen, 2, 48) + 1;  // +1 for exact match tag
}

function ssnTags(cplen) {
    // +1 for exact match tag
    return getSuffixPrefixFieldTagCount(cplen, 2, 20) + getSuffixPrefixFieldTagCount(cplen, 3, 30) +
        1;
}

// Insert a document with fields which get encrypted
jsTestLog("doing single encrypted substring field insert");
assert.commandWorked(edb.basic.einsert({"_id": 1, "firstName": "Linda"}));
let edcCount = 1;
let tagCount = firstNameTags(5);
client.assertEncryptedCollectionCounts("basic", edcCount, tagCount, tagCount);

jsTestLog("doing single encrypted suffix field insert");
assert.commandWorked(edb.basic.einsert({"_id": 2, "location": "New Jersey"}));
tagCount += locationTags(10);
edcCount++;
client.assertEncryptedCollectionCounts("basic", edcCount, tagCount, tagCount);

jsTestLog("doing single encrypted prefix field insert");
assert.commandWorked(edb.basic.einsert({"_id": 3, "lastName": "Belcher"}));
tagCount += lastNameTags(7);
edcCount++;
client.assertEncryptedCollectionCounts("basic", edcCount, tagCount, tagCount);

jsTestLog("doing single encrypted prefix+suffix field insert");
assert.commandWorked(edb.basic.einsert({"_id": 4, "ssn": "997-23-2222"}));
tagCount += ssnTags(11);
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
tagCount *= 2;
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
for (let len of [0, 8 /* < lb */, 16, 18, 48, 101, 160]) {
    const expectTagCt = firstNameTags(len);
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
for (let len of [0, 1 /* < lb */, 16, 18, 48, 101, 160]) {
    const expectTagCt = locationTags(len);
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
for (let len of [0, 1 /* < lb */, 16, 18, 48, 101, 160]) {
    const expectTagCt = lastNameTags(len);
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
