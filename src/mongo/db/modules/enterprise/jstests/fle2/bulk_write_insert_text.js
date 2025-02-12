/**
 * Test that bulkWrite insert works with FLE2 text query types.
 *
 * The test runs commands that are not allowed with security token: bulkWrite.
 * @tags: [
 *   not_allowed_with_signed_security_token,
 *   command_not_supported_in_serverless,
 *   does_not_support_transactions,
 *   featureFlagQETextSearchPreview,
 * ]
 */
import {
    assertIsTextIndexedEncryptedField,
    EncryptedClient,
    kSafeContentField
} from "jstests/fle2/libs/encrypted_client_util.js";
import {
    PrefixField,
    SubstringField,
    SuffixAndPrefixField,
    SuffixField
} from "jstests/fle2/libs/qe_text_search_util.js";
import {
    cursorEntryValidator,
    cursorSizeValidator,
    summaryFieldsValidator
} from "jstests/libs/bulk_write_utils.js";

const dbName = 'basic_insert_text';
const dbTest = db.getSiblingDB(dbName);
dbTest.dropDatabase();

const client = new EncryptedClient(db.getMongo(), dbName);
const edb = client.getDB();

const substringField = new SubstringField(1000, 3, 100, false, false, 1);
const suffixField = new SuffixField(2, 16, false, true, 1);
const prefixField = new PrefixField(3, 30, true, false, 1);
const comboField = new SuffixAndPrefixField(3, 35, 2, 19, true, true, 1);

assert.commandWorked(client.createEncryptionCollection("basic", {
    encryptedFields: {
        "fields": [
            {
                "path": "first",
                "bsonType": "string",
                "queries": substringField.createQueryTypeDescriptor()
            },
            {
                "path": "last",
                "bsonType": "string",
                "queries": suffixField.createQueryTypeDescriptor()
            },
            {
                "path": "country",
                "bsonType": "string",
                "queries": prefixField.createQueryTypeDescriptor()
            },
            {"path": "job", "bsonType": "string", "queries": comboField.createQueryTypeDescriptor()}
        ]
    }
}));

{
    print("Inserting document with text fields");
    const docs = [
        {"_id": 0, "first": "Anton", "last": "Bruckner", "country": "Austria", "job": "organist"},
        {"_id": 1, "first": "Johannes", "last": "Brahms", "country": "Germany", "job": "conductor"},
        {"_id": 2, "first": "Sergei", "last": "Rachmaninov", "country": "Russia", "job": "pianist"}
    ];

    const res = assert.commandWorked(edb.eadminCommand({
        bulkWrite: 1,
        ops: [
            {insert: 0, document: docs[0]},
            {insert: 0, document: docs[1]},
            {insert: 0, document: docs[2]}
        ],
        nsInfo: [{ns: `${dbName}.basic`}]
    }));

    summaryFieldsValidator(
        res, {nErrors: 0, nInserted: 3, nDeleted: 0, nMatched: 0, nModified: 0, nUpserted: 0});
    cursorSizeValidator(res, 3);
    cursorEntryValidator(res.cursor.firstBatch[0], {ok: 1, idx: 0, n: 1});
    cursorEntryValidator(res.cursor.firstBatch[1], {ok: 1, idx: 1, n: 1});
    cursorEntryValidator(res.cursor.firstBatch[2], {ok: 1, idx: 2, n: 1});
    client.assertWriteCommandReplyFields(res);

    let tagCount = 0;
    for (let doc of docs) {
        tagCount += substringField.calculateExpectedTagCount(doc.first.length);
        tagCount += suffixField.calculateExpectedTagCount(doc.last.length);
        tagCount += prefixField.calculateExpectedTagCount(doc.country.length);
        tagCount += comboField.calculateExpectedTagCount(doc.job.length);
    }

    // Verify it is encrypted with an unencrypted client
    print("Testing that all documents are encrypted");
    for (let i = 0; i < docs.length; i++) {
        const rawDoc = dbTest.basic.find({"_id": i}).toArray()[0];
        assertIsTextIndexedEncryptedField(rawDoc["first"]);
        assertIsTextIndexedEncryptedField(rawDoc["last"]);
        assertIsTextIndexedEncryptedField(rawDoc["country"]);
        assertIsTextIndexedEncryptedField(rawDoc["job"]);
        assert(rawDoc[kSafeContentField] !== undefined);
    }

    // Verify we decrypt it clean with an encrypted client.
    print("Testing that all documents can be decrypted by an encrypted client");
    for (let i = 0; i < docs.length; i++) {
        const doc = edb.basic.efindOne({"_id": i});
        assert.eq(doc["first"], docs[i].first);
        assert.eq(doc["last"], docs[i].last);
        assert.eq(doc["country"], docs[i].country);
        assert.eq(doc["job"], docs[i].job);
        assert(doc[kSafeContentField] !== undefined);
    }
}
