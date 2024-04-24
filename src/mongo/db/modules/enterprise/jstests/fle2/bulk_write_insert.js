/**
 * Test that bulkWrite insert works with FLE2.
 *
 * Some of the tests are incompatible with the transaction overrides since any failure
 * will cause a transaction abortion which will make the overrides infinite loop.
 *
 * The test runs commands that are not allowed with security token: bulkWrite.
 * @tags: [
 *   not_allowed_with_signed_security_token,
 *   command_not_supported_in_serverless,
 *   does_not_support_transactions,
 *   requires_fcv_80
 * ]
 */
import {
    assertIsIndexedEncryptedField,
    assertIsUnindexedEncryptedField,
    EncryptedClient,
    kSafeContentField
} from "jstests/fle2/libs/encrypted_client_util.js";
import {
    cursorEntryValidator,
    cursorSizeValidator,
    summaryFieldsValidator
} from "jstests/libs/bulk_write_utils.js";
import {assertDocumentValidationFailure} from "jstests/libs/doc_validation_utils.js";

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

assert.commandWorked(client.createEncryptionCollection("unencrypted_middle", {
    validator: {$jsonSchema: {required: ["middle"]}},
    encryptedFields: {
        "fields": [
            {"path": "first", "bsonType": "string", "queries": {"queryType": "equality"}},
            {"path": "aka", "bsonType": "string", "queries": {"queryType": "equality"}},
        ]
    }
}));

assert.commandWorked(dbTest.createCollection("unencrypted"));

let edb = client.getDB();

{
    print("Inserting documents with fields that get encrypted");
    let res = assert.commandWorked(edb.eadminCommand({
        bulkWrite: 1,
        ops: [
            {
                insert: 0,
                document: {
                    "_id": 1,
                    "first": "dwayne",
                    "middle": "elizondo mountain dew herbert",
                    "aka": "president camacho"
                }
            },
            {
                insert: 0,
                document: {"_id": 2, "first": "bob", "middle": "belcher", "aka": "bobs burgers"}
            },
            {insert: 0, document: {"_id": 3, "first": "linda", "middle": "belcher", "aka": "linda"}}
        ],
        nsInfo: [{ns: "basic_insert.basic"}]
    }));

    summaryFieldsValidator(
        res, {nErrors: 0, nInserted: 3, nDeleted: 0, nMatched: 0, nModified: 0, nUpserted: 0});
    cursorSizeValidator(res, 3);
    cursorEntryValidator(res.cursor.firstBatch[0], {ok: 1, idx: 0, n: 1});
    cursorEntryValidator(res.cursor.firstBatch[1], {ok: 1, idx: 1, n: 1});
    cursorEntryValidator(res.cursor.firstBatch[2], {ok: 1, idx: 2, n: 1});
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
}

{
    // If we start supporting multiple namespaces, we will have to test that it handles bulkWrite on
    // one encrypted collection and one un-encrypted collection, including ordered: false with the
    // encrypted insert failing.
    print("Inserting documents with fields that get encrypted (2 namespaces)");
    let error = assert.throws(() => edb.eadminCommand({
        bulkWrite: 1,
        ops: [
            {
                insert: 0,
                document: {
                    "_id": 1,
                    "first": "dwayne",
                    "middle": "elizondo mountain dew herbert",
                    "aka": "president camacho"
                }
            },
            {
                insert: 1,
                document: {"_id": 2, "first": "bob", "middle": "belcher", "aka": "bobs burgers"}
            }
        ],
        nsInfo: [{ns: "basic_insert.basic"}, {ns: "basic_insert.unencrypted_middle"}]
    }));

    assert.commandFailedWithCode(error, ErrorCodes.BadValue);
    assert.eq(error.reason, "BulkWrite with Queryable Encryption supports only a single namespace");
}

{
    print("Inserting documents with encrypted fields with ordered = true and errors");
    let res = assert.commandWorked(edb.eadminCommand({
        bulkWrite: 1,
        ops: [
            {
                insert: 0,
                document: {"_id": 4, "first": "tina", "aka": "belcher"},
            },
            {
                insert: 0,
                document: {"_id": 5, "first": "louise"},  // "aka" is required and missing.
            },
            {insert: 0, document: {"_id": 6, "first": "gene", "aka": "belcher"}}
        ],
        ordered: true,
        nsInfo: [{ns: "basic_insert.basic"}]
    }));

    summaryFieldsValidator(
        res, {nErrors: 1, nInserted: 1, nDeleted: 0, nMatched: 0, nModified: 0, nUpserted: 0});
    cursorSizeValidator(res, 2);
    cursorEntryValidator(res.cursor.firstBatch[0], {ok: 1, idx: 0, n: 1});
    cursorEntryValidator(res.cursor.firstBatch[1],
                         {ok: 0, idx: 1, n: 0, code: ErrorCodes.DocumentValidationFailure});
    assert.eq(res.cursor.firstBatch[1].errInfo.failingDocumentId,
              5,
              "Unexpected failingDocumentId in response: " + tojson(res));
    assertDocumentValidationFailure(res.cursor.firstBatch[1], "basic_insert.basic");
    client.assertWriteCommandReplyFields(res);

    client.assertEncryptedCollectionCounts("basic", 4, 8, 8);

    {
        print("Testing that all documents are encrypted");
        let rawDoc = dbTest.basic.findOne({"_id": 4});
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
}

{
    print("Inserting docs with encrypted fields and ordered = false to skip over errors");
    let res = assert.commandWorked(edb.eadminCommand({
        bulkWrite: 1,
        ops: [
            {
                insert: 0,
                document: {"_id": 7, "first": "teddy", "aka": "handyman"},
            },
            {
                insert: 0,
                document: {
                    "_id": 8,
                    "first": "edith",  // "aka" is required and missing.
                },
            },
            {
                insert: 0,
                document: {
                    "_id": 9,
                    "aka": "mr. frond",  // "first" is required and missing.
                },
            },
            {insert: 0, document: {"_id": 10, "first": "mort", "aka": "mortician"}}
        ],
        ordered: false,
        nsInfo: [{ns: "basic_insert.basic"}]
    }));

    summaryFieldsValidator(
        res, {nErrors: 2, nInserted: 2, nDeleted: 0, nMatched: 0, nModified: 0, nUpserted: 0});
    cursorSizeValidator(res, 4);
    cursorEntryValidator(res.cursor.firstBatch[0], {ok: 1, idx: 0, n: 1});
    cursorEntryValidator(res.cursor.firstBatch[1],
                         {ok: 0, idx: 1, n: 0, code: ErrorCodes.DocumentValidationFailure});
    assertDocumentValidationFailure(res.cursor.firstBatch[1], "basic_insert.basic");
    assert.eq(res.cursor.firstBatch[1].errInfo.failingDocumentId,
              8,
              "Unexpected failingDocumentId in response: " + tojson(res));
    cursorEntryValidator(res.cursor.firstBatch[2],
                         {ok: 0, idx: 2, n: 0, code: ErrorCodes.DocumentValidationFailure});
    assertDocumentValidationFailure(res.cursor.firstBatch[2], "basic_insert.basic");
    assert.eq(res.cursor.firstBatch[2].errInfo.failingDocumentId,
              9,
              "Unexpected failingDocumentId in response: " + tojson(res));
    cursorEntryValidator(res.cursor.firstBatch[3], {ok: 1, idx: 3, n: 1});
    client.assertWriteCommandReplyFields(res);

    client.assertEncryptedCollectionCounts("basic", 6, 12, 12);

    {
        print("Testing that all documents are encrypted");
        let rawDoc = dbTest.basic.findOne({"_id": 7});
        print(tojson(rawDoc));
        assertIsIndexedEncryptedField(rawDoc["first"]);
        assertIsIndexedEncryptedField(rawDoc["aka"]);
        assert(rawDoc[kSafeContentField] !== undefined);

        let decryptedDoc = edb.basic.efindOne({"_id": 7});
        print(tojson(decryptedDoc));
        assert.eq(decryptedDoc["first"], "teddy");
        assert.eq(decryptedDoc["aka"], "handyman");
        assert(decryptedDoc[kSafeContentField] !== undefined);

        rawDoc = dbTest.basic.findOne({"_id": 10});
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
}

{
    // FLE processInsert handles single document with no encrypted field differently by returning
    // FLEBatchResult::kNotProcessed. Checking BulkWrite handles that correctly, especially for
    // retryable writes.
    print("Inserting doc without field needing to get encrypted into an encrypted collection");
    let res = assert.commandWorked(edb.eadminCommand({
        bulkWrite: 1,
        ops: [{
            insert: 0,
            document: {
                "_id": 1,
                "middle": "elizondo mountain dew herbert",
            }
        }],
        nsInfo: [{ns: "basic_insert.unencrypted_middle"}]
    }));

    summaryFieldsValidator(
        res, {nErrors: 0, nInserted: 1, nDeleted: 0, nMatched: 0, nModified: 0, nUpserted: 0});
    cursorSizeValidator(res, 1);
    cursorEntryValidator(res.cursor.firstBatch[0], {ok: 1, idx: 0, n: 1});
    client.assertWriteCommandReplyFields(res);

    // Verify it is unencrypted with an unencrypted client
    print("Testing that the document is unencrypted");
    {
        let rawDoc = dbTest.unencrypted_middle.findOne({"_id": 1});
        print(tojson(rawDoc));
        assert.eq(rawDoc["middle"], "elizondo mountain dew herbert");
    }
}

{
    print("Inserting into an unencrypted collection but through the EncryptedClient");
    let res = assert.commandWorked(edb.eadminCommand({
        bulkWrite: 1,
        ops: [
            {
                insert: 0,
                document: {
                    "_id": 1,
                    "first": "dwayne",
                    "middle": "elizondo mountain dew herbert",
                    "aka": "president camacho"
                }
            },
            {
                insert: 0,
                document: {"_id": 2, "first": "bob", "middle": "belcher", "aka": "bobs burgers"}
            }
        ],
        nsInfo: [{ns: "basic_insert.unencrypted"}]
    }));

    summaryFieldsValidator(
        res, {nErrors: 0, nInserted: 2, nDeleted: 0, nMatched: 0, nModified: 0, nUpserted: 0});
    cursorSizeValidator(res, 2);
    cursorEntryValidator(res.cursor.firstBatch[0], {ok: 1, idx: 0, n: 1});
    cursorEntryValidator(res.cursor.firstBatch[1], {ok: 1, idx: 1, n: 1});
    client.assertWriteCommandReplyFields(res);
}
