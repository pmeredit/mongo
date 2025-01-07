/**
 * Test that bulkWrite update works with FLE2.
 *
 * Some of the tests are incompatible with the transaction overrides since any failure
 * will cause a transaction abortion which will make the overrides infinite loop.
 *
 * The test runs commands that are not allowed with security token: bulkWrite.
 * @tags: [
 *   not_allowed_with_signed_security_token,
 *   command_not_supported_in_serverless,
 *   does_not_support_transactions,
 *   requires_fcv_81
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

let dbName = 'basic_update';
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

var expected_pre_image = {
    "_id": 1,
    "first": "dwayne",
    "middle": "elizondo mountain dew herbert",
    "aka": "president camacho"
};

// Insert 1 document with a field that gets encrypted, so following bulkWrite can update it.
let res =
    assert.commandWorked(edb.erunCommand({"insert": "basic", documents: [expected_pre_image]}));
assert.eq(res.n, 1);

assert.commandWorked(client.createEncryptionCollection("unencrypted_middle", {
    validator: {$jsonSchema: {required: ["middle"]}},
    encryptedFields: {
        "fields": [
            {"path": "first", "bsonType": "string", "queries": {"queryType": "equality"}},
            {"path": "aka", "bsonType": "string", "queries": {"queryType": "equality"}},
        ]
    }
}));

assert.commandWorked(edb.eadminCommand({
    bulkWrite: 1,
    ops: [{insert: 0, document: {"_id": 2, "middle": "BBB"}}],
    nsInfo: [{ns: "basic_update.unencrypted_middle"}]
}));

{
    print("Update a document with fields that get encrypted");
    res = assert.commandWorked(edb.eadminCommand({
        bulkWrite: 1,
        ops: [{update: 0, filter: {_id: 1}, updateMods: {$set: {middle: "E"}}}],
        nsInfo: [{ns: "basic_update.basic"}]
    }));

    summaryFieldsValidator(
        res, {nErrors: 0, nInserted: 0, nDeleted: 0, nMatched: 1, nModified: 1, nUpserted: 0});
    cursorSizeValidator(res, 1);
    cursorEntryValidator(res.cursor.firstBatch[0], {ok: 1, idx: 0, n: 1, nModified: 1});
    client.assertWriteCommandReplyFields(res);

    // Verify it is encrypted with an unencrypted client
    print("Testing that all documents are encrypted");
    {
        let rawDoc = dbTest.basic.find({"_id": 1}).toArray()[0];
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
        assert.eq(doc["middle"], "E");
        assert.eq(doc["aka"], "president camacho");
        assert(doc[kSafeContentField] !== undefined);
    }
}

{
    print("Update a document with fields that get encrypted (2 namespaces)");
    let error = assert.throws(() => edb.eadminCommand({
        bulkWrite: 1,
        ops: [{update: 0, filter: {_id: 1}, updateMods: {$set: {middle: "E"}}}],
        nsInfo: [{ns: "basic_update.basic"}, {ns: "basic_update.other"}]
    }));

    assert.commandFailedWithCode(error, ErrorCodes.BadValue);
    assert.eq(error.reason, "BulkWrite with Queryable Encryption supports only a single namespace");
}

{
    print("Update 2 documents with fields that get encrypted");
    let error = assert.throws(() => edb.eadminCommand({
        bulkWrite: 1,
        ops: [
            {update: 0, filter: {_id: 1}, updateMods: {$set: {middle: "E"}}},
            {update: 0, filter: {_id: 2}, updateMods: {$set: {middle: "B"}}}
        ],
        nsInfo: [{ns: "basic_update.basic"}]
    }));

    assert.commandFailedWithCode(error, ErrorCodes.BadValue);
    assert.eq(
        error.reason,
        "Client Side Field Level Encryption Error:Only insert is supported in BulkWrite with multiple operations and Queryable Encryption.");
}

{
    print("Insert + Update with fields that get encrypted");
    let error = assert.throws(() => edb.eadminCommand({
        bulkWrite: 1,
        ops: [
            {
                insert: 0,
                document: {"_id": 3, "first": "tina", "aka": "belcher"},
            },
            {update: 0, filter: {_id: 1}, updateMods: {$set: {middle: "E"}}}
        ],
        nsInfo: [{ns: "basic_update.basic"}]
    }));

    assert.commandFailedWithCode(error, ErrorCodes.BadValue);
    assert.eq(
        error.reason,
        "Client Side Field Level Encryption Error:Only insert is supported in BulkWrite with multiple operations and Queryable Encryption.");
}

{
    print("Update with upsert and fields that get encrypted");

    // We want to insert _id: 2, ensure it does not exist yet.
    assert.eq(edb.basic.ecount(), 1);

    res = assert.commandWorked(edb.eadminCommand({
        bulkWrite: 1,
        ops: [{
            update: 0,
            filter: {_id: 2},
            updateMods: {$set: {"first": "bob", "middle": "belcher", "aka": "bobs burgers"}},
            upsert: true
        }],
        nsInfo: [{ns: "basic_update.basic"}]
    }));

    assert.eq(res.nErrors, 0);
    cursorSizeValidator(res, 1);
    cursorEntryValidator(res.cursor.firstBatch[0],
                         {ok: 1, idx: 0, n: 1, nModified: 0, upserted: {_id: 2}});
    client.assertWriteCommandReplyFields(res);

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
    }

    // Verify we decrypt it clean with an encrypted client.
    print("Testing that all documents can be decrypted by an encrypted client");
    {
        let doc = edb.basic.efindOne({"_id": 1});
        print(tojson(doc));
        assert.eq(doc["first"], "dwayne");
        assert.eq(doc["middle"], "E");
        assert.eq(doc["aka"], "president camacho");
        assert(doc[kSafeContentField] !== undefined);

        doc = edb.basic.efindOne({"_id": 2});
        print(tojson(doc));
        assert.eq(doc["first"], "bob");
        assert.eq(doc["middle"], "belcher");
        assert.eq(doc["aka"], "bobs burgers");
        assert(doc[kSafeContentField] !== undefined);
    }
}

{
    print("Update a document with fields that get encrypted (missing required field)");
    res = assert.commandWorked(edb.eadminCommand({
        bulkWrite: 1,
        ops: [{
            update: 0,
            filter: {_id: 3},
            updateMods: {$set: {"first": "linda", "middle": "belcher"}},
            upsert: true
        }],
        nsInfo: [{ns: "basic_update.basic"}]
    }));

    summaryFieldsValidator(
        res, {nErrors: 1, nInserted: 0, nDeleted: 0, nMatched: 0, nModified: 0, nUpserted: 0});
    cursorSizeValidator(res, 1);
    cursorEntryValidator(
        res.cursor.firstBatch[0],
        {ok: 0, idx: 0, n: 0, nModified: 0, code: ErrorCodes.DocumentValidationFailure});
}

{
    print("Update a document with fields that get encrypted (with Let)");

    let error = assert.throws(() => edb.eadminCommand({
        bulkWrite: 1,
        ops: [
            {
                update: 0,
                filter: {$expr: {$eq: ["$first", "$$targetKey"]}},
                updateMods: {$set: {"middle": "El"}}
            },
        ],
        nsInfo: [{ns: "basic_update.basic"}],
        let : {targetKey: "dwayne"}
    }));

    assert.commandFailed(error);
    assert.eq(error.reason,
              "Client Side Field Level Encryption Error:Use of undefined variable: targetKey");
}

{
    // Note: If we start supporting this, some care might be needed around partial success due to
    // SERVER-15292 and the way we handle replies in bulk_write_exec.cpp (see comment there after
    // LOGV2_WARNING(7749700)).
    print("Update with multi and fields that get encrypted");
    let error = assert.throws(() => edb.eadminCommand({
        bulkWrite: 1,
        ops: [{
            update: 0,
            filter: {_id: {$lte: 1}},
            updateMods: {$set: {middle: "elizondo"}},
            multi: true
        }],
        nsInfo: [{ns: "basic_update.basic"}]
    }));

    assert.commandFailedWithCode(error, 6329900);
    assert.eq(
        error.reason,
        "Client Side Field Level Encryption Error:Multi-document updates are not allowed with Queryable Encryption");
}

{
    print("Update a document with fields that get encrypted (with Constants)");
    let error = assert.throws(() => edb.eadminCommand({
        bulkWrite: 1,
        ops: [{
            update: 0,
            filter: {$expr: {$eq: ["$first", "$$targetKey"]}},
            updateMods: {$set: {"middle": "El"}},
            constants: {targetKey: "dwayne"}
        }],
        nsInfo: [{ns: "basic_update.basic"}]
    }));

    assert.commandFailed(error);
    assert.eq(error.reason,
              "Client Side Field Level Encryption Error:Use of undefined variable: targetKey");
}

{
    expected_pre_image = {
        "_id": 3,
        "first": "linda",
        "middle": "belcher",
        "aka": "linda",
        "unencrypted": [{b: 5}, {b: 1}, {b: 2}]
    };

    print(
        "Update a document with fields that get encrypted and unencrypted field with arrayFilters");
    res =
        assert.commandWorked(edb.erunCommand({"insert": "basic", documents: [expected_pre_image]}));
    assert.eq(res.n, 1);

    res = assert.commandWorked(edb.eadminCommand({
        bulkWrite: 1,
        ops: [{
            update: 0,
            filter: {_id: 3},
            updateMods: {$set: {"unencrypted.$[i].b": 6}},
            arrayFilters: [{"i.b": 5}]
        }],
        nsInfo: [{ns: "basic_update.basic"}]
    }));

    summaryFieldsValidator(
        res, {nErrors: 0, nInserted: 0, nDeleted: 0, nMatched: 1, nModified: 1, nUpserted: 0});
    cursorSizeValidator(res, 1);
    cursorEntryValidator(res.cursor.firstBatch[0], {ok: 1, idx: 0, n: 1, nModified: 1});
    client.assertWriteCommandReplyFields(res);

    // Verify it is encrypted with an unencrypted client
    print("Testing that the document is encrypted");
    {
        let rawDoc = dbTest.basic.find({"_id": 3}).toArray()[0];
        print(tojson(rawDoc));
        assertIsIndexedEncryptedField(rawDoc["first"]);
        assertIsUnindexedEncryptedField(rawDoc["middle"]);
        assertIsIndexedEncryptedField(rawDoc["aka"]);
        assert(rawDoc[kSafeContentField] !== undefined);
    }

    // Verify we decrypt it clean with an encrypted client.
    print("Testing that the document can be decrypted by an encrypted client");
    {
        let doc = edb.basic.efindOne({"_id": 3});
        print(tojson(doc));
        assert.eq(doc["first"], "linda");
        assert.eq(doc["middle"], "belcher");
        assert.eq(doc["aka"], "linda");
        assert.eq(doc["unencrypted"], [{b: 6}, {b: 1}, {b: 2}]);
        assert(doc[kSafeContentField] !== undefined);
    }
}

{
    print("QE does not support encrypted array field.");
    // If that changes, update this test by changing the print, the asserts
    // AND testing bulkWrite with arrayFilters on encrypted field.
    let error = assert.throws(() => edb.erunCommand({
        "insert": "basic",
        documents:
            [{"_id": 4, "first": "linda", "middle": "belcher", "aka": [{b: 5}, {b: 1}, {b: 2}]}]
    }));

    assert.commandFailedWithCode(error, 31041);
    assert.eq(error.reason,
              "Client Side Field Level Encryption Error:Cannot encrypt element of type: array");
}

{
    print("Update a document with fields that get encrypted and collation");
    res = assert.commandWorked(edb.eadminCommand({
        bulkWrite: 1,
        ops: [{
            update: 0,
            filter: {_id: 1},
            updateMods: {$set: {middle: "elizondo"}},
            collation: {locale: "en_US", strength: 1}
        }],
        nsInfo: [{ns: "basic_update.basic"}]
    }));

    summaryFieldsValidator(
        res, {nErrors: 0, nInserted: 0, nDeleted: 0, nMatched: 1, nModified: 1, nUpserted: 0});
    cursorSizeValidator(res, 1);
    cursorEntryValidator(res.cursor.firstBatch[0], {ok: 1, idx: 0, n: 1, nModified: 1});
    client.assertWriteCommandReplyFields(res);

    expected_pre_image = {"_id": 1, "first": "dwayne", "middle": "E", "aka": "president camacho"};

    // Verify it is encrypted with an unencrypted client
    print("Testing that all documents are encrypted");
    {
        let rawDoc = dbTest.basic.find({"_id": 1}).toArray()[0];
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
        assert.eq(doc["middle"], "elizondo");
        assert.eq(doc["aka"], "president camacho");
        assert(doc[kSafeContentField] !== undefined);
    }
}

{
    print("Update a document with fields that get encrypted and collation (used in the filter)");
    let error = assert.throws(() => edb.eadminCommand({
        bulkWrite: 1,
        ops: [{
            update: 0,
            filter: {first: "dwayne"},
            updateMods: {$set: {middle: "elizondo"}},
            collation: {locale: "en_US", strength: 1}
        }],
        nsInfo: [{ns: "basic_update.basic"}]
    }));

    assert.commandFailedWithCode(error, 31054);
    assert.eq(
        error.reason,
        "Client Side Field Level Encryption Error:cannot apply non-simple collation when comparing to element first: \"dwayne\" with client-side encryption");
}

{
    print(
        "Update a document with no encrypted field in a collection with optional encrypted fields");

    let res = assert.commandWorked(edb.eadminCommand({
        bulkWrite: 1,
        ops: [{update: 0, filter: {_id: 2}, updateMods: {$set: {middle: "E"}}}],
        nsInfo: [{ns: "basic_update.unencrypted_middle"}],
    }));

    expected_pre_image = {"_id": 2, "middle": "BBB"};
    summaryFieldsValidator(
        res, {nErrors: 0, nInserted: 0, nDeleted: 0, nMatched: 1, nModified: 1, nUpserted: 0});
    cursorSizeValidator(res, 1);
    cursorEntryValidator(res.cursor.firstBatch[0], {ok: 1, idx: 0, n: 1, nModified: 1});
    client.assertWriteCommandReplyFields(res);
}

{
    print("Make sure writeConcern errors are handled correctly in bulkWrite FLE");

    let res = edb.eadminCommand({
        bulkWrite: 1,
        ops: [{update: 0, filter: {_id: 2}, updateMods: {$set: {middle: "E"}}}],
        nsInfo: [{ns: "basic_update.unencrypted_middle"}],
        writeConcern: {w: 50, j: false, wtimeout: 100},
    });

    assert.commandFailed(res);

    assert.eq(res.writeConcernError.code, 100, "writeConcern error not detected in " + tojson(res));
    summaryFieldsValidator(
        res, {nErrors: 0, nInserted: 0, nDeleted: 0, nMatched: 1, nModified: 1, nUpserted: 0});
    cursorSizeValidator(res, 1);
    cursorEntryValidator(res.cursor.firstBatch[0], {ok: 1, idx: 0, n: 1, nModified: 1});
    client.assertWriteCommandReplyFields(res);
}
