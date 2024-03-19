/**
 * Test that bulkWrite delete works with FLE2.
 *
 * Some of the tests are incompatible with the transaction overrides since any failure
 * will cause a transaction abortion which will make the overrides infinite loop.
 *
 * The test runs commands that are not allowed with security token: bulkWrite.
 * @tags: [
 *   not_allowed_with_signed_security_token,
 *   command_not_supported_in_serverless,
 *   does_not_support_transactions,
 *   assumes_read_preference_unchanged,
 *   requires_fcv_80
 * ]
 */
import {EncryptedClient} from "jstests/fle2/libs/encrypted_client_util.js";
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

let insert1 = {
    "insert": "basic",
    documents: [{
        "_id": 1,
        "first": "dwayne",
        "middle": "elizondo mountain dew herbert",
        "aka": "president camacho",
        "age": 33
    }]
};

// Insert 1 document with a field that gets encrypted, so following bulkWrite can update it.
let res = assert.commandWorked(edb.runCommand(insert1));
assert.eq(res.n, 1);

let insert2 = {
    "insert": "basic",
    documents:
        [{"_id": 2, "first": "dwayne", "middle": "schrute", "aka": "regional manager", "age": 53}]
};

res = assert.commandWorked(edb.runCommand(insert2));
assert.eq(res.n, 1);

{
    print("Delete a document using an encrypted filter");
    res = assert.commandWorked(edb.adminCommand({
        bulkWrite: 1,
        ops: [{delete: 0, filter: {"first": "dwayne", "_id": 1}}],
        nsInfo: [{ns: "basic_update.basic"}]
    }));

    summaryFieldsValidator(
        res, {nErrors: 0, nInserted: 0, nDeleted: 1, nMatched: 0, nModified: 0, nUpserted: 0});
    cursorSizeValidator(res, 1);
    cursorEntryValidator(res.cursor.firstBatch[0], {ok: 1, idx: 0, n: 1});
    client.assertWriteCommandReplyFields(res);
    assert.eq(edb.basic.find({"_id": 1}).toArray().length, 0);
}

res = assert.commandWorked(edb.runCommand(insert1));
assert.eq(res.n, 1);

{
    print("Delete a document using an unencrypted filter");
    res = assert.commandWorked(edb.adminCommand({
        bulkWrite: 1,
        ops: [{delete: 0, filter: {"age": 33}}],
        nsInfo: [{ns: "basic_update.basic"}]
    }));

    summaryFieldsValidator(
        res, {nErrors: 0, nInserted: 0, nDeleted: 1, nMatched: 0, nModified: 0, nUpserted: 0});
    cursorSizeValidator(res, 1);
    cursorEntryValidator(res.cursor.firstBatch[0], {ok: 1, idx: 0, n: 1});
    client.assertWriteCommandReplyFields(res);
    assert.eq(edb.basic.find({"_id": 1}).toArray().length, 0);
}

res = assert.commandWorked(edb.runCommand(insert1));
assert.eq(res.n, 1);

{
    print("Delete a document using an encrypted filter and Let");
    let error = assert.throws(() => edb.adminCommand({
        bulkWrite: 1,
        ops: [{
            delete: 0,
            filter: {$expr: {$eq: ["$first", "$$targetKey"]}},
        }],
        nsInfo: [{ns: "basic_update.basic"}],
        let : {targetKey: "dwayne"}
    }));

    assert.commandFailed(error);
    assert.eq(error.reason,
              "Client Side Field Level Encryption Error:Use of undefined variable: targetKey");
}

// Previous test is supposed to fail to delete so the document should still exist.
assert.eq(edb.basic.find({"_id": 1}).toArray().length, 1);

{
    print("Delete documents using an encrypted filter (multiple ops)");

    let error = assert.throws(() => edb.adminCommand({
        bulkWrite: 1,
        ops: [
            {delete: 0, filter: {"first": "dwayne", "_id": 1}},
            {delete: 0, filter: {"first": "dwayne", "_id": 2}}
        ],
        nsInfo: [{ns: "basic_update.basic"}]
    }));

    assert.commandFailedWithCode(error, ErrorCodes.BadValue);
    assert.eq(
        error.reason,
        "Client Side Field Level Encryption Error:Only insert is supported in BulkWrite with multiple operations and Queryable Encryption.");
}

// Previous test is supposed to fail to delete so the document should still exist.
assert.eq(edb.basic.find({"_id": 1}).toArray().length, 1);

{
    print("Delete a document with fields that get encrypted and collation");
    res = assert.commandWorked(edb.adminCommand({
        bulkWrite: 1,
        ops: [{
            delete: 0,
            filter: {"_id": 1},
            collation: {locale: "en_US", strength: 1},
        }],
        nsInfo: [{ns: "basic_update.basic"}]
    }));

    summaryFieldsValidator(
        res, {nErrors: 0, nInserted: 0, nDeleted: 1, nMatched: 0, nModified: 0, nUpserted: 0});
    cursorSizeValidator(res, 1);
    cursorEntryValidator(res.cursor.firstBatch[0], {ok: 1, idx: 0, n: 1});
    client.assertWriteCommandReplyFields(res);
    assert.eq(edb.basic.find({"_id": 1}).toArray().length, 0);
}

res = assert.commandWorked(edb.runCommand(insert1));
assert.eq(res.n, 1);

// Not testing "Delete with multi and fields that get encrypted" here,
// bulk_write_non_retryable_writes_delete.js instead.

{
    print("Delete a document with fields that get encrypted and collation (used in the filter)");
    let error = assert.throws(() => edb.adminCommand({
        bulkWrite: 1,
        ops: [{
            delete: 0,
            filter: {"first": "dwayne", "_id": 1},
            collation: {locale: "en_US", strength: 1},
        }],
        nsInfo: [{ns: "basic_update.basic"}]
    }));

    assert.commandFailedWithCode(error, 31054);
    assert.eq(
        error.reason,
        "Client Side Field Level Encryption Error:cannot apply non-simple collation when comparing to element first: \"dwayne\" with client-side encryption");
}
