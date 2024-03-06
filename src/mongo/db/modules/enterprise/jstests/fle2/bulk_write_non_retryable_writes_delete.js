/**
 * Test that bulkWrite delete works with FLE2.
 *
 * Some of the tests are incompatible with the transaction overrides since any failure
 * will cause a transaction abortion which will make the overrides infinite loop.
 *
 * The test runs commands that are not allowed with security token: bulkWrite.
 * @tags: [
 *   fle2_no_mongos,
 *   assumes_against_mongod_not_mongos,
 *   not_allowed_with_security_token,
 *   command_not_supported_in_serverless,
 *   does_not_support_retryable_writes,
 *   requires_non_retryable_writes,
 *   does_not_support_transactions,
 *   # TODO SERVER-52419 Remove this tag.
 *   featureFlagBulkWriteCommand,
 * ]
 */
import {EncryptedClient} from "jstests/fle2/libs/encrypted_client_util.js";

load("jstests/libs/doc_validation_utils.js");  // For assertDocumentValidationFailure.
load("jstests/libs/bulk_write_utils.js");      // For cursorEntryValidator.

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
    print("Delete documents using an encrypted filter and multi: true");
    res = assert.commandWorked(edb.adminCommand({
        bulkWrite: 1,
        ops: [{delete: 0, filter: {"first": "dwayne"}, multi: true}],
        nsInfo: [{ns: "basic_update.basic"}]
    }));

    assert.eq(res.numErrors, 0);
    cursorEntryValidator(res.cursor.firstBatch[0], {ok: 1, idx: 0, n: 2});
    assert(!res.cursor.firstBatch[1]);
    client.assertWriteCommandReplyFields(res);

    assert.eq(edb.basic.find({"_id": 1}).toArray().length, 0);
    assert.eq(edb.basic.find({"_id": 2}).toArray().length, 0);
}
