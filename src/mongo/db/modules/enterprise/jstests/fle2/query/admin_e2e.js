/**
 * Test administration commands that might contain encrypted fields in filter expressions for FLE2.
 *
 * These end-to-end tests mostly exist to test the happy path and make sure that query analysis is
 * outputting commands that are still valid for mongod to process. Testing edge cases and failure
 * modes for query analysis is covered in fle_collection_validator.js and fle_createindexes.js which
 * explicitly communicate with query analysis in mongocryptd.
 *
 * @tags: [
 *   requires_fcv_70,
 * ]
 */
import {EncryptedClient} from "jstests/fle2/libs/encrypted_client_util.js";

let dbName = 'basic_insert';
let dbTest = db.getSiblingDB(dbName);
dbTest.dropDatabase();

let client = new EncryptedClient(db.getMongo(), dbName);
let edb = client.getDB();

// Ensure encrypted collections can be created.
assert.commandWorked(client.createEncryptionCollection("basic", {
    encryptedFields: {
        "fields": [
            {"path": "first", "bsonType": "string", "queries": {"queryType": "equality"}},
            {"path": "middle", "bsonType": "string"},
            {"path": "aka", "bsonType": "string", "queries": {"queryType": "equality"}},
        ]
    }
}));

// Verify that validators without encrypted fields are acceptable.
assert.commandWorked(client.createEncryptionCollection("unencryptedValidator", {
    encryptedFields: {
        "fields": [
            {"path": "first", "bsonType": "string", "queries": {"queryType": "equality"}},
            {"path": "last", "bsonType": "string"},
            {"path": "aka", "bsonType": "string", "queries": {"queryType": "equality"}},
        ]
    },
    validator: {unencrypted: "abc123"},
}));

// Verify the validator is accepting and rejecting documents properly.
assert.commandFailed(edb.unencryptedValidator.einsert(
    {first: "tony", last: "stark", aka: "iron man", unencrypted: "xyz456"}));

assert.commandWorked(edb.unencryptedValidator.einsert(
    {first: "tony", last: "stark", aka: "iron man", unencrypted: "abc123"}));

// Trying to validate an encrypted field fails at query analysis.
try {
    client.createEncryptionCollection("encryptedValidator", {
        encryptedFields: {
            "fields": [
                {"path": "first", "bsonType": "string", "queries": {"queryType": "equality"}},
                {"path": "middle", "bsonType": "string"},
                {"path": "aka", "bsonType": "string", "queries": {"queryType": "equality"}},
            ]
        },
        validator: {first: "abc123"},
    });
    assert(false, "command succeeded when it should have failed.");
} catch (e) {
    assert(e.message.indexOf("Client Side Field Level Encryption Error") !== -1, e.message);
}

// Run collMod to modify the validator and make sure the new validator is being used when inserting
// documents.
assert.commandWorked(
    edb.erunCommand({collMod: "unencryptedValidator", validator: {unencrypted: "xyz789"}}));

assert.commandFailed(edb.unencryptedValidator.einsert(
    {first: "peter", last: "parker", aka: "spider man", unencrypted: "abc123"}));

assert.commandWorked(edb.unencryptedValidator.einsert(
    {first: "peter", last: "parker", aka: "spider man", unencrypted: "xyz789"}));

// Remove the validator to avoid failing collection validation.
assert.commandWorked(edb.erunCommand({collMod: "unencryptedValidator", validator: {}}));