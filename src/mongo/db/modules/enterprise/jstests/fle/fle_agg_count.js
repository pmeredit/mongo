/**
 * Test that mongocryptd can correctly mark the $count agg stage with intent-to-encrypt
 * placeholders.
 */
(function() {
"use strict";

load("src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js");

const mongocryptd = new MongoCryptD();
mongocryptd.start();
const conn = mongocryptd.getConnection();
const testDB = conn.getDB("test");
const coll = testDB.fle_agg_count;

const encryptedStringSpec = {
    encrypt: {
        algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
        keyId: [UUID()],
        bsonType: "string"
    }
};

let command, cmdRes, expectedResult;

// Test that $count marks the projected field as unencrypted even if it overrides an encrypted
// field.
command = {
    aggregate: coll.getName(),
    pipeline: [{$count: "foo"}, {$match: {foo: {$eq: "winterfell"}}}],
    cursor: {},
    jsonSchema: {type: "object", properties: {foo: encryptedStringSpec}},
    isRemoteSchema: false
};
cmdRes = assert.commandWorked(testDB.runCommand(command));
expectedResult = {
    aggregate: coll.getName(),
    pipeline: [
        {$group: {"_id": {"$const": null}, "foo": {"$sum": {"$const": 1}}}},
        {$project: {"_id": false, "foo": true}},
        {$match: {foo: {$eq: "winterfell"}}}
    ],
    cursor: {}
};
delete cmdRes.result.lsid;
assert.eq(expectedResult, cmdRes.result, cmdRes);
assert.eq(false, cmdRes.hasEncryptionPlaceholders, cmdRes);
assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);

// Test that $count marks a previously encrypted field as not encrypted even if it was not
// projected by this stage.
command = {
    aggregate: coll.getName(),
    pipeline: [{$count: "bar"}, {$match: {foo: {$eq: "winterfell"}}}],
    cursor: {},
    jsonSchema: {type: "object", properties: {foo: encryptedStringSpec}},
    isRemoteSchema: false
};
cmdRes = assert.commandWorked(testDB.runCommand(command));
expectedResult = {
    aggregate: coll.getName(),
    pipeline: [
        {$group: {"_id": {"$const": null}, "bar": {"$sum": {"$const": 1}}}},
        {$project: {"_id": false, "bar": true}},
        {$match: {foo: {$eq: "winterfell"}}}
    ],
    cursor: {}
};
delete cmdRes.result.lsid;
assert.eq(expectedResult, cmdRes.result, cmdRes);
assert.eq(false, cmdRes.hasEncryptionPlaceholders, cmdRes);
assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);

mongocryptd.stop();
})();
