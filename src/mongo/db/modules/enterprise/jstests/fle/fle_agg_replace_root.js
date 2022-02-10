/**
 * Tests to verify the correct response for an aggregation with a $replaceRoot stage.
 */
(function() {
"use strict";

load("src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js");
load("src/mongo/db/modules/enterprise/jstests/fle/lib/utils.js");

const mongocryptd = new MongoCryptD();
mongocryptd.start();
const conn = mongocryptd.getConnection();
const testDB = conn.getDB("test");
const coll = testDB.fle_agg_replace_root;

const encryptedStringSpec = {
    encrypt: {algorithm: kDeterministicAlgo, keyId: [UUID()], bsonType: "string"}
};

let command, cmdRes, expected;

// Test that replacing the root with encrypted field fails.
command = {
    aggregate: coll.getName(),
    pipeline: [{$replaceRoot: {newRoot: "$user.ssn"}}],
    cursor: {},
    jsonSchema: {
        type: "object",
        properties: {user: {type: "object", properties: {ssn: encryptedStringSpec}}}
    },
    isRemoteSchema: false,
};
assert.commandFailedWithCode(testDB.runCommand(command), 31159);

// Test that replacing the root with new field works.
command = {
    aggregate: coll.getName(),
    pipeline: [{$replaceRoot: {newRoot: "$otherField"}}],
    cursor: {},
    jsonSchema: {
        type: "object",
        properties: {user: {type: "object", properties: {ssn: encryptedStringSpec}}}
    },
    isRemoteSchema: false,
};

cmdRes = assert.commandWorked(testDB.runCommand(command));
delete cmdRes.result.lsid;
expected = {
    aggregate: coll.getName(),
    pipeline: [{$replaceRoot: {newRoot: "$otherField"}}],
    cursor: {}
};
assert(cmdRes.schemaRequiresEncryption, cmdRes);
assert(!cmdRes.hasEncryptionPlaceholders, cmdRes);

// Test that $replaceRoot stage cannot create new encrypted fields.
command = {
    aggregate: coll.getName(),
    pipeline: [{$replaceRoot: {newRoot: {encryptedField: "$user"}}}],
    cursor: {},
    jsonSchema:
        {type: "object", properties: {user: encryptedStringSpec, remaining: encryptedStringSpec}},
    isRemoteSchema: false,
};
assert.commandFailedWithCode(testDB.runCommand(command), 31110);

// Test that schema is correctly translated after $replaceRoot stage. 'encryptedField' fields
// will not be an encrypted field after '$replaceRoot' stage.
command = {
    aggregate: coll.getName(),
    pipeline: [
        {$replaceRoot: {newRoot: {encryptedField: "$newField"}}},
        {$match: {encryptedField: "isHere"}}
    ],
    cursor: {},
    jsonSchema: {type: "object", properties: {encryptedField: encryptedStringSpec}},
    isRemoteSchema: false,
};
cmdRes = assert.commandWorked(testDB.runCommand(command));
assert(cmdRes.schemaRequiresEncryption, cmdRes);
assert(!cmdRes.hasEncryptionPlaceholders, cmdRes);

// Test that constants being compared to encrypted fields come back as placeholders.
command = {
    aggregate: coll.getName(),
    pipeline: [{$replaceRoot: {newRoot: {"isTed": {$eq: ["$user", "Ted"]}}}}],
    cursor: {},
    jsonSchema: {type: "object", properties: {user: encryptedStringSpec}},
    isRemoteSchema: false,
};
cmdRes = assert.commandWorked(testDB.runCommand(command));
assert(cmdRes.schemaRequiresEncryption, cmdRes);
assert(cmdRes.hasEncryptionPlaceholders, cmdRes);
assert(cmdRes.result.pipeline[0].$replaceRoot.newRoot.isTed.$eq[1]["$const"] instanceof BinData,
       cmdRes);

mongocryptd.stop();
})();
