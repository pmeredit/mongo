/**
 * Verifies expected command response from mongocryptd for an aggregation pipeline with $unwind
 * stage.
 */
(function() {
"use strict";

load("src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js");
load("src/mongo/db/modules/enterprise/jstests/fle/lib/utils.js");

const mongocryptd = new MongoCryptD();
mongocryptd.start();
const conn = mongocryptd.getConnection();
const testDB = conn.getDB("test");
const coll = testDB.fle_agg_unwind;

// Encryption of a $unwind stage which does not reference an encrypted field is a no-op,
// regardless of algorithm used.
function testUnwindEncryptionForNotReferencedPath(encryptionSpec) {
    const command = Object.assign(
        {aggregate: coll.getName(), pipeline: [{$unwind: {path: "$foo"}}], cursor: {}},
        generateSchema({ssn: encryptionSpec}, coll.getFullName()));
    const cmdRes = assert.commandWorked(testDB.runCommand(command));
    delete command.jsonSchema;
    delete command.isRemoteSchema;
    delete cmdRes.result.lsid;
    assert.eq(command, cmdRes.result, cmdRes);
    assert.eq(false, cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);
}

//
// Test using deterministic encryption algorithm.
//
(function() {
const encryptedStringSpec = {
    encrypt: {algorithm: kDeterministicAlgo, keyId: [UUID()], bsonType: "string"}
};

const ssnEncryptedSpec = generateSchema({ssn: encryptedStringSpec}, coll.getFullName());

testUnwindEncryptionForNotReferencedPath(encryptedStringSpec);

// Encryption of a $unwind stage that references an encrypted field but uses a deterministic
// algorithm is a no-op. This assumes that encrypted fields are banned from arrays. This operation
// is not allowed in FLE 2 as all fields are randomly encrypted.
let cmdRes;
let command =
    Object.assign({aggregate: coll.getName(), pipeline: [{$unwind: {path: "$ssn"}}], cursor: {}},
                  ssnEncryptedSpec);
if (fle2Enabled()) {
    assert.commandFailedWithCode(testDB.runCommand(command), 31153);
} else {
    cmdRes = assert.commandWorked(testDB.runCommand(command));
    delete command.jsonSchema;
    delete command.isRemoteSchema;
    delete cmdRes.result.lsid;
    assert.eq(command, cmdRes.result, cmdRes);
    assert.eq(false, cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);
}

// A $unwind/$match sequence that references an encrypted field and uses a deterministic
// algorithm is marked for encryption. This operation is not allowed in FLE 2 as all fields are
// randomly encrypted.
command = Object.assign({
    aggregate: coll.getName(),
    pipeline: [{$unwind: {path: "$ssn"}}, {$match: {ssn: {$eq: "123456789"}}}],
    cursor: {}
},
                        ssnEncryptedSpec);
if (fle2Enabled()) {
    assert.commandFailedWithCode(testDB.runCommand(command), 31153);
} else {
    cmdRes = assert.commandWorked(testDB.runCommand(command));
    assert(cmdRes.result.pipeline[1].$match.ssn.$eq instanceof BinData, cmdRes);
    assert.eq(true, cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);
}

// A $unwind that replaces an encrypted field with "includeArrayIndex" will no longer
// consider that field to be encrypted.
command = Object.assign({
    aggregate: coll.getName(),
    pipeline: [{$unwind: {path: "$foo", includeArrayIndex: "ssn"}}, {$match: {ssn: {$eq: 0}}}],
    cursor: {}
},
                        ssnEncryptedSpec);
cmdRes = assert.commandWorked(testDB.runCommand(command));
delete command.jsonSchema;
delete command.isRemoteSchema;
delete cmdRes.result.lsid;
assert.eq(command, cmdRes.result, cmdRes);
assert.eq(false, cmdRes.hasEncryptionPlaceholders, cmdRes);
assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);
})();

//
// Test using random encryption algorithm.
//
(function() {
const encryptedStringSpec = {
    encrypt: {algorithm: kRandomAlgo, keyId: [UUID()], bsonType: "string"}
};

const ssnEncryptedSpec = generateSchema({ssn: encryptedStringSpec}, coll.getFullName());

testUnwindEncryptionForNotReferencedPath(encryptedStringSpec);

// Encryption of a $unwind stage that references an encrypted field using a random algorithm
// is expected to fail.
let command =
    Object.assign({aggregate: coll.getName(), pipeline: [{$unwind: {path: "$ssn"}}], cursor: {}},
                  ssnEncryptedSpec);
assert.commandFailedWithCode(testDB.runCommand(command), 31153);

// Encryption of a $unwind stage that references a prefix of an encrypted field using a
// random algorithm is a no-op.
command = Object.assign({
    aggregate: coll.getName(),
    pipeline: [{$unwind: {path: "$ssnArray"}}, {$match: {ssn: {$eq: 123456789}}}],
    cursor: {}
},
                        generateSchema({'ssnArray.ssn': encryptedStringSpec}, coll.getFullName()));
let cmdRes = assert.commandWorked(testDB.runCommand(command));
delete command.jsonSchema;
delete command.isRemoteSchema;
delete cmdRes.result.lsid;
assert.eq(command, cmdRes.result, cmdRes);
assert.eq(false, cmdRes.hasEncryptionPlaceholders, cmdRes);
assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);

// Encryption of a $unwind stage that references a prefix of an encrypted field using a
// random algorithm, followed by a $match against the encrypted field is expected to fail.
command = Object.assign({
    aggregate: coll.getName(),
    pipeline: [{$unwind: {path: "$ssnArray"}}, {$match: {"ssnArray.ssn": {$eq: 123456789}}}],
    cursor: {}
},
                        generateSchema({'ssnArray.ssn': encryptedStringSpec}, coll.getFullName()));
assert.commandFailedWithCode(testDB.runCommand(command), [51158, 63165]);

// Encryption of a $unwind stage that references a potentially encrypted field fails on
// subsequent match.
command = Object.assign({
    aggregate: coll.getName(),
    pipeline: [
        {$project: {ssn: {$cond: {if: {$eq: ["$a", "$b"]}, then: "foo", else: "$ssn"}}}},
        {$unwind: {path: "$ssn"}},
        {$match: {"ssn": {$eq: 123456789}}}
    ],
    cursor: {}
},
                        ssnEncryptedSpec);
assert.commandFailedWithCode(testDB.runCommand(command), [31133, 6331100]);
})();

mongocryptd.stop();
})();
