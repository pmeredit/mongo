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

const encryptedUserSsnSpec = generateSchema({'user.ssn': encryptedStringSpec}, coll.getFullName());

let command, cmdRes, expected;

// Test that replacing the root with encrypted field fails.
command = Object.assign(
    {aggregate: coll.getName(), pipeline: [{$replaceRoot: {newRoot: "$user.ssn"}}], cursor: {}},
    encryptedUserSsnSpec);
assert.commandFailedWithCode(testDB.runCommand(command), 31159);

// Test that replacing the root with new field works.
command = Object.assign(
    {aggregate: coll.getName(), pipeline: [{$replaceRoot: {newRoot: "$otherField"}}], cursor: {}},
    encryptedUserSsnSpec);

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
command = Object.assign({
    aggregate: coll.getName(),
    pipeline: [{$replaceRoot: {newRoot: {encryptedField: "$user"}}}],
    cursor: {}
},
                        generateSchema({user: encryptedStringSpec}, coll.getFullName()));
assert.commandFailedWithCode(testDB.runCommand(command), [31110, 6331102]);

// Test that schema is correctly translated after $replaceRoot stage. 'ssn' fields
// will not be an encrypted field after '$replaceRoot' stage.
command = Object.assign({
    aggregate: coll.getName(),
    pipeline: [
        {$replaceRoot: {newRoot: {encryptedField: "$newField"}}},
        {$match: {encryptedField: "isHere"}}
    ],
    cursor: {}
},
                        generateSchema({encryptedField: encryptedStringSpec}, coll.getFullName()));
cmdRes = assert.commandWorked(testDB.runCommand(command));
assert(cmdRes.schemaRequiresEncryption, cmdRes);
assert(!cmdRes.hasEncryptionPlaceholders, cmdRes);

// Test that constants being compared to encrypted fields come back as placeholders.
command = Object.assign({
    aggregate: coll.getName(),
    pipeline: [{$replaceRoot: {newRoot: {"isTed": {$eq: ["$user", "Ted"]}}}}],
    cursor: {}
},
                        generateSchema({user: encryptedStringSpec}, coll.getFullName()));
if (fle2Enabled()) {
    assert.commandFailedWithCode(testDB.runCommand(command), 6331102);
} else {
    cmdRes = assert.commandWorked(testDB.runCommand(command));
    assert(cmdRes.schemaRequiresEncryption, cmdRes);
    assert(cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert(cmdRes.result.pipeline[0].$replaceRoot.newRoot.isTed.$eq[1]["$const"] instanceof BinData,
           cmdRes);
}

mongocryptd.stop();
})();
