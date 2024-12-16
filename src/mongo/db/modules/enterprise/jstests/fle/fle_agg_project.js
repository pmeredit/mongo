/**
 * Tests to verify the correct response for an aggregation with a $project stage.
 */
import {MongoCryptD} from "src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js";
import {
    fle2Enabled,
    generateSchema
} from "src/mongo/db/modules/enterprise/jstests/fle/lib/utils.js";
import {kDeterministicAlgo} from "src/mongo/db/modules/enterprise/jstests/fle/lib/utils.js";

const mongocryptd = new MongoCryptD();
mongocryptd.start();
const conn = mongocryptd.getConnection();
const testDB = conn.getDB("test");
const coll = testDB.fle_agg_project;

const encryptedStringSpec = () =>
    ({encrypt: {algorithm: kDeterministicAlgo, keyId: [UUID()], bsonType: "string"}});

const encryptedUserSpec = generateSchema({user: encryptedStringSpec()}, coll.getFullName());
const encryptedSsnSpec = generateSchema({ssn: encryptedStringSpec()}, coll.getFullName());

let command, cmdRes, expected;

// Basic inclusion test.
command = Object.assign({aggregate: coll.getName(), pipeline: [{$project: {user: 1}}], cursor: {}},
                        encryptedUserSpec);

cmdRes = assert.commandWorked(testDB.runCommand(command));
delete cmdRes.result.lsid;
delete cmdRes.result.encryptionInformation;
expected = {
    aggregate: coll.getName(),
    pipeline: [{$project: {_id: true, user: true}}],
    cursor: {}
};
assert.eq(expected, cmdRes.result, cmdRes.result);
assert(cmdRes.schemaRequiresEncryption, cmdRes);
assert(!cmdRes.hasEncryptionPlaceholders, cmdRes);

// Test that matching on a field that was projected out does not result in an intent-to-encyrpt
// marking.
command =
    Object.assign({
        aggregate: coll.getName(),
        pipeline: [{$project: {user: 1}}, {$match: {removed: "isHere"}}],
        cursor: {}
    },
                  generateSchema({user: encryptedStringSpec(), removed: encryptedStringSpec()},
                                 coll.getFullName()));

cmdRes = assert.commandWorked(testDB.runCommand(command));
delete cmdRes.result.lsid;
delete cmdRes.result.encryptionInformation;
expected = {
    aggregate: coll.getName(),
    pipeline: [{$project: {_id: true, user: true}}, {$match: {removed: {$eq: "isHere"}}}],
    cursor: {}
};
assert.eq(expected, cmdRes.result, cmdRes.result);
assert(cmdRes.schemaRequiresEncryption, cmdRes);
assert(!cmdRes.hasEncryptionPlaceholders);

// Test that matching on a projected, encrypted field does result in an intent-to-encrypt marking.
command = Object.assign({
    aggregate: coll.getName(),
    pipeline: [{$project: {user: 1}}, {$match: {user: "abc"}}],
    cursor: {}
},
                        generateSchema({user: encryptedStringSpec()}, coll.getFullName()));
// TODO SERVER-65346: permit $project with an encrypted field in FLE 2.
if (fle2Enabled()) {
    assert.commandFailedWithCode(testDB.runCommand(command), 31133);
} else {
    cmdRes = assert.commandWorked(testDB.runCommand(command));
    assert(cmdRes.schemaRequiresEncryption, cmdRes);
    assert(cmdRes.hasEncryptionPlaceholders);
}

// Test that matching on a projected field that is a prefix of an encrypted field does result in an
// intent-to-encrypt marking.
command = Object.assign({
    aggregate: coll.getName(),
    pipeline: [{$project: {user: 1}}, {$match: {'user.ssn': "abc"}}],
    cursor: {}
},
                        generateSchema({'user.ssn': encryptedStringSpec()}, coll.getFullName()));
// TODO SERVER-65346: permit $project with an encrypted field in FLE 2.
if (fle2Enabled()) {
    assert.commandFailedWithCode(testDB.runCommand(command), 31133);
} else {
    cmdRes = assert.commandWorked(testDB.runCommand(command));
    assert(cmdRes.schemaRequiresEncryption, cmdRes);
    assert(cmdRes.hasEncryptionPlaceholders);
}

// Test that matching a renamed dotted field does result in an intent-to-encrypt marking.
command = Object.assign({
    aggregate: coll.getName(),
    pipeline: [{$project: {"newUser": "$user.ssn"}}, {$match: {"newUser": "isHere"}}],
    cursor: {}
},
                        generateSchema({'user.ssn': encryptedStringSpec()}, coll.getFullName()));
// FLE 2 has only limited support for referring to encrypted fields in aggregate expressions.
if (fle2Enabled()) {
    assert.commandFailedWithCode(testDB.runCommand(command), 6331102);
} else {
    cmdRes = assert.commandWorked(testDB.runCommand(command));
    assert(cmdRes.schemaRequiresEncryption, cmdRes);
    assert(cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert(cmdRes.result.pipeline[1].$match.newUser.$eq instanceof BinData, cmdRes);
}

// Test projecting an encrypted dotted field path. This is permitted in CSFLE but not allowed in
// FLE2 due to limited support for encrypted fields referenced in aggregate expressions.
command = Object.assign(
    {aggregate: coll.getName(), pipeline: [{$project: {"newUser": "$user.ssn"}}], cursor: {}},
    generateSchema({'user.ssn': encryptedStringSpec()}, coll.getFullName()));
if (fle2Enabled()) {
    assert.commandFailedWithCode(testDB.runCommand(command), 6331102);
} else {
    cmdRes = assert.commandWorked(testDB.runCommand(command));
    assert(cmdRes.schemaRequiresEncryption, cmdRes);
    assert(!cmdRes.hasEncryptionPlaceholders, cmdRes);
}

// Test that renaming an encrypted field to a new dotted field is allowed as long as the field
// is not referenced in subsequent stages.
command = Object.assign(
    {aggregate: coll.getName(), pipeline: [{$project: {"newUser.ssn": "$ssn"}}], cursor: {}},
    encryptedSsnSpec);
cmdRes = assert.commandWorked(testDB.runCommand(command));
assert(cmdRes.schemaRequiresEncryption, cmdRes);
assert(!cmdRes.hasEncryptionPlaceholders, cmdRes);

// Test that adding a new dotted path which is computed from an encrypted is allowed as long as
// the field is not referenced in subsequent stages.
command =
    Object.assign({
        aggregate: coll.getName(),
        pipeline: [{$project: {"newUser.ssn": {$cond: [true, "$ssn", "$otherSsn"]}}}],
        cursor: {}
    },
                  generateSchema({ssn: encryptedStringSpec(), otherSsn: encryptedStringSpec()},
                                 coll.getFullName()));
// FLE 2 has only limited support for referring to encrypted fields in aggregate expressions.
if (fle2Enabled()) {
    assert.commandFailedWithCode(testDB.runCommand(command), 6331102);
} else {
    cmdRes = assert.commandWorked(testDB.runCommand(command));
    assert(cmdRes.schemaRequiresEncryption, cmdRes);
    assert(!cmdRes.hasEncryptionPlaceholders, cmdRes);
}

// Test that accessing a dotted path which is the target of a rename from an encrypted field is
// not allowed. This is because projecting a dotted path *could* access a sub-document within an
// array. In this example, if 'newUser' was an array then we would end up with an encrypted
// field within an object in that array.
command = Object.assign({
    aggregate: coll.getName(),
    pipeline: [{$project: {"newUser.ssn": "$ssn"}}, {$match: {newUser: {ssn: "1234"}}}],
    cursor: {}
},
                        encryptedSsnSpec);
assert.commandFailedWithCode(testDB.runCommand(command), 31133);

// Basic exclusion test.
command =
    Object.assign({aggregate: coll.getName(), pipeline: [{$project: {"user": 0}}], cursor: {}},
                  encryptedUserSpec);

cmdRes = assert.commandWorked(testDB.runCommand(command));
delete cmdRes.result.lsid;
delete cmdRes.result.encryptionInformation;
expected = {
    aggregate: coll.getName(),
    pipeline: [{$project: {user: false, _id: true}}],
    cursor: {}
};
assert.eq(expected, cmdRes.result, cmdRes.result);
assert(cmdRes.schemaRequiresEncryption, cmdRes);
assert(!cmdRes.hasEncryptionPlaceholders);

// Test that exclusion ignores fields left in schema.
command =
    Object.assign({
        aggregate: coll.getName(),
        pipeline: [{$project: {"user": 0}}, {$match: {remaining: "isHere"}}],
        cursor: {}
    },
                  generateSchema({user: encryptedStringSpec(), remaining: encryptedStringSpec()},
                                 coll.getFullName()));

cmdRes = assert.commandWorked(testDB.runCommand(command));
delete cmdRes.result.lsid;
delete cmdRes.result.encryptionInformation;

assert(cmdRes.result.pipeline[1].$match.remaining.$eq instanceof BinData,
       cmdRes.result.pipeline[1].$match.remaining.$eq);
assert(cmdRes.schemaRequiresEncryption, cmdRes);
assert(cmdRes.hasEncryptionPlaceholders);

// Basic expressions test.
command =
    Object.assign({aggregate: coll.getName(), pipeline: [{$project: {"user": "$a"}}], cursor: {}},
                  encryptedUserSpec);

cmdRes = assert.commandWorked(testDB.runCommand(command));
delete cmdRes.result.lsid;
delete cmdRes.result.encryptionInformation;

expected = {
    aggregate: coll.getName(),
    pipeline: [{$project: {"_id": true, user: "$a"}}],
    cursor: {}
};
assert.eq(expected, cmdRes.result, cmdRes.result);

// Test that constants being compared to encrypted fields come back as placeholders.
command = Object.assign({
    aggregate: coll.getName(),
    pipeline: [{$project: {"isTed": {$eq: ["$user", "Ted"]}}}],
    cursor: {}
},
                        encryptedUserSpec);
// TODO SERVER-65296 Support comparisons to encrypted fields under project.
if (fle2Enabled()) {
    assert.commandFailedWithCode(testDB.runCommand(command), 6331102);
} else {
    cmdRes = assert.commandWorked(testDB.runCommand(command));
    delete cmdRes.result.lsid;
    delete cmdRes.result.encryptionInformation;
    assert(cmdRes.result.pipeline[0].$project.isTed.$eq[1]["$const"] instanceof BinData, cmdRes);
}

// Basic $addFields test.
command = Object.assign(
    {aggregate: coll.getName(), pipeline: [{$addFields: {"user": "$ssn"}}], cursor: {}},
    encryptedUserSpec);
cmdRes = assert.commandWorked(testDB.runCommand(command));
delete cmdRes.result.lsid;
delete cmdRes.result.encryptionInformation;

expected = {
    aggregate: coll.getName(),
    pipeline: [{$addFields: {user: "$ssn"}}],
    cursor: {}
};
assert.eq(expected, cmdRes.result, cmdRes.resuld);
assert(cmdRes.schemaRequiresEncryption, cmdRes);
assert(!cmdRes.hasEncryptionPlaceholders);

// Test that $addFields does not affect the schema for other fields.
command = Object.assign({
    aggregate: coll.getName(),
    pipeline: [{$addFields: {"user": "randomString"}}, {$match: {ssn: "isHere"}}],
    cursor: {}
},
                        encryptedSsnSpec);
cmdRes = assert.commandWorked(testDB.runCommand(command));
delete cmdRes.result.lsid;
delete cmdRes.result.encryptionInformation;

assert(cmdRes.schemaRequiresEncryption, cmdRes);
assert(cmdRes.hasEncryptionPlaceholders);
assert(cmdRes.result.pipeline[1].$match.ssn.$eq instanceof BinData,
       cmdRes.result.pipeline[1].$match.ssn.$eq);

// Test that hasEncryptionPlaceholders is properly set without a $match stage.
command = Object.assign({
    aggregate: coll.getName(),
    pipeline: [
        {$addFields: {"userIs123": {"$eq": ["$ssn", "1234"]}}},
    ],
    cursor: {}
},
                        encryptedSsnSpec);
// TODO SERVER-65296 Support comparisons to encrypted fields under project.
if (fle2Enabled()) {
    assert.commandFailedWithCode(testDB.runCommand(command), 6331102);
} else {
    cmdRes = assert.commandWorked(testDB.runCommand(command));
    delete cmdRes.result.lsid;
    delete cmdRes.result.encryptionInformation;

    assert(cmdRes.schemaRequiresEncryption, cmdRes);
    assert(cmdRes.hasEncryptionPlaceholders);
}

command = Object.assign({
    aggregate: coll.getName(),
    pipeline: [
        {$addFields: {"userIs123": {"$eq": ["$foo", "1234"]}}},
    ],
    cursor: {}
},
                        encryptedSsnSpec);
cmdRes = assert.commandWorked(testDB.runCommand(command));
delete cmdRes.result.lsid;
delete cmdRes.result.encryptionInformation;

assert(cmdRes.schemaRequiresEncryption, cmdRes);
assert(!cmdRes.hasEncryptionPlaceholders, cmdRes);

// TODO SERVER-65296 Support comparisons to encrypted fields under project.
if (!fle2Enabled()) {
    const spec = encryptedStringSpec();
    command = Object.assign({
        aggregate: coll.getName(),
        pipeline: [
            {$addFields: {"cleanedSSN": {$ifNull: ["$ssn", "$otherSsn"]}}},
            {$match: {cleanedSSN: "1234"}}
        ],
        cursor: {}
    },
                            generateSchema({ssn: spec, otherSsn: spec}, coll.getFullName()));
    cmdRes = assert.commandWorked(testDB.runCommand(command));
    assert(cmdRes.hasEncryptionPlaceholders);
    assert(cmdRes.schemaRequiresEncryption);
    assert(cmdRes.result.pipeline[1].$match.cleanedSSN.$eq instanceof BinData, cmdRes);
}

mongocryptd.stop();
