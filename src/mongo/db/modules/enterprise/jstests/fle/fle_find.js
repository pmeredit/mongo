/**
 * Basic set of tests to verify the response from mongocryptd for the find command.
 */
(function() {
'use strict';

load("src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js");
load("src/mongo/db/modules/enterprise/jstests/fle/lib/utils.js");

const mongocryptd = new MongoCryptD();
mongocryptd.start();
const conn = mongocryptd.getConnection();
const collName = "test";
const testDB = conn.getDB(collName);

const schema = generateSchema({
    ssn: {encrypt: {algorithm: kDeterministicAlgo, keyId: [UUID()], bsonType: "long"}},
    "user.account": {encrypt: {algorithm: kDeterministicAlgo, keyId: [UUID()], bsonType: "string"}}
},
                              collName);

function assertEncryptedFieldInResponse({filter, path = "", requiresEncryption}) {
    const res = assert.commandWorked(
        testDB.runCommand(Object.assign({find: "test", filter: filter}, schema)));

    assert.eq(res.result.find, "test", tojson(res));
    assert.eq(res.hasEncryptionPlaceholders, requiresEncryption, tojson(res));
    if (path !== "") {
        assert(res.result.filter[path]["$eq"] instanceof BinData, tojson(res));
    }
}

// Basic top-level field in equality correctly marked for encryption.
assertEncryptedFieldInResponse(
    {filter: {ssn: NumberLong(5)}, path: "ssn", requiresEncryption: true});

// Nested field in equality correctly marked for encryption.
assertEncryptedFieldInResponse(
    {filter: {"user.account": "secret"}, path: "user.account", requiresEncryption: true});

// Elements within $in array correctly marked for encryption.
let res = assert.commandWorked(testDB.runCommand(
    Object.assign({find: "test", filter: {ssn: {"$in": [NumberLong(1234)]}}}, schema)));
assert(res.hasEncryptionPlaceholders, tojson(res));
assert(res.result.filter["ssn"]["$in"][0] instanceof BinData, tojson(res));

// Elements within object in $in array correctly marked for encryption.
res = assert.commandWorked(testDB.runCommand(Object.assign({
    find: "test",
    filter: {user: {"$in": [{account: "1234"}]}},
},
                                                           schema)));
assert(res.hasEncryptionPlaceholders, tojson(res));
assert(res.result.filter["user"]["$in"][0]["account"] instanceof BinData, tojson(res));

// Multiple elements inside $in array correctly marked for encryption.
res = assert.commandWorked(testDB.runCommand(Object.assign({
    find: "test",
    filter: {ssn: {"$in": [NumberLong(1), NumberLong(2), NumberLong(3)]}},
},
                                                           schema)));
assert(res.hasEncryptionPlaceholders, tojson(res));
assert(res.result.filter["ssn"]["$in"][0] instanceof BinData, tojson(res));
assert(res.result.filter["ssn"]["$in"][1] instanceof BinData, tojson(res));
assert(res.result.filter["ssn"]["$in"][2] instanceof BinData, tojson(res));

// Mixture of encrypted and non-encrypt elements inside $in array.
res = assert.commandWorked(testDB.runCommand(Object.assign({
    find: "test",
    filter: {user: {"$in": ["notEncrypted", {also: "notEncrypted"}, {account: "encrypted"}]}},
},
                                                           schema)));
assert(res.hasEncryptionPlaceholders, tojson(res));
assert(res.result.filter["user"]["$in"][0] === "notEncrypted", tojson(res));
assert(res.result.filter["user"]["$in"][1]["also"] === "notEncrypted", tojson(res));
assert(res.result.filter["user"]["$in"][2]["account"] instanceof BinData, tojson(res));

// Responses to queries without any encrypted fields should not set the
// 'hasEncryptionPlaceholders' bit.
assertEncryptedFieldInResponse({filter: {}, requiresEncryption: false});
assertEncryptedFieldInResponse({filter: {"user.notSecure": 5}, requiresEncryption: false});
assertEncryptedFieldInResponse(
    {filter: {user: {"$in": [{notSecure: 1}]}}, requiresEncryption: false});

// Invalid operators should fail with an appropriate error code.
assert.commandFailedWithCode(testDB.runCommand(Object.assign({
    find: "test",
    filter: {ssn: {$gt: 5}},
},
                                                             schema)),
                             51118);
assert.commandFailedWithCode(testDB.runCommand(Object.assign({
    find: "test",
    filter: {ssn: /\d/},
},
                                                             schema)),
                             51092);
assert.commandFailedWithCode(testDB.runCommand(Object.assign({
    find: "test",
    filter: {ssn: {$in: [/\d/]}},
},
                                                             schema)),
                             51015);

// Invalid operators with encrypted fields in RHS object should fail.
assert.commandFailedWithCode(testDB.runCommand(Object.assign({
    find: "test",
    filter: {user: {$gt: {account: "5"}}},
},
                                                             schema)),
                             51119);

// Comparison to a null value correctly fails.
assert.commandFailedWithCode(testDB.runCommand(Object.assign({
    find: "test",
    filter: {ssn: null},
},
                                                             schema)),
                             51095);
assert.commandFailedWithCode(testDB.runCommand(Object.assign({
    find: "test",
    filter: {ssn: {$in: [null]}},
},
                                                             schema)),
                             51120);

// Queries on paths which contain an encrypted prefixed field should fail.
assert.commandFailedWithCode(testDB.runCommand(Object.assign({
    find: "test",
    filter: {'ssn.illegal': 5},
},
                                                             schema)),
                             51102);

// Queries on paths which aren't described in the schema AND don't contain an encrypted prefix
// should not fail.
assert.doesNotThrow(() => testDB.runCommand(Object.assign({
    find: "test",
    filter: {'user.nonexistent': 5},
},
                                                          schema)));

// Invalid expressions correctly fail to parse.
assert.commandFailedWithCode(testDB.runCommand(Object.assign({
    find: "test",
    filter: {$cantDoThis: 5},
},
                                                             schema)),
                             ErrorCodes.BadValue);

// Unknown fields correctly result in an error.
assert.commandFailedWithCode(
    testDB.runCommand(Object.assign({find: "test", filter: {}, whatIsThis: 1}, schema)), 40415);

// Invalid type for command parameters correctly result in an error.
assert.commandFailedWithCode(testDB.runCommand(Object.assign({find: 5, filter: {}}, schema)),
                             ErrorCodes.BadValue);
assert.commandFailedWithCode(testDB.runCommand(Object.assign({
    find: "test",
    filter: "not an object",
},
                                                             schema)),
                             ErrorCodes.TypeMismatch);

if (fle2Enabled()) {
    assert.commandFailedWithCode(
        testDB.runCommand({find: "test", filter: {}, encryptionInformation: "not an object"}),
        6327501);
} else {
    assert.commandFailedWithCode(
        testDB.runCommand({find: "test", filter: {}, jsonSchema: "not an object"}), 51090);
}

// Verify that a schema with 'patternProperties' is supported by mongocryptd for the find
// command. 'patternProperties' only exist with jsonSchema, so these tests always run with
// FLE1, even in the FLE2 suite.
let cmdRes = assert.commandWorked(testDB.runCommand({
    find: "test",
    filter: {userSsn: "123-45-6789"},
    jsonSchema: {
        type: "object",
        patternProperties: {
            "[Ss]sn":
                {encrypt: {algorithm: kDeterministicAlgo, keyId: [UUID()], bsonType: "string"}}
        }
    },
    isRemoteSchema: false,
}));
assert(cmdRes.result.filter.userSsn.$eq instanceof BinData, tojson(cmdRes));

// Verify that a find with a field which is encrypted with a JSONPointer keyId fails.
assert.commandFailedWithCode(testDB.runCommand({
    find: "test",
    filter: {userSsn: "123-45-6789"},
    jsonSchema: {
        type: "object",
        properties:
            {userSsn: {encrypt: {algorithm: kDeterministicAlgo, keyId: "/key", bsonType: "string"}}}
    },
    isRemoteSchema: false,
}),
                             31169);

// Verify that a find with a non-queryable algorithm fails.
assert.commandFailedWithCode(testDB.runCommand(Object.assign({
    find: "test",
    filter: {userSsn: "123-45-6789"},
},
                                                             generateSchema({
                                                                 userSsn: {
                                                                     encrypt: {
                                                                         algorithm: kRandomAlgo,
                                                                         keyId: [UUID()],
                                                                         bsonType: "string",
                                                                     }
                                                                 }
                                                             },
                                                                            collName))),
                             [51158, 63165]);

mongocryptd.stop();
})();
