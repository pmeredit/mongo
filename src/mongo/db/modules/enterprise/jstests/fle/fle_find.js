/**
 * Basic set of tests to verify the response from mongocryptd for the find command.
 */
import {MongoCryptD} from "src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js";
import {
    fle2Enabled,
    generateSchema
} from "src/mongo/db/modules/enterprise/jstests/fle/lib/utils.js";
import {
    kDeterministicAlgo,
    kRandomAlgo
} from "src/mongo/db/modules/enterprise/jstests/fle/lib/utils.js";

const mongocryptd = new MongoCryptD();
mongocryptd.start();
const conn = mongocryptd.getConnection();
const testDB = conn.getDB("test");
const namespace = "test.test";

let schema = generateSchema({
    ssn: {encrypt: {algorithm: kDeterministicAlgo, keyId: [UUID()], bsonType: "long"}},
    "user.account": {encrypt: {algorithm: kDeterministicAlgo, keyId: [UUID()], bsonType: "string"}}
},
                            namespace);

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
                             [51118, 6721001]);
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

// Comparing an encrypted field to an object is banned in FLE 1 and 2. bsonType "object" is not
// an allowed type for a field which is queryable.
schema = generateSchema(
    {user: {queries: {queryType: "equality"}, keyId: UUID(), bsonType: "object"}}, namespace);
let findCmd = {find: "test", filter: {user: {ssn: 5}}};
assert.commandFailedWithCode(testDB.runCommand(Object.assign(findCmd, schema)), [31122, 6338405]);

// And attempting to compare an encrypted field to an object will fail if the bson type of the
// field is not "object".
schema = generateSchema(
    {user: {queries: {queryType: "equality"}, keyId: UUID(), bsonType: "string"}}, namespace);
assert.commandFailedWithCode(testDB.runCommand(Object.assign(findCmd, schema)), 31041);

// And of course we can't perform either of the above queries if the field is not queryable.
schema = generateSchema({user: {keyId: UUID(), bsonType: "object"}}, namespace);
assert.commandFailedWithCode(testDB.runCommand(Object.assign(findCmd, schema)), [51158, 63165]);
schema = generateSchema({user: {keyId: UUID(), bsonType: "string"}}, namespace);
assert.commandFailedWithCode(testDB.runCommand(Object.assign(findCmd, schema)), [51158, 63165]);

// Now consider comparing a non-encrypted field to an object. If no elements under the object are
// encrypted, this is permitted in FLE 1 and FLE 2. So, even though 'user.ssn' is encrypted, the
// user can still compare 'user' to an object. If all documents in the collection have the
// encrypted fields present, these queries should return empty result sets.
schema = generateSchema({'user.ssn': {keyId: UUID(), bsonType: "string"}}, namespace);
findCmd.filter = {
    user: 1
};
assert.commandWorked(testDB.runCommand(Object.assign(findCmd, schema)));
findCmd.filter = {
    user: {_id: 1}
};
assert.commandWorked(testDB.runCommand(Object.assign(findCmd, schema)));

//
// But comparing a non-encrypted field to an object where the object DOES contain encrypted
// sub-fields is not permitted in FLE 2 because the server rewrite would be more complicated.
// Note: FLE 1 and FLE 2 differ in behavior here. FLE 1 permits this query.
//
schema = generateSchema({
    'user.ssn': {queries: {queryType: "equality"}, keyId: UUID(), bsonType: "string"},
    'other.nested.encrypted': {queries: {queryType: "equality"}, keyId: UUID(), bsonType: "string"}
},
                        namespace);
const objectWithEncryptedFieldFilters = [
    {user: {ssn: '123456789'}},
    {user: {age: 30, ssn: '123456789'}},
    {user: {$in: ["notEncrypted", {age: 30}, {age: 35, ssn: '123456789'}]}},
    {'other.nested': {encrypted: "abc"}},
    {other: {nested: {encrypted: "abc"}}},
    {'other.nested': {$in: ["notEncrypted", {num: 1}, {num: 2, encrypted: '123456789'}]}},
];
for (const filter of objectWithEncryptedFieldFilters) {
    findCmd.filter = filter;
    if (fle2Enabled()) {
        assert.commandFailedWithCode(testDB.runCommand(Object.assign(findCmd, schema)),
                                     [6341900, 6341901]);
    } else {
        res = assert.commandWorked(testDB.runCommand(Object.assign(findCmd, schema)));
        assert(res.hasEncryptionPlaceholders, tojson(res));
    }
}

// One last interesting test. This query is permitted because it semantically means: return all
// documents where the field 'other' has exactly one subfield, 'nested.encrypted', and that subfield
// is equal to "abc". In the specified schema, we encrypted the subfield 'encrypted' under 'nested'
// under 'other'. So, the query is not against any encrypted fields.
findCmd.filter = {
    other: {'nested.encrypted': "abc"}
};
res = assert.commandWorked(testDB.runCommand(Object.assign(findCmd, schema)));
assert(!res.hasEncryptionPlaceholders, tojson(res));

// Invalid expressions correctly fail to parse.
schema = generateSchema({
    ssn: {encrypt: {algorithm: kDeterministicAlgo, keyId: [UUID()], bsonType: "long"}},
    "user.account": {encrypt: {algorithm: kDeterministicAlgo, keyId: [UUID()], bsonType: "string"}}
},
                        namespace);
assert.commandFailedWithCode(testDB.runCommand(Object.assign({
    find: "test",
    filter: {$cantDoThis: 5},
},
                                                             schema)),
                             ErrorCodes.BadValue);

// Unknown fields correctly result in an error.
assert.commandFailedWithCode(
    testDB.runCommand(Object.assign({find: "test", filter: {}, whatIsThis: 1}, schema)), 40415);

// Invalid type for command parameters correctly results in an error.
assert.commandFailedWithCode(testDB.runCommand(Object.assign({find: 5, filter: {}}, schema)),
                             [ErrorCodes.InvalidNamespace, 6411900]);
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
                                                                            namespace))),
                             [51158, 63165]);

mongocryptd.stop();
