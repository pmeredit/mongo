/**
 * Set of tests to verify the response from mongocryptd for the find command with a projection.
 */
(function() {
'use strict';

load("src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js");
load("src/mongo/db/modules/enterprise/jstests/fle/lib/utils.js");

const mongocryptd = new MongoCryptD();
mongocryptd.start();
const conn = mongocryptd.getConnection();
const testDB = conn.getDB("test");

const sampleSchema = {
    type: "object",
    properties: {
        ssn: {encrypt: {algorithm: kDeterministicAlgo, keyId: [UUID()], bsonType: "long"}},
        user: {
            type: "object",
            properties: {
                account:
                    {encrypt: {algorithm: kDeterministicAlgo, keyId: [UUID()], bsonType: "string"}}
            }
        }
    }
};

// Verify that just including a projection does not cause the analysis to report
// that the output is encrypted.
let res = assert.commandWorked(testDB.runCommand({
    find: "test",
    filter: {},
    projection: {importantMath: {$add: [NumberLong(3), NumberLong(4)]}},
    jsonSchema: sampleSchema,
    isRemoteSchema: false
}));
assert(!res.hasEncryptionPlaceholders, tojson(res));

// Verify that a non-equality comparison results in an error.
assert.commandFailedWithCode(testDB.runCommand({
    find: "test",
    filter: {},
    projection: {isSsnFake: {$gt: ["$ssn", NumberLong(0)]}},
    jsonSchema: sampleSchema,
    isRemoteSchema: false
}),
                             31110);

// Verify projection without query correctly marks fields.
res = assert.commandWorked(testDB.runCommand({
    find: "test",
    filter: {},
    projection: {isSsnFake: {$eq: ["$ssn", NumberLong(0)]}},
    jsonSchema: sampleSchema,
    isRemoteSchema: false
}));
assert(res.hasEncryptionPlaceholders, tojson(res));
assert(res.result.projection["isSsnFake"]["$eq"][1]["$const"] instanceof BinData, tojson(res));

// Verify projection with query correctly marks fields.
res = assert.commandWorked(testDB.runCommand({
    find: "test",
    filter: {"user.account": "secret"},
    projection:
        {isSsnFake: {"$in": ["$ssn", [NumberLong(0), NumberLong(111111111), NumberLong(42)]]}},
    jsonSchema: sampleSchema,
    isRemoteSchema: false
}));
assert(res.hasEncryptionPlaceholders, tojson(res));
assert(res.result.projection["isSsnFake"]["$in"][1][0]["$const"] instanceof BinData, tojson(res));
assert(res.result.projection["isSsnFake"]["$in"][1][1]["$const"] instanceof BinData, tojson(res));
assert(res.result.projection["isSsnFake"]["$in"][1][2]["$const"] instanceof BinData, tojson(res));

mongocryptd.stop();
})();
