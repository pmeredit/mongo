/**
 * Basic set of tests to verify the response from mongocryptd for the count command.
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
const collName = "test";

const schema = generateSchema({
    ssn: {encrypt: {algorithm: kDeterministicAlgo, keyId: [UUID()], bsonType: "int"}},
    "user.account": {encrypt: {algorithm: kDeterministicAlgo, keyId: [UUID()], bsonType: "string"}}
},
                              "test.test");

const testCases = [
    // Test that a count with no encrypted fields in filter succeeds.
    {schema: schema, query: {"foo": 4}, encryptedPaths: [], notEncryptedPaths: ["foo"]},
    // Test that a count with an encrypted field in filter succeeds.
    {
        schema: schema,
        query: {"ssn": NumberInt(4), "foo": 7},
        encryptedPaths: ["ssn"],
        notEncryptedPaths: ["foo"]
    },
    // Test that a count with no query still succeeds.
    {schema: schema, query: {}, encryptedPaths: [], notEncryptedPaths: []}
];

for (let test of testCases) {
    let countCommand = {count: collName, query: test["query"]};
    countCommand = Object.assign(countCommand, test["schema"]);
    const result = assert.commandWorked(testDB.runCommand(countCommand));
    assert.eq(true, result["schemaRequiresEncryption"]);
    if (test["encryptedPaths"].length >= 1) {
        assert.eq(true, result["hasEncryptionPlaceholders"]);
    }
    const queryDoc = result["result"]["query"];
    // For each field that should be encrypted. Some documents may not contain all of the
    // fields.
    for (let encrypt of test.encryptedPaths) {
        if (queryDoc.hasOwnProperty(encrypt)) {
            assert(queryDoc[encrypt] instanceof BinData, queryDoc);
        }
    }
    // For each field that should not be encrypted. Some documents may not contain all of
    // the fields.
    for (let noEncrypt of test.notEncryptedPaths) {
        if (queryDoc.hasOwnProperty(noEncrypt)) {
            assert(!(queryDoc[noEncrypt] instanceof BinData), queryDoc);
        }
    }
}

// Test that a nested encrypted field in a query succeeds.
let result = assert.commandWorked(
    testDB.runCommand(Object.assign({count: collName, query: {'user.account': "5"}}, schema)));
let docResult = result["result"];
assert(docResult["query"]["user.account"]["$eq"] instanceof BinData, docResult);

if (!fle2Enabled()) {
    // In FLE 1, we can also do a comparison to an object containing an encrypted payload. This is
    // not permitted in FLE 2.
    result = assert.commandWorked(
        testDB.runCommand(Object.assign({count: collName, query: {user: {account: "5"}}}, schema)));
    docResult = result["result"];
    assert(docResult["query"]["user"]["$eq"]["account"] instanceof BinData, docResult);
}

// Test that a count with an invalid query type fails.
assert.commandFailedWithCode(testDB.runCommand(Object.assign({count: collName, query: 5}, schema)),
                             ErrorCodes.TypeMismatch);

// Test that passthrough fields come back.
let countCommand = {
    count: collName,
    query: {},
    limit: 1,
    skip: 1,
    hint: "string",
    readConcern: {level: "majority"}
};

result = assert.commandWorked(testDB.runCommand(Object.assign(countCommand, schema)));
docResult = result["result"];
assert.eq(docResult["count"], countCommand["count"]);
assert.eq(docResult["limit"], countCommand["limit"]);
assert.eq(docResult["skip"], countCommand["skip"]);
assert.eq(docResult["hint"], {"$hint": "string"});
assert.eq(docResult["readConcern"], countCommand["readConcern"]);

mongocryptd.stop();
