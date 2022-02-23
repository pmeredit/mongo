/**
 * Verify that elements with an insert command are correctly marked for encryption.
 *
 * TODO SERVER-63275: Enable for FLE 2
 * @tags: [unsupported_fle_2]
 */

(function() {
"use strict";

load("src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js");
load("src/mongo/db/modules/enterprise/jstests/fle/lib/utils.js");

const mongocryptd = new MongoCryptD();

mongocryptd.start();

const conn = mongocryptd.getConnection();

const encryptDoc = {
    encrypt: {algorithm: kDeterministicAlgo, keyId: [UUID(), UUID()], bsonType: "string"}
};
const collName = "test.foo";

const testCases = [
    // Test that a top level encrypt is translated.
    {
        schema: generateSchema({foo: encryptDoc}, collName),
        docs: [{foo: "bar"}, {foo: "bar"}],
        encryptedPaths: ["foo"],
        notEncryptedPaths: []
    },
    // Test that only the correct fields are translated.
    {
        schema: generateSchema(
            {foo: encryptDoc, 'bar.baz': encryptDoc, 'bar.boo': {type: "string"}}, collName),
        docs: [
            {foo: "bar"},
            {stuff: "baz"},
            {foo: "bin", no: "bar", bar: {baz: "stuff", boo: "plaintext"}}
        ],
        encryptedPaths: ["foo", "bar.baz"],
        notEncryptedPaths: ["bar.boo", "stuff", "no"]
    },
    // Test that a JSONPointer keyId is accepted.
    {
        schema:
            generateSchemaV1({foo: {encrypt: {algorithm: kRandomAlgo, keyId: "/key"}}}, collName),
        docs: [{foo: "bar", "key": "string"}],
        encryptedPaths: ["foo"],
        notEncryptedPaths: []
    },
    // Test that a document with a nested Timestamp(0, 0) succeeds.
    {
        schema: generateSchema({
            'foo.bar': {
                encrypt:
                    {algorithm: kDeterministicAlgo, keyId: [UUID(), UUID()], bsonType: "timestamp"}
            }
        },
                               collName),
        docs: [{foo: {bar: Timestamp(0, 0)}}],
        encryptedPaths: ["foo.bar"],
        notEncryptedPaths: []
    },
];

const extractField = function(doc, fieldName) {
    // Find the field.
    const fieldNames = fieldName.split(".");
    let curField = doc;
    for (let field of fieldNames) {
        if (typeof curField === "undefined") {
            return;
        }
        curField = curField[field];
    }
    return curField;
};

const testDb = conn.getDB("test");
let insertCommand = {insert: collName, documents: [], jsonSchema: {}, isRemoteSchema: false};
for (let test of testCases) {
    Object.assign(insertCommand, test["schema"]);
    insertCommand["documents"] = test["docs"];
    const result = assert.commandWorked(testDb.runCommand(insertCommand));
    for (let encryptedDoc of result["result"]["documents"]) {
        // For each field that should be encrypted. Some documents may not contain all of
        // the fields.
        for (let encrypt of test.encryptedPaths) {
            const curField = extractField(encryptedDoc, encrypt);
            if (typeof curField !== "undefined") {
                assert(curField instanceof BinData,
                       tojson(test) + " Failed doc: " + tojson(encryptedDoc));
            }
        }
        // For each field that should not be encrypted. Some documents may not contain all
        // of the fields.
        for (let noEncrypt of test.notEncryptedPaths) {
            const curField = extractField(encryptedDoc, noEncrypt);
            if (typeof curField !== "undefined") {
                assert(!(curField instanceof BinData),
                       tojson(test) + " Failed doc: " + tojson(encryptedDoc));
            }
        }
    }
}

// Make sure that additional command arguments are correctly included in the response.
insertCommand = {
    insert: collName,
    documents: [{"foo": "bar"}],
    jsonSchema: {type: "object", properties: {bar: encryptDoc}},
    isRemoteSchema: false
};

let res = assert.commandWorked(testDb.runCommand(insertCommand));

// Make sure these two fields are not added by the parsers.
assert.eq(false, res.result.hasOwnProperty("ordered"), tojson(res));
assert.eq(false, res.result.hasOwnProperty("bypassDocumentValidation"), tojson(res));

// Explicitly setting them on the command should override the default.
insertCommand = {
    insert: collName,
    documents: [{"foo": "bar"}],
    jsonSchema: {type: "object", properties: {bar: encryptDoc}},
    isRemoteSchema: false,
    ordered: false,
    bypassDocumentValidation: true,
};

res = assert.commandWorked(testDb.runCommand(insertCommand));
assert.eq(res.result.ordered, false, tojson(res));
assert.eq(res.result.bypassDocumentValidation, true, tojson(res));

// Test that a document without _id fails to insert when the schema says encrypt _id.
assert.commandFailedWithCode(testDb.runCommand({
    insert: collName,
    documents: [{"foo": "bar"}],
    jsonSchema: {type: "object", properties: {"_id": encryptDoc}},
    isRemoteSchema: false
}),
                             51130);

// Test that a document with a top level Timestamp(0, 0) fails to encrypt.
assert.commandFailedWithCode(testDb.runCommand({
    insert: collName,
    documents: [{"foo": Timestamp(0, 0)}],
    jsonSchema: {type: "object", properties: {"foo": encryptDoc}},
    isRemoteSchema: false
}),
                             51129);

// Test that command does not fail if a subfield of _id is encrypted.
assert.commandWorked(testDb.runCommand({
    insert: collName,
    documents: [{"foo": "bar"}],
    jsonSchema:
        {type: "object", properties: {"_id": {type: "object", properties: {"nested": encryptDoc}}}},
    isRemoteSchema: false
}));

// Test that an insert is rejected if a pointer points to an encrypted field.
const pointerDoc = {
    encrypt: {algorithm: kRandomAlgo, keyId: "/key"}
};
assert.commandFailedWithCode(testDb.runCommand({
    insert: collName,
    documents: [{"foo": "bar", "key": "test"}],
    jsonSchema: {type: "object", properties: {"foo": pointerDoc, "key": encryptDoc}},
    isRemoteSchema: false
}),
                             30017);
mongocryptd.stop();
}());
