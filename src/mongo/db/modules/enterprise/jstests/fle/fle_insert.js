/**
 * Verify that elements with an insert command are correctly marked for encryption.
 */

import {MongoCryptD} from "src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js";
import {
    fle2Enabled,
    generateSchema,
    generateSchemaV1
} from "src/mongo/db/modules/enterprise/jstests/fle/lib/utils.js";
import {
    kDeterministicAlgo,
    kRandomAlgo
} from "src/mongo/db/modules/enterprise/jstests/fle/lib/utils.js";

const mongocryptd = new MongoCryptD();

mongocryptd.start();

const conn = mongocryptd.getConnection();

const encryptDoc = () =>
    ({encrypt: {algorithm: kDeterministicAlgo, keyId: [UUID(), UUID()], bsonType: "string"}});
const namespace = "test.foo";

const testCases = [
    // Test that a top level encrypt is translated.
    {
        schema: generateSchema({foo: encryptDoc()}, namespace),
        docs: [{foo: "bar"}, {foo: "bar"}],
        encryptedPaths: ["foo"],
        notEncryptedPaths: []
    },
    // Test that only the correct fields are translated.
    {
        schema: generateSchema(
            {foo: encryptDoc(), 'bar.baz': encryptDoc(), 'bar.boo': {type: "string"}}, namespace),
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
            generateSchemaV1({foo: {encrypt: {algorithm: kRandomAlgo, keyId: "/key"}}}, namespace),
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
                               namespace),
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
for (let test of testCases) {
    let insertCommand = {insert: "foo", documents: []};
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
let insertCommand = Object.assign({insert: "foo", documents: [{"foo": "bar"}]},
                                  generateSchema({bar: encryptDoc()}, namespace));

let res = assert.commandWorked(testDb.runCommand(insertCommand));

// Make sure these two fields are not added by the parsers.
assert.eq(false, res.result.hasOwnProperty("ordered"), tojson(res));
assert.eq(false, res.result.hasOwnProperty("bypassDocumentValidation"), tojson(res));

// Explicitly setting them on the command should override the default.
insertCommand = Object.assign({
    insert: "foo",
    documents: [{"foo": "bar"}],
    ordered: false,
    bypassDocumentValidation: true,
},
                              generateSchema({bar: encryptDoc()}, namespace));

res = assert.commandWorked(testDb.runCommand(insertCommand));
assert.eq(res.result.ordered, false, tojson(res));
assert.eq(res.result.bypassDocumentValidation, true, tojson(res));

// Test that a document with a top level Timestamp(0, 0) fails to encrypt.
assert.commandFailedWithCode(
    testDb.runCommand(Object.assign({
        insert: "foo",
        documents: [{"foo": Timestamp(0, 0)}],
    },
                                    generateSchema({"foo": encryptDoc()}, namespace))),
    51129);

// Test that an insert is rejected if a pointer points to an encrypted field.
const pointerDoc = {
    encrypt: {algorithm: kRandomAlgo, keyId: "/key"}
};
assert.commandFailedWithCode(testDb.runCommand({
    insert: "foo",
    documents: [{"foo": "bar", "key": "test"}],
    jsonSchema: {type: "object", properties: {"foo": pointerDoc, "key": encryptDoc()}},
    isRemoteSchema: false
}),
                             30017);

// The following tests show behavior of FLE 1 and FLE 2 when encrypting _id. The restrictions on
// _id with random encryption from FLE 1 must apply to FLE 2 since all fields are encrypted randomly
// in FLE 2 regardless of the queryability.

// Test that a document without _id fails to insert when the schema says to encrypt _id with the
// the deterministic algorithm on FLE 1 or FLE 2.
let encryptSchema = generateSchema(
    {"_id": {queries: {queryType: "equality"}, keyId: UUID(), bsonType: "string"}}, namespace);
assert.commandFailedWithCode(testDb.runCommand(Object.assign({
    insert: "foo",
    documents: [{"foo": "bar"}],
},
                                                             encryptSchema)),
                             fle2Enabled() ? 6316403 : 51130);

// Test that a document without _id fails to insert when the schema says to encrypt _id with the
// the random algorithm on FLE 1 or FLE 2.
encryptSchema = generateSchema({"_id": {keyId: UUID(), bsonType: "string"}}, namespace);
assert.commandFailedWithCode(testDb.runCommand(Object.assign({
    insert: "foo",
    documents: [{"foo": "bar"}],
},
                                                             encryptSchema)),
                             fle2Enabled() ? 6316403 : 51194);

// A nested field under _id is allowed to be encrypted with the deterministic algorithm. In FLE 2,
// all fields are encrypted randomly regardless of the queryability, so this is disallowed.
encryptSchema = generateSchema(
    {"_id.nested": {queries: {queryType: "equality"}, keyId: UUID(), bsonType: "string"}},
    namespace);
if (fle2Enabled()) {
    assert.commandFailedWithCode(testDb.runCommand(Object.assign({
        insert: "foo",
        documents: [{"foo": "bar"}],
    },
                                                                 encryptSchema)),
                                 6316403);
} else {
    assert.commandWorked(testDb.runCommand(Object.assign({
        insert: "foo",
        documents: [{"foo": "bar"}],
    },
                                                         encryptSchema)));
}

// Test that a document without _id fails to insert when the schema says to encrypt a subfiled of
// _id with the the random algorithm on FLE 1 or FLE 2.
encryptSchema = generateSchema({"_id.nested": {keyId: UUID(), bsonType: "string"}}, namespace);
assert.commandFailedWithCode(testDb.runCommand(Object.assign({
    insert: "foo",
    documents: [{"foo": "bar"}],
},
                                                             encryptSchema)),
                             fle2Enabled() ? 6316403 : 51194);

// Test that inserting a document with encrypted data at a path that is marked for encryption,
// fails.
encryptSchema = generateSchema(
    {foo: {encrypt: {algorithm: kRandomAlgo, keyId: [UUID()], bsonType: "string"}}}, namespace);
assert.commandFailedWithCode(testDb.runCommand(Object.assign({
    insert: "foo",
    documents: [{foo: BinData(6, "data")}],
},
                                                             encryptSchema)),
                             31041);

mongocryptd.stop();
