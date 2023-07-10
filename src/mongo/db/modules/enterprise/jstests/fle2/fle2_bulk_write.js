/**
 * Verify that elements with an bulkWrite command are correctly marked for encryption.
 */

(function() {
"use strict";

load("src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js");
load("src/mongo/db/modules/enterprise/jstests/fle/lib/utils.js");

const mongocryptd = new MongoCryptD();

mongocryptd.start();

const conn = mongocryptd.getConnection();

const schema = {
    encryptionInformation: {
        type: 1,
        schema: {
            "test.default": {
                fields: [{
                    path: "foo",
                    bsonType: "string",
                    queries: {
                        queryType: "equality",
                    },
                    keyId: UUID()
                }]
            },
        }
    }
};

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

let test = {
    schema: schema,
    docs: [{foo: "bar"}, {foo: "baz"}, {bar: "foo"}],
    encryptedPaths: ["foo"],
    notEncryptedPaths: ["bar"]
};

let bulkWriteCommand = {
    bulkWrite: 1,
    ops: [
        {insert: 0, document: test.docs[0]},
        {insert: 0, document: test.docs[1]},
        {insert: 0, document: test.docs[2]},
    ],
    nsInfo: [{ns: "test.default"}]
};
Object.assign(bulkWriteCommand.nsInfo[0], test["schema"]);
const result = assert.commandWorked(conn.adminCommand(bulkWriteCommand));

assert.eq(result.ok, 1, tojson(result));
assert.eq(result.hasEncryptionPlaceholders, true, tojson(result));

for (let encryptedOp of result["result"]["ops"]) {
    let encryptedDoc = encryptedOp["document"];
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

mongocryptd.stop();
}());
