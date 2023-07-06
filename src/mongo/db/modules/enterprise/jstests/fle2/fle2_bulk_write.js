/**
 * Verify that elements with an bulkWrite command are correctly marked for encryption.
 */

import {MongoCryptD} from "src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js";

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

let testCases = [
    {
        schema: schema,
        encryptedPaths: ["foo"],
        notEncryptedPaths: ["bar"],
        bulkWriteCommand: {
            bulkWrite: 1,
            ops: [
                {insert: 0, document: {foo: "bar"}},
                {insert: 0, document: {foo: "baz"}},
                {insert: 0, document: {bar: "foo"}},
            ],
            nsInfo: [{ns: "test.default"}]
        }
    },
    {
        schema: schema,
        encryptedPaths: ["foo"],
        notEncryptedPaths: ["bar"],
        bulkWriteCommand: {
            bulkWrite: 1,
            ops: [{
                update: 0,
                filter: {foo: "bar"},
                updateMods: {$set: {foo: "baz", bar: "foo"}},
            }],
            nsInfo: [{ns: "test.default"}]
        }
    }
];

for (let test of testCases) {
    Object.assign(test.bulkWriteCommand.nsInfo[0], test["schema"]);
    const result = assert.commandWorked(conn.adminCommand(test.bulkWriteCommand));

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
}

mongocryptd.stop();