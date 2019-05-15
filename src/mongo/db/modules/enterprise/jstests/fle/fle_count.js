/**
 * Basic set of tests to verify the response from mongocryptd for the count command.
 */

(function() {
    'use strict';

    load("src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js");

    const mongocryptd = new MongoCryptD();
    mongocryptd.start();
    const conn = mongocryptd.getConnection();
    const testDB = conn.getDB("test");

    const sampleSchema = {
        type: "object",
        properties: {
            ssn: {
                encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
                    keyId: [UUID()],
                    bsonType: "int"
                }
            },
            user: {
                type: "object",
                properties: {
                    account: {
                        encrypt: {
                            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
                            keyId: [UUID()],
                            bsonType: "string"
                        }
                    }
                }
            }
        }
    };

    const testCases = [
        // Test that a count with no encrypted fields in filter succeeds.
        {schema: sampleSchema, query: {"foo": 4}, encryptedPaths: [], notEncryptedPaths: ["foo"]},
        // Test that a count with an encrypted field in filter succeeds.
        {
          schema: sampleSchema,
          query: {"ssn": NumberInt(4), "foo": 7},
          encryptedPaths: ["ssn"],
          notEncryptedPaths: ["foo"]
        },
        // Test that a count with no query still succeeds.
        {schema: sampleSchema, query: {}, encryptedPaths: [], notEncryptedPaths: []}
    ];
    let countCommand = {count: "test", query: {}, jsonSchema: {}};

    for (let test of testCases) {
        countCommand["jsonSchema"] = test["schema"];
        countCommand["query"] = test["query"];
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
    countCommand["jsonSchema"] = sampleSchema;
    countCommand["query"] = {user: {account: "5"}};
    let result = assert.commandWorked(testDB.runCommand(countCommand));
    let docResult = result["result"];
    assert(docResult["query"]["user"]["$eq"]["account"] instanceof BinData, docResult);

    // Test that a count with an invalid query type fails.
    countCommand["jsonSchema"] = sampleSchema;
    countCommand["query"] = 5;
    assert.commandFailedWithCode(testDB.runCommand(countCommand), ErrorCodes.TypeMismatch);

    // Test that passthrough fields come back.
    countCommand = {
        count: "test",
        query: {},
        jsonSchema: sampleSchema,
        limit: 1,
        skip: 1,
        hint: "string",
        readConcern: {level: "majority"}
    };

    result = assert.commandWorked(testDB.runCommand(countCommand));
    docResult = result["result"];
    assert.eq(docResult["count"], countCommand["count"]);
    assert.eq(docResult["limit"], countCommand["limit"]);
    assert.eq(docResult["skip"], countCommand["skip"]);
    assert.eq(docResult["hint"], {"$hint": "string"});
    assert.eq(docResult["readConcern"], countCommand["readConcern"]);

    mongocryptd.stop();
})();
