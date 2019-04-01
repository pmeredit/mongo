/**
 * Verify that updates to encrypted fields are correctly marked for encryption.
 */
(function() {
    "use strict";

    load("src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js");

    const mongocryptd = new MongoCryptD();

    mongocryptd.start();

    const conn = mongocryptd.getConnection();

    const encryptDoc = {
        encrypt: {
            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
            initializationVector: BinData(0, "ASNFZ4mrze/ty6mHZUMhAQ=="),
            keyId: [UUID(), UUID()],
            bsonType: "string"
        }
    };

    const testCases = [
        // Test that a top level field is encrypted.
        {
          schema: {type: "object", properties: {foo: encryptDoc}},
          updates: [{q: {}, u: {"$set": {"foo": "2"}}}],
          encryptedPaths: ["foo"],
          notEncryptedPaths: []
        },
        // Test that a dotted field is encrypted.
        {
          schema:
              {type: "object", properties: {foo: {type: "object", properties: {bar: encryptDoc}}}},
          updates: [{q: {}, u: {"$set": {"foo.bar": "2"}}}],
          encryptedPaths: ["foo.bar"],
          notEncryptedPaths: ["foo"]
        },
        // Test that multiple correct fields are encrypted.
        {
          schema: {
              type: "object",
              properties: {foo: {type: "object", properties: {bar: encryptDoc}}, baz: encryptDoc}
          },
          updates: [{q: {}, u: {"$set": {"foo.bar": "2", "baz": 5, "plain": 7}}}],
          encryptedPaths: ["foo.bar", "baz"],
          notEncryptedPaths: ["plain"]
        },
        // Test that an update path with a numeric path component works properly. The
        // schema indicates that the numeric path component is a field name, not an array
        // index.
        {
          schema:
              {type: "object", properties: {foo: {type: "object", properties: {1: encryptDoc}}}},
          updates: [{q: {}, u: {"$set": {"foo.1": 3}}}],
          encryptedPaths: ["foo.1"],
          notEncryptedPaths: []
        },
        // Test a basic multi-statement update.
        {
          schema: {type: "object", properties: {foo: encryptDoc, bar: encryptDoc}},
          updates: [{q: {}, u: {"$set": {"foo": 3}}}, {q: {}, u: {"$set": {"bar": 2}}}],
          encryptedPaths: ["foo", "bar"],
          notEncryptedPaths: []
        },

    ];

    const testDb = conn.getDB("test");
    let updateCommand = {update: "test", updates: [], jsonSchema: {}};

    for (let test of testCases) {
        updateCommand["jsonSchema"] = test["schema"];
        updateCommand["updates"] = test["updates"];
        const result = assert.commandWorked(testDb.runCommand(updateCommand));
        if (test["encryptedPaths"].length >= 1) {
            assert.eq(true, result["hasEncryptionPlaceholders"]);
        }
        for (let encryptedDoc of result["result"]["updates"]) {
            const realUpdate = encryptedDoc["u"]["$set"];
            // For each field that should be encrypted. Some documents may not contain all of the
            // fields.
            for (let encrypt of test.encryptedPaths) {
                if (realUpdate.hasOwnProperty(encrypt)) {
                    assert(realUpdate[encrypt] instanceof BinData, tojson(realUpdate));
                }
            }
            // For each field that should not be encrypted. Some documents may not contain all of
            // the fields.
            for (let noEncrypt of test.notEncryptedPaths) {
                if (realUpdate.hasOwnProperty(noEncrypt)) {
                    assert(!(realUpdate[noEncrypt] instanceof BinData), tojson(realUpdate));
                }
            }
        }
    }
    // Test that fields in q get replaced.
    updateCommand["jsonSchema"] = {type: "object", properties: {foo: encryptDoc, bar: encryptDoc}};
    updateCommand["updates"] = [{q: {bar: 5}, u: {"$set": {"foo": "2"}}}];
    let result = assert.commandWorked(testDb.runCommand(updateCommand));
    assert(result["result"]["updates"][0]["q"]["bar"]["$eq"] instanceof BinData, tojson(result));

    // Test that q is correctly marked for encryption.
    updateCommand["jsonSchema"] = {type: "object", properties: {foo: encryptDoc, bar: encryptDoc}};
    updateCommand["updates"] = [{q: {bar: {"$eq": 5}}, u: {"$set": {"foo": "2"}}}];
    result = assert.commandWorked(testDb.runCommand(updateCommand));
    assert(result["result"]["updates"][0]["q"]["bar"]["$eq"] instanceof BinData, tojson(result));

    updateCommand["jsonSchema"] = {type: "object", properties: {foo: encryptDoc, bar: encryptDoc}};
    updateCommand["updates"] = [{q: {bar: {"$in": [1, 5]}}, u: {"$set": {"foo": "2"}}}];
    result = assert.commandWorked(testDb.runCommand(updateCommand));
    assert(result["result"]["updates"][0]["q"]["bar"]["$in"][0] instanceof BinData, tojson(result));
    assert(result["result"]["updates"][0]["q"]["bar"]["$in"][1] instanceof BinData, tojson(result));

    // Test that an invalid q fails.
    updateCommand["jsonSchema"] = {type: "object", properties: {foo: encryptDoc, bar: encryptDoc}};
    updateCommand["updates"] = [{q: {bar: {"$gt": 5}}, u: {"$set": {"foo": "2"}}}];
    result = assert.commandFailedWithCode(testDb.runCommand(updateCommand), 51118);

    // Test that a $set path with an encrypted field in its prefix fails.
    updateCommand["jsonSchema"] = {type: "object", properties: {foo: encryptDoc, bar: encryptDoc}};
    updateCommand["updates"] = [{q: {bar: 5}, u: {"$set": {"foo.baz": "2"}}}];
    assert.commandFailedWithCode(testDb.runCommand(updateCommand), 51102);

    // Test that an update command with a field encrypted with a JSON Pointer keyId fails.
    updateCommand["jsonSchema"] = {
        type: "object",
        properties: {
            foo: {
                encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
                    initializationVector: BinData(0, "ASNFZ4mrze/ty6mHZUMhAQ=="),
                    keyId: "/key"
                }
            }
        }
    };
    updateCommand["updates"] = [{q: {}, u: {"$set": {foo: 5}}}];
    assert.commandFailedWithCode(testDb.runCommand(updateCommand), 51093);

    // Test that an update command with a q field encrypted with the random algorithm fails.
    updateCommand["jsonSchema"] = {
        type: "object",
        properties: {
            foo: {
                encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                    keyId: [UUID(), UUID()],
                }
            }
        }
    };
    updateCommand["updates"] = [{q: {foo: 2}, u: {"$set": {foo: 5}}}];
    assert.commandFailedWithCode(testDb.runCommand(updateCommand), 51158);

    mongocryptd.stop();
}());
