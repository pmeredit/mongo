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
        // Test that an encrypted field in an object replacement style update is correctly marked
        // for encryption.
        {
          schema: {type: "object", properties: {foo: encryptDoc, bar: encryptDoc}},
          updates: [{q: {foo: 2}, u: {foo: 2, baz: 3}}, {q: {}, u: {foo: 4, bar: 3}}],
          encryptedPaths: ["foo", "bar"],
          notEncryptedPaths: ["baz"]
        }
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
            let realUpdate = encryptedDoc["u"];
            if (realUpdate.hasOwnProperty("$set")) {
                realUpdate = realUpdate["$set"];
            }
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

    // Test that encryption occurs in $set to an object.
    updateCommand["jsonSchema"] = {
        type: "object",
        properties: {
            foo: {
                type: "object",
                properties: {
                    bar: encryptDoc,
                    baz: {type: "object", properties: {encrypted: encryptDoc}}
                }
            }
        }
    };
    updateCommand["updates"] =
        [{q: {}, u: {"$set": {"foo": {"bar": 5, "baz": {"encrypted": 2}, "boo": 2}}}}];
    result = assert.commandWorked(testDb.runCommand(updateCommand));
    assert(result["result"]["updates"][0]["u"]["$set"]["foo"]["bar"] instanceof BinData,
           tojson(result));
    assert(
        result["result"]["updates"][0]["u"]["$set"]["foo"]["baz"]["encrypted"] instanceof BinData,
        tojson(result));
    assert.eq(result["result"]["updates"][0]["u"]["$set"]["foo"]["boo"], 2, tojson(result));

    // Test that encryption occurs in object replacement style update with nested fields.
    updateCommand["jsonSchema"] = {
        type: "object",
        properties: {foo: {type: "object", properties: {bar: encryptDoc}}}
    };
    updateCommand["updates"] = [{q: {}, u: {foo: {bar: "string"}}}];
    result = assert.commandWorked(testDb.runCommand(updateCommand));
    assert(result["result"]["updates"][0]["u"]["foo"]["bar"] instanceof BinData, tojson(result));

    // Schema to use for dotted path testing.
    const dottedSchema = {
        type: "object",
        properties: {
            "d": {
                type: "object",
                properties: {"e": {type: "object", properties: {"f": encryptDoc}}}
            },
        }
    };

    // Test that $set to a dotted path correctly does not mark field for encryption if schema has
    // field names with embedded dots.
    updateCommand["jsonSchema"] = dottedSchema;
    updateCommand["updates"] = [{q: {}, u: {"$set": {"d": {"e.f": 4}}}}];
    result = assert.commandWorked(testDb.runCommand(updateCommand));
    assert.eq(result["result"]["updates"][0]["u"]["$set"],
              updateCommand["updates"][0]["u"]["$set"],
              result);

    // Test that $set of a non-object to a prefix of an encrypted field fails.
    updateCommand["jsonSchema"] = dottedSchema;
    updateCommand["updates"] = [{q: {}, u: {"$set": {"d": {"e": 4}}}}];
    result = assert.commandFailedWithCode(testDb.runCommand(updateCommand), 51159);

    // Test that $set of an object to a prefix of an encrypted field succeeds.
    updateCommand["jsonSchema"] = dottedSchema;
    updateCommand["updates"] = [{q: {}, u: {"$set": {"d": {"e": {"foo": 5}}}}}];
    result = assert.commandWorked(testDb.runCommand(updateCommand));
    assert.eq(result["result"]["updates"][0]["u"], updateCommand["updates"][0]["u"], result);

    // Test that an object replacement update correctly does not mark field for encryption if
    // schema has field names with embedded dots.
    updateCommand["jsonSchema"] = dottedSchema;
    updateCommand["updates"] = [{q: {}, u: {"d": {"e.f": 4}}}];
    result = assert.commandWorked(testDb.runCommand(updateCommand));
    assert.eq(result["result"]["updates"][0]["u"], updateCommand["updates"][0]["u"], result);

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
