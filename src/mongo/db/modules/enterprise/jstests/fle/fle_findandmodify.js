/**
 * Verify that findAndModify commands are correctly marked for encryption if fields referenced in
 * the query or update are defined as encrypted.
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
            keyId: [UUID(), UUID()],
            bsonType: "string"
        }
    };

    const testCases = [
        // Test that the query using the 'remove' form of findAndModify gets encrypted.
        {
          schema: {type: "object", properties: {bar: encryptDoc}},
          query: {bar: "2"},
          remove: true,
          encryptedPaths: ["bar"],
          notEncryptedPaths: [],
          errorCode: 0
        },
        // Test that a findAndModify using the pipeline form of an update fails with NotImplemented.
        {
          schema: {type: "object", properties: {bar: encryptDoc}},
          query: {bar: "2"},
          update: [{$addFields: {newThing: "new"}}],
          encryptedPaths: ["bar"],
          notEncryptedPaths: [],
          errorCode: ErrorCodes.NotImplemented,
        },
        // Test that a top level field is encrypted.
        {
          schema: {type: "object", properties: {foo: encryptDoc}},
          query: {},
          update: {"$set": {"foo": "2"}},
          encryptedPaths: ["foo"],
          notEncryptedPaths: [],
          errorCode: 0
        },
        // Test that a dotted field is encrypted.
        {
          schema:
              {type: "object", properties: {foo: {type: "object", properties: {bar: encryptDoc}}}},
          query: {},
          update: {"$set": {"foo.bar": "2"}},
          encryptedPaths: ["foo.bar"],
          notEncryptedPaths: ["foo"],
          errorCode: 0
        },
        // Test that multiple correct fields are encrypted.
        {
          schema: {
              type: "object",
              properties: {foo: {type: "object", properties: {bar: encryptDoc}}, baz: encryptDoc}
          },
          query: {},
          update: {"$set": {"foo.bar": "2", "baz": "5", "plain": 7}},
          encryptedPaths: ["foo.bar", "baz"],
          notEncryptedPaths: ["plain"],
          errorCode: 0
        },
        // Test that an update path with a numeric path component works properly. The
        // schema indicates that the numeric path component is a field name, not an array
        // index.
        {
          schema:
              {type: "object", properties: {foo: {type: "object", properties: {1: encryptDoc}}}},
          query: {},
          update: {"$set": {"foo.1": "3"}},
          encryptedPaths: ["foo.1"],
          notEncryptedPaths: [],
          errorCode: 0
        },
        // Test that encrypted fields referenced in a query are correctly marked for encryption.
        {
          schema: {type: "object", properties: {bar: encryptDoc}},
          query: {bar: "2"},
          update: {foo: 2, baz: 3},
          encryptedPaths: ["bar"],
          notEncryptedPaths: ["foo", "baz"],
          errorCode: 0
        },
        // Test that encrypted fields referenced in a query and update are correctly marked for
        // encryption.
        {
          schema: {type: "object", properties: {foo: encryptDoc, bar: encryptDoc}},
          query: {bar: "2"},
          update: {foo: "2", baz: 3},
          encryptedPaths: ["foo", "bar"],
          notEncryptedPaths: ["baz"],
          errorCode: 0
        },
        // Test that an $unset with a q field gets encrypted.
        {
          schema: {type: "object", properties: {foo: encryptDoc}},
          query: {foo: "2"},
          update: {"$unset": {"bar": 1}},
          encryptedPaths: ["foo"],
          notEncryptedPaths: ["bar"],
          errorCode: 0
        },
        // Test that $unset works with an encrypted field.
        {
          schema: {type: "object", properties: {foo: encryptDoc}},
          query: {},
          update: {"$unset": {"foo": 1}},
          encryptedPaths: [],
          notEncryptedPaths: [],
          errorCode: 0
        },
        // Test that an update command with a field encrypted with a JSON Pointer keyId fails.
        {
          schema: {
              type: "object",
              properties: {
                  foo: {
                      encrypt: {
                          algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
                          keyId: "/key",
                          bsonType: "double"
                      }
                  }
              }
          },
          query: {},
          update: {$set: {foo: 5}},
          encryptedPaths: [],
          notEncryptedPaths: [],
          errorCode: 51093
        },
        // Test that an update command with a q field encrypted with the random algorithm fails.
        {
          schema: {
              type: "object",
              properties: {
                  foo: {
                      encrypt: {
                          algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                          keyId: [UUID(), UUID()],
                      }
                  }
              }
          },
          query: {foo: 2},
          update: {"$set": {foo: 5}},
          encryptedPaths: [],
          notEncryptedPaths: [],
          errorCode: 51158
        }
    ];

    const testDb = conn.getDB("test");
    let updateCommand = {findAndModify: "test", query: {}, update: {}, jsonSchema: {}};

    for (let test of testCases) {
        updateCommand.jsonSchema = test.schema;
        updateCommand.query = test.query;
        if (test.update) {
            updateCommand.update = test.update;
            delete updateCommand.remove;
        } else {
            updateCommand.remove = true;
            delete updateCommand.update;
        }
        const errorCode = test.errorCode;

        if (errorCode == 0) {
            const result = assert.commandWorked(testDb.runCommand(updateCommand));
            assert.eq(test.encryptedPaths.length >= 1, result.hasEncryptionPlaceholders);

            // Retrieve the interesting part of the update and query sections
            let realUpdate = null;
            if (result.result.hasOwnProperty("update")) {
                let update = result.result.update;
                if (update.hasOwnProperty("$set")) {
                    realUpdate = update.$set;
                } else if (update.hasOwnProperty("$unset")) {
                    realUpdate = update.$unset;
                }
            }
            let realQuery = result.result.query;

            // For each field that should be encrypted verify both the query
            // and the update. Some documents may not contain all of the fields.
            for (let encrypt of test.encryptedPaths) {
                if (realQuery.hasOwnProperty(encrypt)) {
                    assert(realQuery[encrypt].$eq instanceof BinData, tojson(realQuery));
                }
                if (realUpdate && realUpdate.hasOwnProperty(encrypt)) {
                    assert(realUpdate[encrypt] instanceof BinData, tojson(realUpdate));
                }
            }
            // For each field that should not be encrypted verify both the query
            // and the update. Some documents may not contain all of the fields.
            for (let noEncrypt of test.notEncryptedPaths) {
                if (realQuery.hasOwnProperty(noEncrypt)) {
                    assert(!(realQuery[encrypt].$eq instanceof BinData, tojson(realQuery)));
                }
                if (realUpdate && realUpdate.hasOwnProperty(noEncrypt)) {
                    assert(!(realUpdate[noEncrypt] instanceof BinData), tojson(realUpdate));
                }
            }
        } else {
            assert.commandFailedWithCode(testDb.runCommand(updateCommand), errorCode);
        }
    }

    // Test that a query with set membership is correctly marked for encryption.
    updateCommand.jsonSchema = {type: "object", properties: {foo: encryptDoc, bar: encryptDoc}};
    updateCommand.query = {bar: {$in: ["1", "5"]}};
    updateCommand.update = {$set: {foo: "2"}};
    const result = assert.commandWorked(testDb.runCommand(updateCommand));
    assert(result.result.query.bar.$in[0] instanceof BinData, tojson(result));
    assert(result.result.query.bar.$in[1] instanceof BinData, tojson(result));

    // Test that a $rename without encryption does not fail.
    updateCommand.jsonSchema = {type: "object", properties: {foo: encryptDoc, bar: encryptDoc}};
    updateCommand.query = {};
    updateCommand.update = {"$rename": {"baz": "boo"}};
    assert.commandWorked(testDb.runCommand(updateCommand));

    // Test that a $rename with one encrypted field fails.
    updateCommand.jsonSchema = {type: "object", properties: {foo: encryptDoc, bar: encryptDoc}};
    updateCommand.query = {};
    updateCommand.update = {"$rename": {"foo": "boo"}};
    assert.commandFailedWithCode(testDb.runCommand(updateCommand), 51160);

    // Test that a $rename between encrypted fields with the same metadata does not fail.
    updateCommand.jsonSchema = {type: "object", properties: {foo: encryptDoc, bar: encryptDoc}};
    updateCommand.query = {};
    updateCommand.update = {"$rename": {"foo": "bar"}};
    assert.commandWorked(testDb.runCommand(updateCommand));

    // Test that a $rename between encrypted fields with different metadata fails.
    updateCommand.jsonSchema = {
        type: "object",
        properties: {
            foo: {encrypt: {algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random", keyId: "/key"}},
            bar: encryptDoc
        }
    };
    updateCommand.query = {};
    updateCommand.update = {"$rename": {"foo": "bar"}};
    assert.commandFailedWithCode(testDb.runCommand(updateCommand), 51160);

    // Test that a $rename fails if the source field name is a prefix of an encrypted field.
    updateCommand.jsonSchema = {
        type: "object",
        properties: {foo: {type: "object", properties: {bar: encryptDoc}}}
    };
    updateCommand.query = {};
    updateCommand.update = {"$rename": {"foo": "baz"}};
    assert.commandFailedWithCode(testDb.runCommand(updateCommand), 51161);

    // Test that a $rename fails if the destination field name is a prefix of an encrypted field.
    updateCommand.jsonSchema = {
        type: "object",
        properties: {foo: {type: "object", properties: {bar: encryptDoc}}}
    };
    updateCommand.query = {};
    updateCommand.update = {"$rename": {"baz": "foo"}};
    assert.commandFailedWithCode(testDb.runCommand(updateCommand), 51161);

    // Test that a replacement-style update with an encrypted Timestamp(0, 0) and upsert fails.
    updateCommand.jsonSchema = {type: "object", properties: {foo: encryptDoc}};
    updateCommand.query = {};
    updateCommand.update = {foo: Timestamp(0, 0)};
    updateCommand.upsert = true;
    assert.commandFailedWithCode(testDb.runCommand(updateCommand), 51129);

    // Test that an update with an encrypted _id and upsert succeeds.
    updateCommand.jsonSchema = {type: "object", properties: {foo: encryptDoc, _id: encryptDoc}};
    updateCommand.query = {};
    updateCommand.update = {_id: "7", foo: "5"};
    updateCommand.upsert = true;
    assert.commandWorked(testDb.runCommand(updateCommand));

    // Test that an update with a missing encrypted _id and upsert fails.
    updateCommand.jsonSchema = {type: "object", properties: {foo: encryptDoc, _id: encryptDoc}};
    updateCommand.query = {};
    updateCommand.update = {foo: 5};
    updateCommand.upsert = true;
    assert.commandFailedWithCode(testDb.runCommand(updateCommand), 51130);

    // Test that a $set with an encrypted Timestamp(0,0) and upsert succeeds since the server does
    // not autogenerate the current time in this case.
    updateCommand.jsonSchema = {
        type: "object",
        properties: {
            foo: {
                encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
                    keyId: [UUID(), UUID()],
                    bsonType: "timestamp"
                }
            }
        }
    };
    updateCommand.query = {};
    updateCommand.update = {"$set": {foo: Timestamp(0, 0)}};
    updateCommand.upsert = true;
    assert.commandWorked(testDb.runCommand(updateCommand));

    // Test that arrayFilters on non-encrypted fields is allowed.
    updateCommand.jsonSchema = {type: "object", properties: {foo: encryptDoc}};
    updateCommand.update = {"$set": {"bar.$[i]": 1}};
    updateCommand.arrayFilters = [{i: 0}];
    assert.commandWorked(testDb.runCommand(updateCommand));

    // Test that an arrayFilter used on an encrypted field path is not allowed.
    updateCommand.jsonSchema = {type: "object", properties: {foo: encryptDoc}};
    updateCommand.update = {"$set": {"foo.$[i]": 1}};
    updateCommand.arrayFilters = [{i: 0}];
    assert.commandFailedWithCode(testDb.runCommand(updateCommand), 51150);

    mongocryptd.stop();
}());
