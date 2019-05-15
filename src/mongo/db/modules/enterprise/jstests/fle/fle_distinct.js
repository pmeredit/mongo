/**
 * Basic set of tests to verify the response from mongocryptd for the distinct command.
 */
(function() {
    'use strict';

    load("src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js");

    const mongocryptd = new MongoCryptD();
    mongocryptd.start();
    const conn = mongocryptd.getConnection();
    const testDB = conn.getDB("test");

    const encryptObj = {
        encrypt: {
            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
            keyId: [UUID()],
            bsonType: "int"
        }
    };

    const sampleSchema = {
        type: "object",
        properties: {
            ssn: encryptObj,
            fieldWithEncryptedChild: {type: "object", properties: {ssn: encryptObj}},
            fieldWithEncryptedPatternPropertiesChild: {
                type: "object",
                patternProperties:
                    {"^s.*": {type: "object", properties: {encryptField: encryptObj}}}
            },
            fieldWithEncryptedAdditionalPropertiesChild: {
                type: "object",
                additionalProperties: {type: "object", properties: {encryptField: encryptObj}}
            },
            ssnWithPointer: {
                encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
                    keyId: "/whoKnows",
                    bsonType: "int"
                }
            }
        }
    };

    // Test that encrypted fields in the query are correctly replaced with an encryption
    // placeholder.
    let res = assert.commandWorked(testDB.runCommand({
        distinct: "test",
        key: "a",
        query: {ssn: {$eq: NumberInt(5)}},
        jsonSchema: sampleSchema,
        isRemoteSchema: false
    }));
    assert.eq(res.result.distinct, "test", tojson(res));
    assert.eq(res.hasEncryptionPlaceholders, true, tojson(res));
    assert(res.result.query.ssn.$eq instanceof BinData, tojson(res));

    // Test that a missing distinct key correctly fails.
    assert.commandFailedWithCode(
        testDB.runCommand({distinct: "test", jsonSchema: sampleSchema, isRemoteSchema: false}),
        40414);

    // Test that the command fails if the distinct key is an encrypted field with a JSON Pointer
    // keyId.
    assert.commandFailedWithCode(testDB.runCommand({
        distinct: "test",
        key: "ssnWithPointer",
        jsonSchema: sampleSchema,
        isRemoteSchema: false
    }),
                                 51131);

    // Test that invalid generic command arguments are ignored. The rationale for this is that there
    // is no sensitive/encrypted data within these options.
    assert.commandWorked(testDB.runCommand({
        distinct: "test",
        key: "ssn",
        readConcern: "invalid",
        jsonSchema: sampleSchema,
        isRemoteSchema: false
    }));
    assert.commandWorked(testDB.runCommand({
        distinct: "test",
        key: "ssn",
        maxTimeMS: -1,
        jsonSchema: sampleSchema,
        isRemoteSchema: false
    }));

    // Test that a distinct command with unknown fields correctly fails.
    assert.commandFailedWithCode(testDB.runCommand({
        distinct: "test",
        key: "ssn",
        invalidFieldName: true,
        jsonSchema: sampleSchema,
        isRemoteSchema: false
    }),
                                 40415);

    // Test that a distinct command with a field encrypted with a JSON Pointer keyId fails.
    assert.commandFailedWithCode(testDB.runCommand({
        distinct: "test",
        key: "a",
        query: {ssnWithPointer: {$eq: NumberInt(5)}},
        jsonSchema: sampleSchema,
        isRemoteSchema: false
    }),
                                 51093);

    // Test that the command fails if the distinct key is a field encrypted with a randomized
    // algorithm.
    assert.commandFailedWithCode(testDB.runCommand({
        distinct: "test",
        key: "encryptField",
        jsonSchema: {
            type: "object",
            properties: {
                encryptField: {
                    encrypt: {
                        algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                        keyId: [UUID()],
                    }
                }
            }
        },
        isRemoteSchema: false
    }),
                                 31026);

    // Test that the command fails if the distinct key matches a pattern properties field encrypted
    // with a randomized algorithm.
    assert.commandFailedWithCode(testDB.runCommand({
        distinct: "test",
        key: "ssn",
        jsonSchema: {
            type: "object",
            properties: {_id: {type: "string"}},
            patternProperties: {
                "^s.*": {
                    encrypt: {
                        algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                        keyId: [UUID()],
                    }
                }
            }
        },
        isRemoteSchema: false
    }),
                                 31026);

    // Test that the command fails if the distinct key is an additional properties field encrypted
    // with a randomized algorithm.
    assert.commandFailedWithCode(testDB.runCommand({
        distinct: "test",
        key: "anyField",
        jsonSchema: {
            type: "object",
            properties: {_id: {type: "string"}},
            additionalProperties: {
                encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                    keyId: [UUID()],
                }
            }
        },
        isRemoteSchema: false
    }),
                                 31026);

    // Test that the command works when 'key' is a nested encrypted field.
    assert.commandWorked(testDB.runCommand({
        distinct: "test",
        key: "fieldWithEncryptedChild.ssn",
        jsonSchema: sampleSchema,
        isRemoteSchema: false
    }));

    // Test that the command fails when 'key' is a prefix of a nested encrypted field.
    assert.commandFailedWithCode(testDB.runCommand({
        distinct: "test",
        key: "fieldWithEncryptedChild",
        jsonSchema: sampleSchema,
        isRemoteSchema: false
    }),
                                 31027);

    // Test that the command works when 'key' is an encrypted field nested in patternProperties.
    assert.commandWorked(testDB.runCommand({
        distinct: "test",
        key: "fieldWithEncryptedPatternPropertiesChild.ssn.encryptField",
        jsonSchema: sampleSchema,
        isRemoteSchema: false
    }));

    // Test that the command works when 'key' doesn't match a patternProperties field, which has a
    // nested encrypted field.
    assert.commandWorked(testDB.runCommand({
        distinct: "test",
        key: "fieldWithEncryptedPatternPropertiesChild.nonMatchingField",
        jsonSchema: sampleSchema,
        isRemoteSchema: false
    }));

    // Test that the command fails when 'key' is a prefix of an encrypted field nested in
    // patternProperties.
    assert.commandFailedWithCode(testDB.runCommand({
        distinct: "test",
        key: "fieldWithEncryptedPatternPropertiesChild.ssn",
        jsonSchema: sampleSchema,
        isRemoteSchema: false
    }),
                                 31027);
    assert.commandFailedWithCode(testDB.runCommand({
        distinct: "test",
        key: "fieldWithEncryptedPatternPropertiesChild",
        jsonSchema: sampleSchema,
        isRemoteSchema: false
    }),
                                 31027);

    // Test that the command works when 'key' is an encrypted field nested in additionalProperties.
    assert.commandWorked(testDB.runCommand({
        distinct: "test",
        key: "fieldWithEncryptedAdditionalPropertiesChild.additionalField.encryptField",
        jsonSchema: sampleSchema,
        isRemoteSchema: false
    }));

    // Test that the command fails when 'key' is a prefix of encrypted field nested in
    // additionalProperties.
    assert.commandFailedWithCode(testDB.runCommand({
        distinct: "test",
        key: "fieldWithEncryptedAdditionalPropertiesChild.someAdditionalField",
        jsonSchema: sampleSchema,
        isRemoteSchema: false
    }),
                                 31027);
    assert.commandFailedWithCode(testDB.runCommand({
        distinct: "test",
        key: "fieldWithEncryptedAdditionalPropertiesChild",
        jsonSchema: sampleSchema,
        isRemoteSchema: false
    }),
                                 31027);

    mongocryptd.stop();
})();
