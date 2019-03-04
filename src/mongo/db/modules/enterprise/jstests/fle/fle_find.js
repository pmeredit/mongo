/**
 * Basic set of tests to verify the response from mongocryptd for the find command.
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
                    initializationVector: BinData(0, "ASNFZ4mrze/ty6mHZUMhAQ==")
                }
            },
            user: {
                type: "object",
                properties: {
                    account: {
                        encrypt: {
                            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                            keyId: [UUID()],
                        }
                    }
                }
            }
        }
    };

    function assertEncryptedFieldInResponse({filter, path = "", requiresEncryption}) {
        const res = assert.commandWorked(
            testDB.runCommand({find: "test", filter: filter, jsonSchema: sampleSchema}));

        assert(res.result.find == "test", tojson(res));
        assert(res.hasEncryptionPlaceholders == requiresEncryption, tojson(res));
        assert(path === "" || res.result.filter[path]["$eq"] instanceof BinData, tojson(res));
    }

    // Basic top-level field correctly marked for encryption.
    assertEncryptedFieldInResponse({filter: {ssn: 5}, path: "ssn", requiresEncryption: true});

    // Nested field correctly marked for encryption.
    assertEncryptedFieldInResponse(
        {filter: {"user.account": "secret"}, path: "user.account", requiresEncryption: true});

    // Responses to queries without any encrypted fields should not set the
    // 'hasEncryptionPlaceholders' bit.
    assertEncryptedFieldInResponse({filter: {}, requiresEncryption: false});
    assertEncryptedFieldInResponse({filter: {"user.notSecure": 5}, requiresEncryption: false});

    // Invalid operators should fail with an appropriate error code.
    assert.commandFailedWithCode(
        testDB.runCommand({find: "test", filter: {ssn: {$gt: 5}}, jsonSchema: sampleSchema}),
        51092);
    assert.commandFailedWithCode(
        testDB.runCommand({find: "test", filter: {ssn: /\d/}, jsonSchema: sampleSchema}), 51092);

    // Comparison to a null value correctly fails.
    assert.commandFailedWithCode(
        testDB.runCommand({find: "test", filter: {ssn: null}, jsonSchema: sampleSchema}), 51095);
    // TODO SERVER-39417 adds support for $in, but a null element within the $in array should still
    // fail.
    assert.commandFailedWithCode(
        testDB.runCommand({find: "test", filter: {ssn: {$in: [null]}}, jsonSchema: sampleSchema}),
        51094);

    // Invalid expressions correctly fail to parse.
    assert.commandFailedWithCode(
        testDB.runCommand({find: "test", filter: {$cantDoThis: 5}, jsonSchema: sampleSchema}),
        ErrorCodes.BadValue);

    // Unknown fields correctly result in an error.
    assert.commandFailedWithCode(
        testDB.runCommand({find: "test", filter: {}, jsonSchema: sampleSchema, whatIsThis: 1}),
        ErrorCodes.FailedToParse);

    // Invalid type for command parameters correctly result in an error.
    assert.commandFailedWithCode(testDB.runCommand({find: 5, filter: {}, jsonSchema: sampleSchema}),
                                 13111);
    assert.commandFailedWithCode(
        testDB.runCommand({find: "test", filter: "not an object", jsonSchema: sampleSchema}),
        ErrorCodes.FailedToParse);
    assert.commandFailedWithCode(
        testDB.runCommand({find: "test", filter: {}, jsonSchema: "same here"}), 51090);

    mongocryptd.stop();
})();
