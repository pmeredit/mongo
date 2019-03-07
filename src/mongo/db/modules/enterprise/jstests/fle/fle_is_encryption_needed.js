// Validate isEncryptionNeeded works
//
load("src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js");

(function() {
    'use strict';

    const mongocryptd = new MongoCryptD();

    mongocryptd.start();

    const conn = mongocryptd.getConnection();

    const ret = assert.commandWorked(conn.adminCommand(
        {isEncryptionNeeded: 1, jsonSchema: {properties: {foo: {type: "string"}}}}));

    assert.eq(ret.requiresEncryption, false);

    const retTwo = assert.commandWorked(conn.adminCommand({
        isEncryptionNeeded: 1,
        jsonSchema: {
            properties: {foo: {type: "object", properties: {bar: {encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                    keyId: [UUID("4edee966-03cc-4525-bfa8-de8acd6746fa")]
            }}}}},
            type: "object"
        }
    }));

    assert.eq(retTwo.requiresEncryption, true);

    const retThree = assert.commandWorked(conn.adminCommand({
        isEncryptionNeeded: 1,
        jsonSchema: {
            properties: {
                foo: {
                    type: "object",
                    properties: {
                        bar: {type: "object", properties: {baz: {encrypt: {
                            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                            keyId: [UUID("4edee966-03cc-4525-bfa8-de8acd6746fa")]
                        }}}},
                        oof: {type: "string"}
                    }
                },
                zap: {type: "string"}
            },
            type: "object"
        }
    }));

    assert.eq(retThree.requiresEncryption, true);

    const retFour = assert.commandFailed(conn.adminCommand({
        isEncryptionNeeded: 1,
        jsonSchema: {
            properties: {
                foo: {
                    type: "object",
                    invalid: {
                        bar: {type: "object", properties: {baz: {encrypt: {
                            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                            keyId: [UUID("4edee966-03cc-4525-bfa8-de8acd6746fa")]
                        }}}},
                        oof: {type: "string"}
                    }
                },
                zap: {type: "string"}
            },
            type: "object"
        }
    }));

    const retFive = assert.commandFailed(conn.adminCommand({isEncryptionNeeded: 1}));

    mongocryptd.stop();
})();
