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
            properties: {foo: {type: "object", properties: {bar: {encrypt: {}}}}},
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
                        bar: {type: "object", properties: {baz: {encrypt: {}}}},
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
                        bar: {type: "object", properties: {baz: {encrypt: {}}}},
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
