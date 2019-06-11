/**
 * Verifies expected command response from mongocryptd for an aggregation pipeline with $unwind
 * stage.
 */
(function() {
    "use strict";

    load("src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js");

    const mongocryptd = new MongoCryptD();
    mongocryptd.start();
    const conn = mongocryptd.getConnection();
    const testDB = conn.getDB("test");
    const coll = testDB.fle_agg_unwind;

    // Encryption of a $unwind stage which does not reference an encrypted field is a no-op,
    // regardless of algorithm used.
    function testUnwindEncryptionForNotReferencedPath(encryptionSpec) {
        const command = {
            aggregate: coll.getName(),
            pipeline: [{$unwind: {path: "$foo"}}],
            cursor: {},
            jsonSchema: {type: "object", properties: {ssn: encryptionSpec}},
            isRemoteSchema: false,
        };
        const cmdRes = assert.commandWorked(testDB.runCommand(command));
        delete command.jsonSchema;
        delete command.isRemoteSchema;
        delete cmdRes.result.lsid;
        assert.eq(command, cmdRes.result, cmdRes);
        assert.eq(false, cmdRes.hasEncryptionPlaceholders, cmdRes);
        assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);
    }

    //
    // Test using deterministic encryption algorithm.
    //
    (function() {
        const encryptedStringSpec = {
            encrypt: {
                algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
                keyId: [UUID()],
                bsonType: "string"
            }
        };

        testUnwindEncryptionForNotReferencedPath(encryptedStringSpec);

        // Encryption of a $unwind stage that references an encrypted field but uses a deterministic
        // algorithm is a no-op. This assumes that encrypted fields are banned from arrays.
        let command = {
            aggregate: coll.getName(),
            pipeline: [{$unwind: {path: "$ssnArray"}}],
            cursor: {},
            jsonSchema: {type: "object", properties: {ssnArray: encryptedStringSpec}},
            isRemoteSchema: false,
        };
        let cmdRes = assert.commandWorked(testDB.runCommand(command));
        delete command.jsonSchema;
        delete command.isRemoteSchema;
        delete cmdRes.result.lsid;
        assert.eq(command, cmdRes.result, cmdRes);
        assert.eq(false, cmdRes.hasEncryptionPlaceholders, cmdRes);
        assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);

        // A $unwind/$match sequence that references an encrypted field and uses a deterministic
        // algorithm is marked for encryption.
        command = {
            aggregate: coll.getName(),
            pipeline: [{$unwind: {path: "$ssnArray"}}, {$match: {ssnArray: {$eq: "123456789"}}}],
            cursor: {},
            jsonSchema: {type: "object", properties: {ssnArray: encryptedStringSpec}},
            isRemoteSchema: false,
        };
        cmdRes = assert.commandWorked(testDB.runCommand(command));
        assert(cmdRes.result.pipeline[1].$match.ssnArray.$eq instanceof BinData, cmdRes);
        assert.eq(true, cmdRes.hasEncryptionPlaceholders, cmdRes);
        assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);

        // A $unwind that replaces an encrypted field with "includeArrayIndex" will no longer
        // consider that field to be encrypted.
        command = {
            aggregate: coll.getName(),
            pipeline:
                [{$unwind: {path: "$foo", includeArrayIndex: "ssn"}}, {$match: {ssn: {$eq: 0}}}],
            cursor: {},
            jsonSchema: {type: "object", properties: {ssn: encryptedStringSpec}},
            isRemoteSchema: false,
        };
        cmdRes = assert.commandWorked(testDB.runCommand(command));
        delete command.jsonSchema;
        delete command.isRemoteSchema;
        delete cmdRes.result.lsid;
        assert.eq(command, cmdRes.result, cmdRes);
        assert.eq(false, cmdRes.hasEncryptionPlaceholders, cmdRes);
        assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);
    })();

    //
    // Test using random encryption algorithm.
    //
    (function() {
        const encryptedStringSpec = {
            encrypt: {
                algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                keyId: [UUID()],
                bsonType: "string"
            }
        };

        testUnwindEncryptionForNotReferencedPath(encryptedStringSpec);

        // Encryption of a $unwind stage that references an encrypted field using a random algorithm
        // is expected to fail.
        let command = {
            aggregate: coll.getName(),
            pipeline: [{$unwind: {path: "$ssnArray"}}],
            cursor: {},
            jsonSchema: {type: "object", properties: {ssnArray: encryptedStringSpec}},
            isRemoteSchema: false,
        };
        assert.commandFailedWithCode(testDB.runCommand(command), 31153);

        // Encryption of a $unwind stage that references a prefix of an encrypted field using a
        // random algorithm is a no-op.
        command = {
            aggregate: coll.getName(),
            pipeline: [{$unwind: {path: "$ssnArray"}}, {$match: {ssn: {$eq: 123456789}}}],
            cursor: {},
            jsonSchema: {
                type: "object",
                properties: {ssnArray: {type: "object", properties: {ssn: encryptedStringSpec}}}
            },
            isRemoteSchema: false,
        };
        let cmdRes = assert.commandWorked(testDB.runCommand(command));
        delete command.jsonSchema;
        delete command.isRemoteSchema;
        delete cmdRes.result.lsid;
        assert.eq(command, cmdRes.result, cmdRes);
        assert.eq(false, cmdRes.hasEncryptionPlaceholders, cmdRes);
        assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);

        // Encryption of a $unwind stage that references a prefix of an encrypted field using a
        // random algorithm, followed by a $match against the encrypted field is expected to fail.
        command = {
            aggregate: coll.getName(),
            pipeline:
                [{$unwind: {path: "$ssnArray"}}, {$match: {"ssnArray.ssn": {$eq: 123456789}}}],
            cursor: {},
            jsonSchema: {
                type: "object",
                properties: {ssnArray: {type: "object", properties: {ssn: encryptedStringSpec}}}
            },
            isRemoteSchema: false,
        };
        assert.commandFailedWithCode(testDB.runCommand(command), 51158);

        // Encryption of a $unwind stage that references a potentially encrypted field fails on
        // subsequent match.
        // TODO SERVER-41715: This test should fail on the match due to a potentially encrypted
        // matched field, rather than encounter error code 51203.
        command = {
            aggregate: coll.getName(),
            pipeline: [
                {
                  $project: {
                      ssnArray:
                          {$cond: {if: {$eq: ["$a", "$b"]}, then: "foo", else: "$ssnArray"}}
                  }
                },
                {$unwind: {path: "$ssnArray"}},
                {$match: {"ssnArray": {$eq: 123456789}}}
            ],
            cursor: {},
            jsonSchema: {type: "object", properties: {ssnArray: encryptedStringSpec}},
            isRemoteSchema: false,
        };
        assert.commandFailedWithCode(testDB.runCommand(command), 51203);
    })();

    mongocryptd.stop();
})();
