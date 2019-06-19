/**
 * Tests to verify the correct response for an aggregation with a $redact stage.
 */
(function() {
    "use strict";

    load("src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js");

    const mongocryptd = new MongoCryptD();
    mongocryptd.start();
    const conn = mongocryptd.getConnection();
    const testDB = conn.getDB("test");
    const coll = testDB.fle_agg_redact;

    const encryptedStringSpec = {
        encrypt: {
            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
            keyId: [UUID()],
            bsonType: "string"
        }
    };

    let command, cmdRes, expected;

    // Test that basic $redact works.
    command = {
        aggregate: coll.getName(),
        pipeline: [{
            $redact: {
                $cond:
                    {if: {$eq: ["$someField", "someValue"]}, then: "$$PRUNE", else: "$$DESCEND"}
            }
        }],
        cursor: {},
        jsonSchema: {
            type: "object",
            properties: {user: {type: "object", properties: {ssn: encryptedStringSpec}}}
        },
        isRemoteSchema: false,
    };
    cmdRes = assert.commandWorked(testDB.runCommand(command));
    delete cmdRes.result.lsid;
    expected = {
        aggregate: coll.getName(),
        pipeline: [{
            $redact:
                {$cond: [{$eq: ["$someField", {"$const": "someValue"}]}, "$$PRUNE", "$$DESCEND"]}
        }],
        cursor: {}
    };
    assert(cmdRes.schemaRequiresEncryption, cmdRes);
    assert(!cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert.eq(cmdRes.result, expected, cmdRes);

    // Test that schema is correctly translated after $redact stage. 'encryptedField' fields stay as
    // encrypted field after '$redact' stage.
    command = {
        aggregate: coll.getName(),
        pipeline: [
            {
              $redact: {
                  $cond: {
                      if: {$eq: ["$someField", "someValue"]},
                      then: "$$PRUNE",
                      else: "$$DESCEND"
                  }
              }
            },
            {$match: {encryptedField: "isHere"}}
        ],
        cursor: {},
        jsonSchema: {type: "object", properties: {encryptedField: encryptedStringSpec}},
        isRemoteSchema: false,
    };
    cmdRes = assert.commandWorked(testDB.runCommand(command));
    assert(cmdRes.schemaRequiresEncryption, cmdRes);
    assert(cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert(cmdRes.result.pipeline[1].$match.encryptedField.$eq instanceof BinData, cmdRes);

    // Test that constants being compared to encrypted fields come back as placeholders.
    command = {
        aggregate: coll.getName(),
        pipeline: [{
            $redact:
                {$cond: {if: {$eq: ["$user", "redactUser"]}, then: "$$PRUNE", else: "$$DESCEND"}}
        }],
        cursor: {},
        jsonSchema: {type: "object", properties: {user: encryptedStringSpec}},
        isRemoteSchema: false,
    };
    cmdRes = assert.commandWorked(testDB.runCommand(command));
    assert(cmdRes.schemaRequiresEncryption, cmdRes);
    assert(cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert(cmdRes.result.pipeline[0].$redact.$cond[0].$eq[1]["$const"] instanceof BinData, cmdRes);

    mongocryptd.stop();
})();
