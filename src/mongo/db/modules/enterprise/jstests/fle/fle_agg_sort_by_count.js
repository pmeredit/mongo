/**
 * Test that mongocryptd can correctly mark the $sortByCount agg stage with intent-to-encrypt
 * placeholders.
 */
(function() {
    "use strict";

    load("src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js");

    const mongocryptd = new MongoCryptD();
    mongocryptd.start();
    const conn = mongocryptd.getConnection();
    const testDB = conn.getDB("test");
    const coll = testDB.fle_agg_sort_by_count;

    const encryptedStringSpec = {
        encrypt: {
            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
            keyId: [UUID()],
            bsonType: "string"
        }
    };

    const encryptedRandomSpec = {
        encrypt: {algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random", keyId: [UUID()]}
    };

    let command, cmdRes, expectedResult;

    // Test that $sortByCount marks the projected fields '_id' and 'count' as not encrypted even if
    // they override encrypted fields.
    command = {
        aggregate: coll.getName(),
        pipeline: [
            {$sortByCount: "$foo"},
            {$match: {$and: [{_id: {$eq: "winterfell"}}, {count: {$eq: "winterfell"}}]}}
        ],
        cursor: {},
        jsonSchema:
            {type: "object", properties: {_id: encryptedStringSpec, count: encryptedStringSpec}},
        isRemoteSchema: false
    };
    cmdRes = assert.commandWorked(testDB.runCommand(command));
    expectedResult = {
        aggregate: coll.getName(),
        pipeline: [
            {$group: {_id: "$foo", count: {"$sum": {"$const": 1}}}},
            {$sort: {count: -1}},
            {$match: {$and: [{_id: {$eq: "winterfell"}}, {count: {$eq: "winterfell"}}]}}
        ],
        cursor: {}
    };
    delete cmdRes.result.lsid;
    assert.eq(expectedResult, cmdRes.result, cmdRes);
    assert.eq(false, cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);

    // Test that $sortByCount specified with an expression has constants correctly marked for
    // encryption.
    command = {
        aggregate: coll.getName(),
        pipeline:
            [{$sortByCount: {$cond: [{$eq: ["$qty", {$const: "thousand"}]}, "1000", "not1000"]}}],
        cursor: {},
        jsonSchema: {type: "object", properties: {qty: encryptedStringSpec}},
        isRemoteSchema: false
    };
    cmdRes = assert.commandWorked(testDB.runCommand(command));
    assert.eq(true, cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);
    assert(cmdRes.hasOwnProperty("result"), cmdRes);
    assert.eq(coll.getName(), cmdRes.result.aggregate, cmdRes);
    assert(cmdRes.result.pipeline[0].$group._id.$cond[0].$eq[1]["$const"] instanceof BinData,
           cmdRes);

    // Test that $sortByCount specified with an expression requires a stable output type across
    // documents to allow for comparisons.
    command = {
        aggregate: coll.getName(),
        pipeline:
            [{$sortByCount: {$cond: [{$eq: ["$qty", {$const: "thousand"}]}, "$qtyOther", "$qty"]}}],
        cursor: {},
        jsonSchema: {type: "object", properties: {qty: encryptedStringSpec}},
        isRemoteSchema: false
    };
    assert.commandFailedWithCode(testDB.runCommand(command), 51222);

    // Test that $sortByCount succeeds if it is specified with a deterministically encrypted
    // field.
    command = {
        aggregate: coll.getName(),
        pipeline: [{$sortByCount: "$foo"}, {$match: {_id: {$eq: "winterfell"}}}],
        cursor: {},
        jsonSchema: {type: "object", properties: {foo: encryptedStringSpec}},
        isRemoteSchema: false
    };
    cmdRes = assert.commandWorked(testDB.runCommand(command));
    assert.eq(true, cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);
    assert(cmdRes.hasOwnProperty("result"), cmdRes);
    assert.eq(coll.getName(), cmdRes.result.aggregate, cmdRes);
    assert(cmdRes.result.pipeline[2].$match["_id"].$eq instanceof BinData, cmdRes);

    // Test that $sortByCount fails if it is specified with a prefix of a deterministically
    // encrypted field.
    command = {
        aggregate: coll.getName(),
        pipeline: [{$sortByCount: "$foo"}],
        cursor: {},
        jsonSchema: {
            type: "object",
            properties: {foo: {type: "object", properties: {bar: encryptedStringSpec}}}
        },
        isRemoteSchema: false
    };
    assert.commandFailedWithCode(testDB.runCommand(command), 31129);

    // Test that $sortByCount fails if it is specified with a path with an encrypted prefix.
    command = {
        aggregate: coll.getName(),
        pipeline: [{$sortByCount: "$foo.bar"}],
        cursor: {},
        jsonSchema: {type: "object", properties: {foo: encryptedStringSpec}},
        isRemoteSchema: false
    };
    assert.commandFailedWithCode(testDB.runCommand(command), 51102);

    // Test that $sortByCount fails if it is specified with a field encrypted with the random
    // algorithm.
    command = {
        aggregate: coll.getName(),
        pipeline: [{$sortByCount: "$foo"}],
        cursor: {},
        jsonSchema: {type: "object", properties: {foo: encryptedRandomSpec}},
        isRemoteSchema: false
    };
    assert.commandFailedWithCode(testDB.runCommand(command), 51222);

    mongocryptd.stop();
})();
