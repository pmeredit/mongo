/**
 * Tests to verify the correct response for an aggregation with a $project stage.
 */
(function() {
    "use strict";

    load("src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js");

    const mongocryptd = new MongoCryptD();
    mongocryptd.start();
    const conn = mongocryptd.getConnection();
    const testDB = conn.getDB("test");
    const coll = testDB.fle_agg_project;

    const encryptedStringSpec = {
        encrypt: {
            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
            keyId: [UUID()],
            bsonType: "string"
        }
    };

    let command, cmdRes, expected;

    // Basic inclusion test.
    command = {
        aggregate: coll.getName(),
        pipeline: [{$project: {user: 1}}],
        cursor: {},
        jsonSchema: {type: "object", properties: {user: encryptedStringSpec}},
        isRemoteSchema: false,
    };

    cmdRes = assert.commandWorked(testDB.runCommand(command));
    delete cmdRes.result.lsid;
    expected = {
        aggregate: coll.getName(),
        pipeline: [{$project: {_id: true, user: true}}],
        cursor: {}
    };
    assert.eq(expected, cmdRes.result, cmdRes.result);
    assert(cmdRes.schemaRequiresEncryption, cmdRes);
    assert(!cmdRes.hasEncryptionPlaceholders, cmdRes);

    // Test that matching on a field that was projected out does not result in an intent-to-encyrpt
    // marking.
    command = {
        aggregate: coll.getName(),
        pipeline: [{$project: {user: 1}}, {$match: {removed: "isHere"}}],
        cursor: {},
        jsonSchema:
            {type: "object", properties: {user: encryptedStringSpec, removed: encryptedStringSpec}},
        isRemoteSchema: false,
    };

    cmdRes = assert.commandWorked(testDB.runCommand(command));
    delete cmdRes.result.lsid;
    expected = {
        aggregate: coll.getName(),
        pipeline: [{$project: {_id: true, user: true}}, {$match: {removed: {$eq: "isHere"}}}],
        cursor: {}
    };
    assert.eq(expected, cmdRes.result, cmdRes.result);
    assert(cmdRes.schemaRequiresEncryption, cmdRes);
    assert(!cmdRes.hasEncryptionPlaceholders);

    // Test that matching a renamed dotted field does result in an intent-to-encrypt marking.
    command = {
        aggregate: coll.getName(),
        pipeline: [{$project: {"newUser": "$user.ssn"}}, {$match: {"newUser": "isHere"}}],
        cursor: {},
        jsonSchema: {
            type: "object",
            properties: {user: {type: "object", properties: {ssn: encryptedStringSpec}}}
        },
        isRemoteSchema: false,
    };

    cmdRes = assert.commandWorked(testDB.runCommand(command));
    assert(cmdRes.schemaRequiresEncryption, cmdRes);
    assert(cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert(cmdRes.result.pipeline[1].$match.newUser.$eq instanceof BinData, cmdRes);

    // Basic exclusion test.
    command = {
        aggregate: coll.getName(),
        pipeline: [{$project: {"user": 0}}],
        cursor: {},
        jsonSchema: {type: "object", properties: {user: encryptedStringSpec}},
        isRemoteSchema: false,
    };

    cmdRes = assert.commandWorked(testDB.runCommand(command));
    delete cmdRes.result.lsid;
    expected = {aggregate: coll.getName(), pipeline: [{$project: {user: false}}], cursor: {}};
    assert.eq(expected, cmdRes.result, cmdRes.result);
    assert(cmdRes.schemaRequiresEncryption, cmdRes);
    assert(!cmdRes.hasEncryptionPlaceholders);

    // Test that exclusion ignores fields left in schema.
    command = {
        aggregate: coll.getName(),
        pipeline: [{$project: {"user": 0}}, {$match: {remaining: "isHere"}}],
        cursor: {},
        jsonSchema: {
            type: "object",
            properties: {user: encryptedStringSpec, remaining: encryptedStringSpec}
        },
        isRemoteSchema: false,
    };

    cmdRes = assert.commandWorked(testDB.runCommand(command));
    delete cmdRes.result.lsid;
    assert(cmdRes.result.pipeline[1].$match.remaining.$eq instanceof BinData,
           cmdRes.result.pipeline[1].$match.remaining.$eq);
    assert(cmdRes.schemaRequiresEncryption, cmdRes);
    assert(cmdRes.hasEncryptionPlaceholders);

    // Basic expressions test.
    command = {
        aggregate: coll.getName(),
        pipeline: [{$project: {"user": "$a"}}],
        cursor: {},
        jsonSchema: {type: "object", properties: {user: encryptedStringSpec}},
        isRemoteSchema: false,
    };

    cmdRes = assert.commandWorked(testDB.runCommand(command));
    delete cmdRes.result.lsid;
    expected = {
        aggregate: coll.getName(),
        pipeline: [{$project: {"_id": true, user: "$a"}}],
        cursor: {}
    };
    assert.eq(expected, cmdRes.result, cmdRes.result);

    // Test that constants being compared to encrypted fields come back as placeholders.
    command = {
        aggregate: coll.getName(),
        pipeline: [{$project: {"isTed": {$eq: ["$user", "Ted"]}}}],
        cursor: {},
        jsonSchema: {type: "object", properties: {user: encryptedStringSpec}},
        isRemoteSchema: false,
    };
    cmdRes = assert.commandWorked(testDB.runCommand(command));
    delete cmdRes.result.lsid;
    assert(cmdRes.result.pipeline[0].$project.isTed.$eq[1]["$const"] instanceof BinData, cmdRes);

    // Basic $addFields test.
    command = {
        aggregate: coll.getName(),
        pipeline: [{$addFields: {"user": "$ssn"}}],
        cursor: {},
        jsonSchema: {type: "object", properties: {user: encryptedStringSpec}},
        isRemoteSchema: false,
    };
    cmdRes = assert.commandWorked(testDB.runCommand(command));
    delete cmdRes.result.lsid;
    expected = {aggregate: coll.getName(), pipeline: [{$addFields: {user: "$ssn"}}], cursor: {}};
    assert.eq(expected, cmdRes.result, cmdRes.resuld);
    assert(cmdRes.schemaRequiresEncryption, cmdRes);
    assert(!cmdRes.hasEncryptionPlaceholders);

    // Test that $addFields does not affect the schema for other fields.
    command = {
        aggregate: coll.getName(),
        pipeline: [{$addFields: {"user": "randomString"}}, {$match: {ssn: "isHere"}}],
        cursor: {},
        jsonSchema: {type: "object", properties: {ssn: encryptedStringSpec}},
        isRemoteSchema: false,
    };
    cmdRes = assert.commandWorked(testDB.runCommand(command));
    delete cmdRes.result.lsid;
    assert(cmdRes.schemaRequiresEncryption, cmdRes);
    assert(cmdRes.hasEncryptionPlaceholders);
    assert(cmdRes.result.pipeline[1].$match.ssn.$eq instanceof BinData,
           cmdRes.result.pipeline[1].$match.ssn.$eq);

    // Test that hasEncryptionPlaceholders is properly set without a $match stage.
    command = {
        aggregate: coll.getName(),
        pipeline: [
            {$addFields: {"userIs123": {"$eq": ["$ssn", "1234"]}}},
        ],
        cursor: {},
        jsonSchema: {type: "object", properties: {ssn: encryptedStringSpec}},
        isRemoteSchema: false,
    };
    cmdRes = assert.commandWorked(testDB.runCommand(command));
    delete cmdRes.result.lsid;
    assert(cmdRes.schemaRequiresEncryption, cmdRes);
    assert(cmdRes.hasEncryptionPlaceholders);

    command = {
        aggregate: coll.getName(),
        pipeline: [
            {$addFields: {"userIs123": {"$eq": ["$foo", "1234"]}}},
        ],
        cursor: {},
        jsonSchema: {type: "object", properties: {ssn: encryptedStringSpec}},
        isRemoteSchema: false,
    };
    cmdRes = assert.commandWorked(testDB.runCommand(command));
    delete cmdRes.result.lsid;
    assert(cmdRes.schemaRequiresEncryption, cmdRes);
    assert(!cmdRes.hasEncryptionPlaceholders, cmdRes);
    mongocryptd.stop();
})();
