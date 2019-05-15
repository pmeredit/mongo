/**
 * Basic set of tests to verify the command response from mongocryptd for the aggregate command.
 */
(function() {
    "use strict";

    load("src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js");

    const mongocryptd = new MongoCryptD();
    mongocryptd.start();
    const conn = mongocryptd.getConnection();
    const testDB = conn.getDB("test");
    const coll = testDB.fle_agg;

    const encryptedStringSpec = {
        encrypt: {
            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
            keyId: [UUID()],
            bsonType: "string"
        }
    };

    let command, cmdRes, schema;

    // Test that a $match stage which does not reference an encrypted field is correctly reflected
    // back from mongocryptd.
    command = {
        aggregate: coll.getName(),
        pipeline: [{$match: {}}],
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

    // Test that a single $match stage on a top-level encrypted field is correctly marked for
    // encryption.
    schema = {type: "object", properties: {location: encryptedStringSpec}};
    command = {
        aggregate: coll.getName(),
        pipeline: [{$match: {location: "winterfell"}}],
        cursor: {},
        jsonSchema: schema,
        isRemoteSchema: false,
    };
    cmdRes = assert.commandWorked(testDB.runCommand(command));
    assert.eq(true, cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);
    assert(cmdRes.hasOwnProperty("result"), cmdRes);
    assert.eq(coll.getName(), cmdRes.result.aggregate, cmdRes);
    assert(cmdRes.result.pipeline[0].$match.location.$eq instanceof BinData, cmdRes);

    // Test that a $match stage alongside a no-op stage is correctly marked for encryption.
    command = {
        aggregate: coll.getName(),
        pipeline: [{$limit: NumberLong(1)}, {$match: {location: "kings landing"}}],
        cursor: {},
        jsonSchema: schema,
        isRemoteSchema: false,
    };
    cmdRes = assert.commandWorked(testDB.runCommand(command));
    assert.eq(true, cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);
    assert(cmdRes.hasOwnProperty("result"), cmdRes);
    assert.eq(coll.getName(), cmdRes.result.aggregate, cmdRes);
    assert.eq(cmdRes.result.pipeline[0], {$limit: NumberLong(1)}, cmdRes);
    assert(cmdRes.result.pipeline[1].$match.location.$eq instanceof BinData, cmdRes);

    // Test that a $match stage on a nested encrypted field is correctly marked for encryption.
    schema = {
        type: "object",
        properties: {user: {type: "object", properties: {name: encryptedStringSpec}}}
    };
    command = {
        aggregate: coll.getName(),
        pipeline: [{$match: {"user.name": "night king"}}],
        cursor: {},
        jsonSchema: schema,
        isRemoteSchema: false,
    };
    cmdRes = assert.commandWorked(testDB.runCommand(command));
    assert.eq(true, cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);
    assert(cmdRes.hasOwnProperty("result"), cmdRes);
    assert.eq(coll.getName(), cmdRes.result.aggregate, cmdRes);
    assert(cmdRes.result.pipeline[0].$match["user.name"].$eq instanceof BinData, cmdRes);

    // Test that a $match stage on a matching patternProperty is correctly marked for encryption.
    schema = {type: "object", patternProperties: {loc: encryptedStringSpec}};
    command = {
        aggregate: coll.getName(),
        pipeline: [{$match: {location: "castle black"}}],
        cursor: {},
        jsonSchema: schema,
        isRemoteSchema: false,
    };
    cmdRes = assert.commandWorked(testDB.runCommand(command));
    assert.eq(true, cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);
    assert(cmdRes.hasOwnProperty("result"), cmdRes);
    assert.eq(coll.getName(), cmdRes.result.aggregate, cmdRes);
    assert(cmdRes.result.pipeline[0].$match.location.$eq instanceof BinData, cmdRes);

    // Test that a $match stage on an additionalProperty is correctly marked for encryption.
    schema = {type: "object", properties: {}, additionalProperties: encryptedStringSpec};
    command = {
        aggregate: coll.getName(),
        pipeline: [{$match: {location: "castle black"}}],
        cursor: {},
        jsonSchema: schema,
        isRemoteSchema: false,
    };
    cmdRes = assert.commandWorked(testDB.runCommand(command));
    assert.eq(true, cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);
    assert(cmdRes.hasOwnProperty("result"), cmdRes);
    assert.eq(coll.getName(), cmdRes.result.aggregate, cmdRes);
    assert(cmdRes.result.pipeline[0].$match.location.$eq instanceof BinData, cmdRes);

    // Test that a $sort stage which does not reference an encrypted field is correctly reflected
    // back from mongocryptd.
    command = {
        aggregate: coll.getName(),
        pipeline: [{$sort: {bar: 1}}],
        cursor: {},
        jsonSchema: {type: "object", properties: {foo: encryptedStringSpec}},
        isRemoteSchema: false,
    };
    cmdRes = assert.commandWorked(testDB.runCommand(command));
    delete command.jsonSchema;
    delete command.isRemoteSchema;
    delete cmdRes.result.lsid;
    assert.eq(command, cmdRes.result, cmdRes);
    assert.eq(false, cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);

    // Test that a $sort stage that references an encrypted field fails.
    command = {
        aggregate: coll.getName(),
        pipeline: [{$sort: {name: -1, ssn: 1}}],
        cursor: {},
        jsonSchema: {type: "object", properties: {ssn: encryptedStringSpec}},
        isRemoteSchema: false,
    };
    assert.commandFailedWithCode(testDB.runCommand(command), 51201);

    // Test that a $sort stage that references a prefix of an encrypted field fails.
    command = {
        aggregate: coll.getName(),
        pipeline: [{$sort: {identity: -1}}],
        cursor: {},
        jsonSchema: {
            type: "object",
            properties: {identity: {type: "object", properties: {ssn: encryptedStringSpec}}},
        },
        isRemoteSchema: false,
    };
    assert.commandFailedWithCode(testDB.runCommand(command), 51201);

    // Test that a $sort stage that references a path with an encrypted prefix fails.
    command = {
        aggregate: coll.getName(),
        pipeline: [{$sort: {"identity.ssn.number": -1}}],
        cursor: {},
        jsonSchema: {
            type: "object",
            properties: {identity: {type: "object", properties: {ssn: encryptedStringSpec}}}
        },
        isRemoteSchema: false,
    };
    assert.commandFailedWithCode(testDB.runCommand(command), 51102);

    // Test that stages which cannot contain sensitive data are correctly reflected back from
    // mongocryptd. For stages that output fields, we additionally verify if subsequent $match
    // has unencrypted constants.
    const pipelinesForNotAffectedStages = [
        [{$collStats: {count: {}}}, {$match: {count: {$gt: "10000"}}}],
        [{$indexStats: {}}, {$match: {host: {"$eq": "examplehost.local:27017"}}}],
        [{$limit: NumberLong(1)}],
        [{$sample: {size: NumberLong(1)}}],
        [{$skip: NumberLong(1)}]
    ];

    for (let pipe of pipelinesForNotAffectedStages) {
        const aggCommand = {
            aggregate: "test",
            pipeline: pipe,
            cursor: {},
            jsonSchema: {type: "object", properties: {}, additionalProperties: encryptedStringSpec},
            isRemoteSchema: false,
        };

        cmdRes = assert.commandWorked(testDB.runCommand(aggCommand));
        delete aggCommand.jsonSchema;
        delete aggCommand.isRemoteSchema;
        delete cmdRes.result.lsid;
        assert.eq(aggCommand, cmdRes.result, cmdRes);
        assert.eq(false, cmdRes.hasEncryptionPlaceholders, cmdRes);
        assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);
    }

    // Correctly fail for unsupported aggregation stages.
    const invalidStages = [
        {$lookup: {from: "other", localField: "ssn", foreignField: "sensitive", as: "res"}},
        {
          $graphLookup: {
              from: "other",
              startWith: "$reportsTo",
              connectFromField: "reportsTo",
              connectToField: "name",
              as: "reportingHierarchy"
          }
        },
        {
          $facet: {
              "pipeline1": [{$unwind: "$tags"}, {$sortByCount: "$tags"}],
              "pipeline2": [{$match: {ssn: 5}}]
          }
        },
        {$group: {_id: null}},
        {$unwind: "$ssn"},
        {$redact: "$$DESCEND"},
        {$bucketAuto: {groupBy: "$_id", buckets: 2}},
        {$planCacheStats: {}},
        {$_internalInhibitOptimization: {}},
        {$out: "other"},
    ];

    for (let stage of invalidStages) {
        const aggCommand = {
            aggregate: "test",
            pipeline: [stage],
            cursor: {},
            jsonSchema: {},
            isRemoteSchema: false,
        };

        assert.commandFailedWithCode(testDB.runCommand(aggCommand), 31011);
    }

    // Test that all collection-less aggregations result in a failure.
    command = {
        aggregate: 1,
        pipeline: [{$changeStream: {}}],
        cursor: {},
        jsonSchema: {},
        isRemoteSchema: false,
    };
    assert.commandFailedWithCode(testDB.runCommand(command), 40573);
    command = {
        aggregate: 1,
        pipeline: [{$listLocalSessions: {}}],
        cursor: {},
        jsonSchema: {},
        isRemoteSchema: false,
    };
    assert.commandFailedWithCode(testDB.runCommand(command), 31106);
    command = {
        aggregate: 1,
        pipeline: [{$listLocalSessions: {allUsers: true}}],
        cursor: {},
        jsonSchema: {},
        isRemoteSchema: false,
    };
    assert.commandFailedWithCode(testDB.runCommand(command), 31106);
    command = {
        aggregate: 1,
        pipeline: [{$listSessions: {}}],
        cursor: {},
        jsonSchema: {},
        isRemoteSchema: false,
    };
    assert.commandFailedWithCode(testDB.runCommand(command), 31106);
    command = {
        aggregate: 1,
        pipeline: [{$listSessions: {allUsers: true}}],
        cursor: {},
        jsonSchema: {},
        isRemoteSchema: false,
    };
    assert.commandFailedWithCode(testDB.runCommand(command), 31106);

    // CurrentOp must be run against admin db.
    assert.commandFailedWithCode(testDB.getSiblingDB("admin").runCommand({
        aggregate: 1,
        pipeline: [{$currentOp: {}}],
        cursor: {},
        jsonSchema: {},
        isRemoteSchema: false,
    }),
                                 31011);

    // Invalid pipelines correctly fail to parse.
    assert.commandFailedWithCode(testDB.runCommand({
        aggregate: "test",
        pipeline: [{$unknownStage: {}}],
        cursor: {},
        jsonSchema: {},
        isRemoteSchema: false,
    }),
                                 40324);

    // Generic command options are correctly reflected back from mongocryptd.
    command = {
        aggregate: "test",
        pipeline: [{$limit: NumberLong(1)}],
        allowDiskUse: true,
        cursor: {},
        maxTimeMS: 1,
        explain: true,
        jsonSchema: {},
        isRemoteSchema: false,
    };

    cmdRes = assert.commandWorked(testDB.runCommand(command));
    delete command.jsonSchema;
    delete command.isRemoteSchema;
    delete cmdRes.result.lsid;
    assert.eq(command, cmdRes.result, cmdRes);
    assert.eq(false, cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert.eq(false, cmdRes.schemaRequiresEncryption, cmdRes);

    command.allowDiskUse = false;
    command.explain = false;
    command.jsonSchema = {};
    command.isRemoteSchema = false;
    cmdRes = assert.commandWorked(testDB.runCommand(command));
    delete command.jsonSchema;
    delete command.isRemoteSchema;
    delete cmdRes.result.lsid;
    assert.eq(command, cmdRes.result, cmdRes);
    assert.eq(false, cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert.eq(false, cmdRes.schemaRequiresEncryption, cmdRes);

    mongocryptd.stop();
})();
