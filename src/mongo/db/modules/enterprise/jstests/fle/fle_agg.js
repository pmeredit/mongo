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

    // Test that stages which cannot contain senstive data are correctly reflected back from
    // mongocryptd.
    command = {
        aggregate: coll.getName(),
        pipeline: [{$limit: NumberLong(1)}],
        cursor: {},
        jsonSchema: {},
    };
    cmdRes = assert.commandWorked(testDB.runCommand(command));
    delete command.jsonSchema;
    delete cmdRes.result.lsid;
    assert.eq(command, cmdRes.result, cmdRes);
    assert.eq(false, cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert.eq(false, cmdRes.schemaRequiresEncryption, cmdRes);

    // Test that a $match stage which does not reference an encrypted field is correctly reflected
    // back from mongocryptd.
    command = {
        aggregate: coll.getName(),
        pipeline: [{$match: {}}],
        cursor: {},
        jsonSchema: {type: "object", properties: {ssn: encryptedStringSpec}},
    };
    cmdRes = assert.commandWorked(testDB.runCommand(command));
    delete command.jsonSchema;
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
    };
    cmdRes = assert.commandWorked(testDB.runCommand(command));
    assert.eq(true, cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);
    assert(cmdRes.hasOwnProperty("result"), cmdRes);
    assert.eq(coll.getName(), cmdRes.result.aggregate, cmdRes);
    assert(cmdRes.result.pipeline[0].$match.location.$eq instanceof BinData, cmdRes);

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
        {$geoNear: {near: [0.0, 0.0], distanceField: "dist"}},
        {$sample: {size: 1}},
        {$_internalInhibitOptimization: {}},
        {$skip: 1},
        {$sort: {ssn: 1}},
        {$indexStats: {}},
        {$collStats: {}},
        {$out: "other"},
        {$changeStream: {}},
    ];

    for (let stage of invalidStages) {
        const aggCommand = {
            aggregate: "test",
            pipeline: [stage],
            cursor: {},
            jsonSchema: {},
        };

        assert.commandFailed(testDB.runCommand(aggCommand));
    }

    // Test that all collection-less aggregations result in a failure.
    command = {
        aggregate: 1,
        pipeline: [{$changeStream: {}}],
        cursor: {},
        jsonSchema: {},
    };
    assert.commandFailedWithCode(testDB.runCommand(command), 40573);
    command = {
        aggregate: 1,
        pipeline: [{$listLocalSessions: {}}],
        cursor: {},
        jsonSchema: {},
    };
    assert.commandFailedWithCode(testDB.runCommand(command), 31106);
    command = {
        aggregate: 1,
        pipeline: [{$listLocalSessions: {allUsers: true}}],
        cursor: {},
        jsonSchema: {},
    };
    assert.commandFailedWithCode(testDB.runCommand(command), 31106);

    // CurrentOp must be run against admin db.
    assert.commandFailedWithCode(testDB.getSiblingDB("admin").runCommand({
        aggregate: 1,
        pipeline: [{$currentOp: {}}],
        cursor: {},
        jsonSchema: {},
    }),
                                 31011);

    // Invalid pipelines correctly fail to parse.
    assert.commandFailedWithCode(testDB.runCommand({
        aggregate: "test",
        pipeline: [{$unknownStage: {}}],
        cursor: {},
        jsonSchema: {},
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
    };

    cmdRes = assert.commandWorked(testDB.runCommand(command));
    delete command.jsonSchema;
    delete cmdRes.result.lsid;
    assert.eq(command, cmdRes.result, cmdRes);
    assert.eq(false, cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert.eq(false, cmdRes.schemaRequiresEncryption, cmdRes);

    command.allowDiskUse = false;
    command.explain = false;
    command.jsonSchema = {};
    cmdRes = assert.commandWorked(testDB.runCommand(command));
    delete command.jsonSchema;
    delete cmdRes.result.lsid;
    assert.eq(command, cmdRes.result, cmdRes);
    assert.eq(false, cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert.eq(false, cmdRes.schemaRequiresEncryption, cmdRes);

    mongocryptd.stop();
})();
