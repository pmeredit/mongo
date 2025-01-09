/**
 * Basic set of tests to verify the command response from query analysis for the aggregate command.
 */
import {MongoCryptD} from "src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js";
import {
    fle2Enabled,
    generateSchema,
    generateSchemasFromSchemaMap
} from "src/mongo/db/modules/enterprise/jstests/fle/lib/utils.js";
import {
    kDeterministicAlgo,
    kRandomAlgo
} from "src/mongo/db/modules/enterprise/jstests/fle/lib/utils.js";

const mongocryptd = new MongoCryptD();
mongocryptd.start();
const conn = mongocryptd.getConnection();
const testDB = conn.getDB("test");
const coll = testDB.fle_agg;

const encryptedStringSpec = {
    encrypt: {algorithm: kDeterministicAlgo, keyId: [UUID()], bsonType: "string"}
};

let command, cmdRes, schema;

// Builds an aggregate command to send to query analysis using the given schema spec. May build FLE
// 1 or FLE 2 syntax depending on the test suite parameters.
function buildAggregate(pipeline, schema) {
    return Object.assign({aggregate: coll.getName(), pipeline: pipeline, cursor: {}},
                         generateSchema(schema, coll.getFullName()));
}

function buildAggregateWithSchemaMap(pipeline, schemaMap) {
    return Object.assign({aggregate: coll.getName(), pipeline: pipeline, cursor: {}},
                         generateSchemasFromSchemaMap(schemaMap));
}

function buildCollectionlessAggregate(pipeline) {
    return Object.assign({aggregate: 1, pipeline: pipeline, cursor: {}},
                         generateSchema({}, coll.getFullName()));
}

// Test that a $match stage which does not reference an encrypted field is correctly reflected
// back from mongocryptd.
command = buildAggregate([{$match: {}}], {ssn: encryptedStringSpec});
cmdRes = assert.commandWorked(testDB.runCommand(command));
delete command.jsonSchema;
delete command.isRemoteSchema;
delete cmdRes.result.lsid;
assert.eq(command, cmdRes.result, cmdRes);
assert.eq(false, cmdRes.hasEncryptionPlaceholders, cmdRes);
assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);

// Test that a single $match stage on a top-level encrypted field is correctly marked for
// encryption.
schema = {
    location: encryptedStringSpec
};
command = buildAggregate([{$match: {location: "winterfell"}}], schema);
cmdRes = assert.commandWorked(testDB.runCommand(command));
assert.eq(true, cmdRes.hasEncryptionPlaceholders, cmdRes);
assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);
assert(cmdRes.hasOwnProperty("result"), cmdRes);
assert.eq(coll.getName(), cmdRes.result.aggregate, cmdRes);
assert(cmdRes.result.pipeline[0].$match.location.$eq instanceof BinData, cmdRes);

// Test that a $match stage alongside a no-op stage is correctly marked for encryption.
command = buildAggregate([{$limit: NumberLong(1)}, {$match: {location: "kings landing"}}], schema);
cmdRes = assert.commandWorked(testDB.runCommand(command));
assert.eq(true, cmdRes.hasEncryptionPlaceholders, cmdRes);
assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);
assert(cmdRes.hasOwnProperty("result"), cmdRes);
assert.eq(coll.getName(), cmdRes.result.aggregate, cmdRes);
assert.eq(cmdRes.result.pipeline[0], {$limit: NumberLong(1)}, cmdRes);
assert(cmdRes.result.pipeline[1].$match.location.$eq instanceof BinData, cmdRes);

// Test that a $match stage on a nested encrypted field is correctly marked for encryption.
schema = {
    'user.name': encryptedStringSpec
};
command = buildAggregate([{$match: {"user.name": "night king"}}], schema);
cmdRes = assert.commandWorked(testDB.runCommand(command));
assert.eq(true, cmdRes.hasEncryptionPlaceholders, cmdRes);
assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);
assert(cmdRes.hasOwnProperty("result"), cmdRes);
assert.eq(coll.getName(), cmdRes.result.aggregate, cmdRes);
assert(cmdRes.result.pipeline[0].$match["user.name"].$eq instanceof BinData, cmdRes);

// Test that a $match stage on a matching patternProperty is correctly marked for encryption. Only
// supported in FLE 1.
if (!fle2Enabled()) {
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
}

// Test that a $sort stage which does not reference an encrypted field is correctly reflected
// back from mongocryptd.
command = buildAggregate([{$sort: {bar: 1}}], {foo: encryptedStringSpec});
cmdRes = assert.commandWorked(testDB.runCommand(command));
delete command.jsonSchema;
delete command.isRemoteSchema;
delete cmdRes.result.lsid;
assert.eq(command, cmdRes.result, cmdRes);
assert.eq(false, cmdRes.hasEncryptionPlaceholders, cmdRes);
assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);

// Test that a $sort stage that references an encrypted field fails.
command = buildAggregate([{$sort: {name: -1, ssn: 1}}], {ssn: encryptedStringSpec});
assert.commandFailedWithCode(testDB.runCommand(command), 51201);

// Test that a $sort stage that references a prefix of an encrypted field fails.
command = buildAggregate([{$sort: {identity: -1}}], {'identity.ssn': encryptedStringSpec});
assert.commandFailedWithCode(testDB.runCommand(command), 51201);

// Test that a $sort stage that references a path with an encrypted prefix fails.
command =
    buildAggregate([{$sort: {"identity.ssn.number": -1}}], {'identity.ssn': encryptedStringSpec});
assert.commandFailedWithCode(testDB.runCommand(command), 51102);

// Test that stages which cannot contain sensitive data are correctly reflected back from
// mongocryptd. For stages that output fields, we additionally verify if subsequent $match
// has unencrypted constants.
const pipelinesForNotAffectedStages = [
    [{$collStats: {count: {}}}, {$match: {count: {$gt: "10000"}}}],
    [{$indexStats: {}}, {$match: {host: {"$eq": "examplehost.local:27017"}}}],
    [{$limit: NumberLong(1)}],
    [{$sample: {size: NumberLong(1)}}],
    [{$skip: NumberLong(1)}],
    [{$listSearchIndexes: {}}],
];

for (let pipe of pipelinesForNotAffectedStages) {
    const aggCommand = buildAggregate(pipe, {ssn: encryptedStringSpec});

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
    {
        stage: {
            $facet: {
                "pipeline1": [{$unwind: "$tags"}, {$sortByCount: "$tags"}],
                "pipeline2": [{$match: {ssn: 5}}]
            }
        },
        schemaMap: {[coll.getFullName()]: {}}
    },
    {stage: {$redact: "$$DESCEND"}, schemaMap: {[coll.getFullName()]: {}}},
    {stage: {$planCacheStats: {}}, schemaMap: {[coll.getFullName()]: {}}},
    {stage: {$_internalInhibitOptimization: {}}, schemaMap: {[coll.getFullName()]: {}}},
    {stage: {$out: "other"}, schemaMap: {[coll.getFullName()]: {}, "test.other": {}}}
];

for (let stageAndSchema of invalidStages) {
    const aggCommand =
        buildAggregateWithSchemaMap([stageAndSchema.stage], stageAndSchema.schemaMap);
    assert.commandFailedWithCode(testDB.runCommand(aggCommand), [31011]);
}

// Correctly fail for stages that query an encrypted field with encrypted data.
command = Object.assign(
    {
        aggregate: "fle_agg",
        pipeline: [{$match: {location: BinData(6, "data")}}],
        allowDiskUse: true,
        cursor: {},
        maxTimeMS: 1,
        explain: true,
    },
    generateSchema(
        {location: {encrypt: {algorithm: kRandomAlgo, keyId: [UUID()], bsonType: "string"}}},
        coll.getFullName()));
assert.commandFailedWithCode(testDB.runCommand(command), 31041);

// Correctly fail for stages which reference additional collections without providing their schema.
command = buildAggregate([{
            $graphLookup: {
                from: "other",
                startWith: "$reportsTo",
                connectFromField: "reportsTo",
                connectToField: "name",
                as: "reportingHierarchy"
            }
        }], {});

assert.commandFailedWithCode(testDB.runCommand(command), [9710000]);
command = buildAggregate(
    [{$lookup: {from: "other", localField: "ssn", foreignField: "sensitive", as: "res"}}], {});
assert.commandFailedWithCode(testDB.runCommand(command), [9710000]);

// Test that all collection-less aggregations result in a failure.
command = buildCollectionlessAggregate([{$changeStream: {}}]);
assert.commandFailedWithCode(testDB.runCommand(command), [
    9686712 /*fle2*/,
    51213 /*legacy fle1*/
]);

command = buildCollectionlessAggregate([{$listLocalSessions: {}}]);
assert.commandFailedWithCode(testDB.runCommand(command), [
    31106 /*legacy fle1*/,
    9686712 /*fle2*/
]);
command = buildCollectionlessAggregate([{$listLocalSessions: {allUsers: true}}]);
assert.commandFailedWithCode(testDB.runCommand(command), [
    31106 /*legacy fle1*/,
    9686712 /*fle2*/
]);
command = buildCollectionlessAggregate([{$listSessions: {}}]);
assert.commandFailedWithCode(testDB.runCommand(command), [
    31106 /*legacy fle1*/,
    9686712 /*fle2*/
]);
command = buildCollectionlessAggregate([{$listSessions: {allUsers: true}}]);
assert.commandFailedWithCode(testDB.runCommand(command), [
    31106 /*legacy fle1*/,
    9686712 /*fle2*/
]);

// CurrentOp must be run against admin db.
assert.commandFailedWithCode(
    testDB.getSiblingDB("admin").runCommand(buildCollectionlessAggregate([{$currentOp: {}}])), [
        9686712 /*fle2*/,
        51213 /*legacy fle1*/
    ]);

// Invalid pipelines correctly fail to parse.
assert.commandFailedWithCode(testDB.runCommand(buildAggregate([{$unknownStage: {}}], {})), 40324);

// Generic command options are correctly reflected back from mongocryptd.
command = Object.assign({
    aggregate: "fle_agg",
    pipeline: [{$limit: NumberLong(1)}],
    allowDiskUse: true,
    cursor: {},
    maxTimeMS: 1,
    explain: true,
},
                        generateSchema({}, coll.getFullName()));

cmdRes = assert.commandWorked(testDB.runCommand(command));
delete command.jsonSchema;
delete command.isRemoteSchema;
delete cmdRes.result.lsid;
assert.eq(command, cmdRes.result, cmdRes);
assert.eq(false, cmdRes.hasEncryptionPlaceholders, cmdRes);
assert.eq(false, cmdRes.schemaRequiresEncryption, cmdRes);

command.allowDiskUse = false;
command.explain = false;
Object.assign(command, generateSchema({}, coll.getFullName()));
cmdRes = assert.commandWorked(testDB.runCommand(command));
delete command.jsonSchema;
delete command.isRemoteSchema;
delete cmdRes.result.lsid;
assert.eq(command, cmdRes.result, cmdRes);
assert.eq(false, cmdRes.hasEncryptionPlaceholders, cmdRes);
assert.eq(false, cmdRes.schemaRequiresEncryption, cmdRes);

mongocryptd.stop();
