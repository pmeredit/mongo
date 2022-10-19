/**
 * Verify that updates to encrypted fields are correctly marked for encryption.
 */
(function() {
"use strict";

load("src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js");
load("src/mongo/db/modules/enterprise/jstests/fle/lib/utils.js");

const mongocryptd = new MongoCryptD();

mongocryptd.start();

const conn = mongocryptd.getConnection();

const encryptDoc = () =>
    ({encrypt: {algorithm: kDeterministicAlgo, keyId: [UUID(), UUID()], bsonType: "string"}});
const namespace = "test.test";

const testCases = [
    // Test that a top level field is encrypted.
    {
        schema: generateSchema({foo: encryptDoc()}, namespace),
        updates: [{q: {}, u: {"$set": {"foo": "2"}}}],
        encryptedPaths: ["foo"],
        notEncryptedPaths: []
    },
    // Test that a dotted field is encrypted.
    {
        schema: generateSchema({'foo.bar': encryptDoc()}, namespace),
        updates: [{q: {}, u: {"$set": {"foo.bar": "2"}}}],
        encryptedPaths: ["foo.bar"],
        notEncryptedPaths: ["foo"]
    },
    // Test that multiple correct fields are encrypted.
    {
        schema: generateSchema({'foo.bar': encryptDoc(), baz: encryptDoc()}, namespace),
        updates: [{q: {}, u: {"$set": {"foo.bar": "2", "baz": "5", "plain": 7}}}],
        encryptedPaths: ["foo.bar", "baz"],
        notEncryptedPaths: ["plain"]
    },
    // Test that an update path with a numeric path component works properly. The
    // schema indicates that the numeric path component is a field name, not an array
    // index.
    {
        schema: generateSchema({'foo.1': encryptDoc()}, namespace),
        updates: [{q: {}, u: {"$set": {"foo.1": "3"}}}],
        encryptedPaths: ["foo.1"],
        notEncryptedPaths: []
    },
    // Test a basic multi-statement update.
    {
        schema: generateSchema({foo: encryptDoc(), bar: encryptDoc()}, namespace),
        updates: [{q: {}, u: {"$set": {"foo": "3"}}}, {q: {}, u: {"$set": {"bar": "2"}}}],
        encryptedPaths: ["foo", "bar"],
        notEncryptedPaths: []
    },
    // Test that an encrypted field in an object replacement style update is correctly marked
    // for encryption.
    {
        schema: generateSchema({foo: encryptDoc(), bar: encryptDoc()}, namespace),
        updates: [{q: {foo: "2"}, u: {foo: "2", baz: 3}}, {q: {}, u: {foo: "4", bar: "3"}}],
        encryptedPaths: ["foo", "bar"],
        notEncryptedPaths: ["baz"]
    }
];

const testDb = conn.getDB("test");
let updateCommand = {update: "test", updates: []};

for (let test of testCases) {
    Object.assign(updateCommand, test["schema"]);
    updateCommand.updates = test.updates;
    const result = assert.commandWorked(testDb.runCommand(updateCommand), tojson(test));
    assert.eq(test["encryptedPaths"].length >= 1, result["hasEncryptionPlaceholders"]);
    for (let encryptedDoc of result.result.updates) {
        let realUpdate = encryptedDoc.u;
        if (realUpdate.hasOwnProperty("$set")) {
            realUpdate = realUpdate["$set"];
        }
        // For each field that should be encrypted. Some documents may not contain all of the
        // fields.
        for (let encrypt of test.encryptedPaths) {
            if (realUpdate.hasOwnProperty(encrypt)) {
                assert(realUpdate[encrypt] instanceof BinData, tojson(realUpdate));
            }
        }
        // For each field that should not be encrypted. Some documents may not contain all of
        // the fields.
        for (let noEncrypt of test.notEncryptedPaths) {
            if (realUpdate.hasOwnProperty(noEncrypt)) {
                assert(!(realUpdate[noEncrypt] instanceof BinData), tojson(realUpdate));
            }
        }
    }
}

let schema = generateSchema({foo: encryptDoc(), bar: encryptDoc()}, namespace);

// Test that the 'upsert' field gets passed through.
updateCommand.updates = [{q: {bar: "5"}, u: {"$set": {"foo": "2"}}, upsert: true}];
let result = assert.commandWorked(testDb.runCommand(Object.assign(updateCommand, schema)));
assert(result.result.updates[0]["upsert"], result);

//
// Test that 'multi' gets passed through. Note: FLE 1 and FLE 2 differ in behavior here. For FLE 2,
// 'multi: true' is disallowed.
//
updateCommand.updates = [{q: {bar: "5"}, u: {"$set": {"foo": "2"}}, multi: true}];
if (fle2Enabled()) {
    assert.commandFailedWithCode(testDb.runCommand(Object.assign(updateCommand, schema)), 6329900);
} else {
    result = assert.commandWorked(testDb.runCommand(Object.assign(updateCommand, schema)));
    assert(result.result.updates[0]["multi"], result);
}

// Test that fields in q get replaced.
updateCommand.updates = [{q: {bar: "5"}, u: {"$set": {"foo": "2"}}}];
result = assert.commandWorked(testDb.runCommand(Object.assign(updateCommand, schema)));
assert(result.result.updates[0]["q"]["bar"]["$eq"] instanceof BinData, tojson(result));

// Test that q is correctly marked for encryption.
updateCommand.updates = [{q: {bar: {$eq: "5"}}, u: {"$set": {"foo": "2"}}}];
result = assert.commandWorked(testDb.runCommand(Object.assign(updateCommand, schema)));
assert(result.result.updates[0]["q"]["bar"]["$eq"] instanceof BinData, tojson(result));

updateCommand.updates = [{q: {bar: {$in: ["1", "5"]}}, u: {$set: {foo: "2"}}}];
result = assert.commandWorked(testDb.runCommand(Object.assign(updateCommand, schema)));
assert(result.result.updates[0]["q"]["bar"]["$in"][0] instanceof BinData, tojson(result));
assert(result.result.updates[0]["q"]["bar"]["$in"][1] instanceof BinData, tojson(result));

// Test that encryption occurs in $set to an object.
schema = generateSchema({'foo.bar': encryptDoc(), 'foo.baz.encrypted': encryptDoc()}, namespace);

updateCommand.updates = [{q: {}, u: {$set: {foo: {bar: "5", baz: {encrypted: "2"}, boo: 2}}}}];
result = assert.commandWorked(testDb.runCommand(Object.assign(updateCommand, schema)));
assert(result.result.updates[0].u["$set"]["foo"]["bar"] instanceof BinData, tojson(result));
assert(result.result.updates[0].u["$set"]["foo"]["baz"]["encrypted"] instanceof BinData,
       tojson(result));
assert.eq(result.result.updates[0].u["$set"]["foo"]["boo"], 2, tojson(result));

// Test that encryption occurs in object replacement style update with nested fields.
schema = generateSchema({'foo.bar': encryptDoc()}, namespace);

updateCommand.updates = [{q: {}, u: {foo: {bar: "string"}}}];
result = assert.commandWorked(testDb.runCommand(Object.assign(updateCommand, schema)));
assert(result.result.updates[0].u["foo"]["bar"] instanceof BinData, tojson(result));

// Schema to use for dotted path testing.
schema = generateSchema({'d.e.f': encryptDoc()}, namespace);

// Test that $set to a dotted path correctly does not mark field for encryption if schema has
// field names with embedded dots.
updateCommand.updates = [{q: {}, u: {"$set": {"d": {"e.f": 4}}}}];
result = assert.commandWorked(testDb.runCommand(Object.assign(updateCommand, schema)));
assert.eq(result.result.updates[0].u["$set"], updateCommand.updates[0].u["$set"], result);

// Test that $set of a non-object to a prefix of an encrypted field fails.
updateCommand.updates = [{q: {}, u: {"$set": {"d": {"e": 4}}}}];
result =
    assert.commandFailedWithCode(testDb.runCommand(Object.assign(updateCommand, schema)), 51159);

// Test that $set of an object to a prefix of an encrypted field succeeds.
updateCommand.updates = [{q: {}, u: {"$set": {"d": {"e": {"foo": 5}}}}}];
result = assert.commandWorked(testDb.runCommand(Object.assign(updateCommand, schema)));
assert.eq(result.result.updates[0].u, updateCommand.updates[0].u, result);

// Test that an object replacement update correctly does not mark field for encryption if
// schema has field names with embedded dots.
updateCommand.updates = [{q: {}, u: {"d": {"e.f": 4}}}];
result = assert.commandWorked(testDb.runCommand(Object.assign(updateCommand, schema)));
assert.eq(result.result.updates[0].u, updateCommand.updates[0].u, result);

// Test that a positional update is valid if fields nested below the array are not encrypted.
updateCommand.updates = [{q: {"d.e": 2}, u: {"d.e.array.$.g": 4}}];
result = assert.commandWorked(testDb.runCommand(Object.assign(updateCommand, schema)));
assert.eq(result.result.updates[0].u, updateCommand.updates[0].u, result);

// Test that a positional update of an encrypted field fails.
schema = generateSchema({'a.0': encryptDoc()}, namespace);
updateCommand.updates = [{q: {arr: {$eq: 5}}, u: {$set: {"a.$": 6}}}];
result =
    assert.commandFailedWithCode(testDb.runCommand(Object.assign(updateCommand, schema)), 51149);

schema = generateSchema({'foo': encryptDoc()}, namespace);
updateCommand.updates = [{q: {bar: 5}, u: {$set: {"foo.$": 6}}}];
result =
    assert.commandFailedWithCode(testDb.runCommand(Object.assign(updateCommand, schema)), 51149);

schema = generateSchema({'a.b': encryptDoc()}, namespace);
updateCommand.updates = [{q: {"a.b": "4"}, u: {$set: {"a.$": "5"}}}];
result =
    assert.commandFailedWithCode(testDb.runCommand(Object.assign(updateCommand, schema)), 51149);

// Test that an invalid q fails.
schema = generateSchema({foo: encryptDoc(), bar: encryptDoc()}, namespace);

updateCommand.updates = [{q: {bar: {"$gt": 5}}, u: {"$set": {"foo": "2"}}}];
result = assert.commandFailedWithCode(testDb.runCommand(Object.assign(updateCommand, schema)),
                                      [51118, 6721001]);

// Test that a $rename without encryption does not fail.
updateCommand.updates = [{q: {}, u: {"$rename": {"baz": "boo"}}}];
assert.commandWorked(testDb.runCommand(Object.assign(updateCommand, schema)));

// Test that a $rename with one encrypted field fails.
updateCommand.updates = [{q: {}, u: {"$rename": {"foo": "boo"}}}];
assert.commandFailedWithCode(testDb.runCommand(Object.assign(updateCommand, schema)),
                             [51160, 6329901]);

//
// Test that a $rename between encrypted fields with the same metadata does not fail. Note: FLE 1
// and FLE 2 differ in behavior here. In FLE 2, $rename with encrypted fields is always forbidden.
//
updateCommand.updates = [{q: {}, u: {"$rename": {"foo": "bar"}}}];
if (fle2Enabled()) {
    assert.commandFailedWithCode(testDb.runCommand(Object.assign(updateCommand, schema)), 6329901);
} else {
    let spec = encryptDoc();
    let sameMetadataSchema = generateSchema({foo: spec, bar: spec}, namespace);
    assert.commandWorked(testDb.runCommand(Object.assign(updateCommand, sameMetadataSchema)));
}

// Test that a $rename between encrypted fields with different metadata fails.
schema = generateSchema({
    foo: {encrypt: {algorithm: kRandomAlgo, keyId: [UUID()], bsonType: "string"}},
    bar: encryptDoc()
},
                        namespace);

updateCommand.updates = [{q: {}, u: {"$rename": {"foo": "bar"}}}];
assert.commandFailedWithCode(testDb.runCommand(Object.assign(updateCommand, schema)),
                             [51160, 6329901]);

// Test that a $rename fails if the source field name is a prefix of an encrypted field.
schema = generateSchema({'foo.bar': encryptDoc()}, namespace);
updateCommand.updates = [{q: {}, u: {"$rename": {"foo": "baz"}}}];
assert.commandFailedWithCode(testDb.runCommand(Object.assign(updateCommand, schema)), 51161);

// Test that a $rename fails if the destination field name is a prefix of an encrypted field.
schema = generateSchema({'foo.bar': encryptDoc()}, namespace);
updateCommand.updates = [{q: {}, u: {"$rename": {"baz": "foo"}}}];
assert.commandFailedWithCode(testDb.runCommand(Object.assign(updateCommand, schema)), 51161);

// Test that a $set path with an encrypted field in its prefix fails.
schema = generateSchema({foo: encryptDoc(), bar: encryptDoc()}, namespace);

updateCommand.updates = [{q: {bar: "5"}, u: {$set: {"foo.baz": "2"}}}];
assert.commandFailedWithCode(testDb.runCommand(Object.assign(updateCommand, schema)), 51102);

// Test that an update command with a q field encrypted with the random algorithm fails.
schema = generateSchema(
    {foo: {encrypt: {algorithm: kRandomAlgo, keyId: [UUID(), UUID()], bsonType: "double"}}},
    namespace);

updateCommand = {
    update: "test",
    updates: [{q: {foo: 2}, u: {"$set": {foo: 5}}}]
};
assert.commandFailedWithCode(testDb.runCommand(Object.assign(updateCommand, schema)),
                             [51158, 63165]);

// $set to a field encrypted with the random algorithm is allowed.
updateCommand.updates = [{q: {}, u: {"$set": {foo: 5}}}];
result = assert.commandWorked(testDb.runCommand(Object.assign(updateCommand, schema)));
assert.eq(true, result.hasEncryptionPlaceholders, result);

// Replacement update which encrypts a field with the random algorithm is also allowed.
updateCommand.updates = [{q: {}, u: {foo: 5}}];
result = assert.commandWorked(testDb.runCommand(Object.assign(updateCommand, schema)));
assert.eq(true, result.hasEncryptionPlaceholders, result);

// Test that an $unset with a q field gets encrypted.
schema = generateSchema({foo: encryptDoc()}, namespace);

updateCommand.updates = [{q: {foo: "4"}, u: {$unset: {bar: 1}}}];
result = assert.commandWorked(testDb.runCommand(Object.assign(updateCommand, schema)));
assert(result.result.updates[0]["q"]["foo"]["$eq"] instanceof BinData, tojson(result));

// Test that $unset works with an encrypted field.
updateCommand.updates = [{q: {}, u: {"$unset": {"foo": 1}}}];
result = assert.commandWorked(testDb.runCommand(Object.assign(updateCommand, schema)));
assert.eq(result.result.updates[0].u, updateCommand.updates[0].u, tojson(result));

// Test that a replacement-style update with an encrypted Timestamp(0, 0) and upsert fails.
updateCommand.updates = [{q: {}, u: {foo: Timestamp(0, 0)}, upsert: true}];
assert.commandFailedWithCode(testDb.runCommand(Object.assign(updateCommand, schema)), 51129);

//
// Test that an update with an encrypted _id and upsert succeeds. Note: FLE 1 and FLE 2 differ in
// behavior here. For FLE 2, any schema with encrypted _id is disallowed.
//
schema = generateSchema({foo: encryptDoc(), _id: encryptDoc()}, namespace);

updateCommand.updates = [{q: {}, u: {_id: "7", foo: "5"}, upsert: true}];
if (fle2Enabled()) {
    assert.commandFailedWithCode(testDb.runCommand(Object.assign(updateCommand, schema)), 6316403);
} else {
    assert.commandWorked(testDb.runCommand(Object.assign(updateCommand, schema)));
}

// Test that an update with a missing encrypted _id and upsert fails.
updateCommand.updates = [{q: {}, u: {foo: 5}, upsert: true}];
assert.commandFailedWithCode(testDb.runCommand(Object.assign(updateCommand, schema)),
                             [6316403, 51130]);

updateCommand.updates = [{q: {_id: 1}, u: {foo: 5}, upsert: true}];
assert.commandFailedWithCode(testDb.runCommand(Object.assign(updateCommand, schema)),
                             [6316403, 51130]);

// Test that a $set with an encrypted Timestamp(0,0) and upsert succeeds since the server does
// not autogenerate the current time in this case.
schema = generateSchema({
    foo: {encrypt: {algorithm: kDeterministicAlgo, keyId: [UUID(), UUID()], bsonType: "timestamp"}}
},
                        namespace);
updateCommand.updates = [{q: {}, u: {$set: {foo: Timestamp(0, 0)}}, upsert: true}];
assert.commandWorked(testDb.runCommand(Object.assign(updateCommand, schema)));

// Test that arrayFilters on a non-encrypted field is allowed.
schema = generateSchema({foo: encryptDoc()}, namespace);
updateCommand.updates = [{q: {}, u: {"$set": {"bar.$[i]": 1}}, arrayFilters: [{i: 0}]}];
assert.commandWorked(testDb.runCommand(Object.assign(updateCommand, schema)));

// Test that an arrayFilter used on an encrypted field path is not allowed.
updateCommand.updates = [{q: {}, u: {"$set": {"foo.$[i]": 1}}, arrayFilters: [{i: 0}]}];
assert.commandFailedWithCode(testDb.runCommand(Object.assign(updateCommand, schema)), 51150);

// Test that pipelines in update are allowed if the schema of the document flowing out of the
// pipeline matches the schema of the collection.
schema = generateSchema({foo: encryptDoc(), bar: {type: "string"}}, namespace);

updateCommand.updates = [{q: {}, u: [{$addFields: {bar: "good afternoon"}}]}];
result = assert.commandWorked(testDb.runCommand(Object.assign(updateCommand, schema)));
assert.eq(true, result.schemaRequiresEncryption, result);
assert.eq(false, result.hasEncryptionPlaceholders, result);

// Pipelines which modify the type of an unencrypted field to mismatch the original schema do
// not fail. This is because the schema analysis for encryption does not disambiguate between an
// unencrypted string and int, for example.
updateCommand.updates = [{q: {}, u: [{$addFields: {bar: 5}}]}];
result = assert.commandWorked(testDb.runCommand(Object.assign(updateCommand, schema)));
assert.eq(true, result.schemaRequiresEncryption, result);
assert.eq(false, result.hasEncryptionPlaceholders, result);

// Pipelines with illegal stages for update correctly fail.
updateCommand.updates[0].u = [{$match: {foo: "good afternoon"}}];
assert.commandFailedWithCode(testDb.runCommand(Object.assign(updateCommand, schema)),
                             ErrorCodes.InvalidOptions);

// Pipelines which perform a comparison against an encrypted field are correctly marked for
// encryption.
updateCommand.updates[0].u =
    [{$addFields: {bar: {$cond: {if: {$eq: ["$foo", "afternoon"]}, then: "good", else: "bad"}}}}];

if (fle2Enabled()) {
    assert.commandFailedWithCode(testDb.runCommand(Object.assign(updateCommand, schema)), 6331102);
} else {
    result = assert.commandWorked(testDb.runCommand(Object.assign(updateCommand, schema)));
    assert.eq(true, result.schemaRequiresEncryption, result);
    assert.eq(true, result.hasEncryptionPlaceholders, result);
    assert(
        result.result.updates[0].u[0]["$addFields"].bar["$cond"][0]["$eq"][1]["$const"] instanceof
            BinData,
        tojson(result));
}

//
// Pipelines which produce an output schema which does not match the original schema for the
// collection correctly fail in FLE 1. Note: FLE 1 and FLE 2 differ in behavior here. FLE 2 is more
// permissive and allows pipelines which only affect the schema of non-encrypted fields.
//
updateCommand.updates[0].u = [{$addFields: {newField: 5}}];
if (fle2Enabled()) {
    result = assert.commandWorked(testDb.runCommand(Object.assign(updateCommand, schema)));
    assert.eq(true, result.schemaRequiresEncryption, result);
    assert.eq(false, result.hasEncryptionPlaceholders, result);
} else {
    assert.commandFailedWithCode(testDb.runCommand(Object.assign(updateCommand, schema)), 31146);
}

updateCommand.updates[0].u = [{$addFields: {foo: 5}}];
assert.commandFailedWithCode(testDb.runCommand(Object.assign(updateCommand, schema)),
                             [31146, 6329902]);

updateCommand.updates[0].u = [{$project: {bar: 1}}];
assert.commandFailedWithCode(testDb.runCommand(Object.assign(updateCommand, schema)),
                             [31146, 6329902]);

// Pipelines which perform an invalid operation on an encrypted field correctly fail.
schema = generateSchema({foo: encryptDoc()}, namespace);
updateCommand.updates[0].u = [{$addFields: {newField: {$add: ["$foo", 1]}}}];
assert.commandFailedWithCode(testDb.runCommand(Object.assign(updateCommand, schema)),
                             [31110, 6331102]);

// Pipelines in update are not allowed if _id is marked for encryption and upsert is set to true.
schema = generateSchema({_id: encryptDoc()}, namespace);
updateCommand.updates = [{q: {}, u: [{}], upsert: true}];
assert.commandFailedWithCode(testDb.runCommand(Object.assign(updateCommand, schema)),
                             [31150, 6316403]);

// Updating a document with encrypted data at a path that is marked for encryption, fails.
schema = generateSchema({foo: encryptDoc(), bar: encryptDoc()}, namespace);
updateCommand.updates = [{q: {bar: {$in: ["1", "5"]}}, u: {$set: {foo: BinData(6, "data")}}}];
assert.commandFailedWithCode(testDb.runCommand(Object.assign(updateCommand, schema)), 31041);

//
// Note: the following syntax is only valid for FLE 1, so the following tests exercise FLE 1-only
// behavior.
//
if (!fle2Enabled()) {
    updateCommand = {
        update: "test",
        isRemoteSchema: false,
        jsonSchema: {type: "object", additionalProperties: encryptDoc()},
        updates: [{q: {}, u: [{$addFields: {newField: "$foo"}}]}]
    };
    assert.commandFailedWithCode(testDb.runCommand(updateCommand), 31146);

    updateCommand.jsonSchema = {type: "object", patternProperties: {foo: encryptDoc()}};
    updateCommand.updates[0].u = [{$addFields: {newField: "$foo"}}];
    assert.commandFailedWithCode(testDb.runCommand(updateCommand), 31146);

    // Test that an update command with a field encrypted with a JSON Pointer keyId fails.
    updateCommand = {
        update: "test",
        isRemoteSchema: false,
        jsonSchema: {
            type: "object",
            properties: {foo: {encrypt: {algorithm: kRandomAlgo, keyId: "/key", bsonType: "int"}}}
        },
        updates: [{q: {}, u: {$set: {foo: NumberInt(5)}}}]
    };
    assert.commandFailedWithCode(testDb.runCommand(updateCommand), 51093);
}

mongocryptd.stop();
}());
