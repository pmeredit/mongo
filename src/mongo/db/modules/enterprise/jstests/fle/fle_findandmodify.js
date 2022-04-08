/**
 * Verify that findAndModify commands are correctly marked for encryption if fields referenced in
 * the query or update are defined as encrypted.
 */
(function() {
"use strict";

load("src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js");
load("src/mongo/db/modules/enterprise/jstests/fle/lib/utils.js");

const mongocryptd = new MongoCryptD();

mongocryptd.start();

const conn = mongocryptd.getConnection();

const encryptDoc = {
    encrypt: {algorithm: kDeterministicAlgo, keyId: [UUID(), UUID()], bsonType: "string"}
};
const namespace = "test.test";

const testCases = [
    // Test that the query using the 'remove' form of findAndModify gets encrypted.
    {
        schema: generateSchema({bar: encryptDoc}, namespace),
        query: {bar: "2"},
        remove: true,
        encryptedPaths: ["bar"],
        notEncryptedPaths: [],
        errorCode: 0
    },
    // Test that a top level field is encrypted.
    {
        schema: generateSchema({foo: encryptDoc}, namespace),
        query: {},
        update: {"$set": {"foo": "2"}},
        encryptedPaths: ["foo"],
        notEncryptedPaths: [],
        errorCode: 0
    },
    // Test that a dotted field is encrypted.
    {
        schema: generateSchema({'foo.bar': encryptDoc}, namespace),
        query: {},
        update: {"$set": {"foo.bar": "2"}},
        encryptedPaths: ["foo.bar"],
        notEncryptedPaths: ["foo"],
        errorCode: 0
    },
    // Test that multiple correct fields are encrypted.
    {
        schema: generateSchema({'foo.bar': encryptDoc, baz: encryptDoc}, namespace),
        query: {},
        update: {"$set": {"foo.bar": "2", "baz": "5", "plain": 7}},
        encryptedPaths: ["foo.bar", "baz"],
        notEncryptedPaths: ["plain"],
        errorCode: 0
    },
    // Test that an update path with a numeric path component works properly. The
    // schema indicates that the numeric path component is a field name, not an array
    // index.
    {
        schema: generateSchema({'foo.1': encryptDoc}, namespace),
        query: {},
        update: {"$set": {"foo.1": "3"}},
        encryptedPaths: ["foo.1"],
        notEncryptedPaths: [],
        errorCode: 0
    },
    // Test that encrypted fields referenced in a query are correctly marked for encryption.
    {
        schema: generateSchema({bar: encryptDoc}, namespace),
        query: {bar: "2"},
        update: {foo: 2, baz: 3},
        encryptedPaths: ["bar"],
        notEncryptedPaths: ["foo", "baz"],
        errorCode: 0
    },
    // Test that encrypted fields referenced in a query and update are correctly marked for
    // encryption.
    {
        schema: generateSchema({foo: encryptDoc, bar: encryptDoc}, namespace),
        query: {bar: "2"},
        update: {foo: "2", baz: 3},
        encryptedPaths: ["foo", "bar"],
        notEncryptedPaths: ["baz"],
        errorCode: 0
    },
    // Test that an $unset with a q field gets encrypted.
    {
        schema: generateSchema({foo: encryptDoc}, namespace),
        query: {foo: "2"},
        update: {"$unset": {"bar": 1}},
        encryptedPaths: ["foo"],
        notEncryptedPaths: ["bar"],
        errorCode: 0
    },
    // Test that $unset works with an encrypted field.
    {
        schema: generateSchema({foo: encryptDoc}, namespace),
        query: {},
        update: {"$unset": {"foo": 1}},
        encryptedPaths: [],
        notEncryptedPaths: [],
        errorCode: 0
    },
    // Test that an update command with a q field encrypted with the random algorithm fails.
    {
        schema: generateSchema(
            {foo: {encrypt: {algorithm: kRandomAlgo, keyId: [UUID(), UUID()], bsonType: "double"}}},
            namespace),
        query: {foo: 2},
        update: {"$set": {foo: 5}},
        encryptedPaths: [],
        notEncryptedPaths: [],
        errorCode: [51158, 63165]
    }
];

const testDb = conn.getDB("test");
let updateCommand = {findAndModify: "test", query: {}, update: {}};

for (let test of testCases) {
    Object.assign(updateCommand, test.schema);
    updateCommand.query = test.query;
    if (test.update) {
        updateCommand.update = test.update;
        delete updateCommand.remove;
    } else {
        updateCommand.remove = true;
        delete updateCommand.update;
    }
    const errorCode = test.errorCode;

    if (errorCode == 0) {
        const result = assert.commandWorked(testDb.runCommand(updateCommand));
        assert.eq(test.encryptedPaths.length >= 1, result.hasEncryptionPlaceholders);

        // Retrieve the interesting part of the update and query sections
        let realUpdate = null;
        if (result.result.hasOwnProperty("update")) {
            let update = result.result.update;
            if (update.hasOwnProperty("$set")) {
                realUpdate = update.$set;
            } else if (update.hasOwnProperty("$unset")) {
                realUpdate = update.$unset;
            }
        }
        let realQuery = result.result.query;

        // For each field that should be encrypted verify both the query
        // and the update. Some documents may not contain all of the fields.
        for (let encrypt of test.encryptedPaths) {
            if (realQuery.hasOwnProperty(encrypt)) {
                assert(realQuery[encrypt].$eq instanceof BinData, tojson(realQuery));
            }
            if (realUpdate && realUpdate.hasOwnProperty(encrypt)) {
                assert(realUpdate[encrypt] instanceof BinData, tojson(realUpdate));
            }
        }
        // For each field that should not be encrypted verify both the query
        // and the update. Some documents may not contain all of the fields.
        for (let noEncrypt of test.notEncryptedPaths) {
            if (realQuery.hasOwnProperty(noEncrypt)) {
                assert(!(realQuery[encrypt].$eq instanceof BinData, tojson(realQuery)));
            }
            if (realUpdate && realUpdate.hasOwnProperty(noEncrypt)) {
                assert(!(realUpdate[noEncrypt] instanceof BinData), tojson(realUpdate));
            }
        }
    } else {
        assert.commandFailedWithCode(testDb.runCommand(updateCommand), errorCode);
    }
}

//
// Test that an update command with a field encrypted with a JSON Pointer keyId fails. Note: FLE 1
// and FLE 2 differ in behavior here. For FLE 2, this schema fails because keyId must have be UUID,
// so we get a wrong type error (code 14).
//
if (fle2Enabled()) {
    updateCommand = {
        findAndModify: "test",
        query: {},
        update: {$set: {foo: NumberLong(5)}},
        encryptionInformation: {
            schema: {
                'test.test': {
                    fields: [{
                        path: 'foo',
                        queries: {queryType: "equality"},
                        keyId: "/key",
                        bsonType: "long"
                    }]
                }
            }
        }
    };
    assert.commandFailedWithCode(testDb.runCommand(updateCommand), 14);
} else {
    updateCommand = {
        findAndModify: "test",
        query: {},
        update: {$set: {foo: NumberLong(5)}},
        jsonSchema: {
            type: "object",
            properties:
                {foo: {encrypt: {algorithm: kDeterministicAlgo, keyId: "/key", bsonType: "long"}}}
        },
        isRemoteSchema: false
    };
    assert.commandFailedWithCode(testDb.runCommand(updateCommand), 31169);
}

// Test that a query with set membership is correctly marked for encryption.
let schema = generateSchema({foo: encryptDoc, bar: encryptDoc}, namespace);

updateCommand.query = {
    bar: {$in: ["1", "5"]}
};
updateCommand.update = {
    $set: {foo: "2"}
};
let result = assert.commandWorked(testDb.runCommand(Object.assign(updateCommand, schema)));
assert(result.result.query.bar.$in[0] instanceof BinData, tojson(result));
assert(result.result.query.bar.$in[1] instanceof BinData, tojson(result));

// Test that a $rename without encryption does not fail.
updateCommand.query = {};
updateCommand.update = {
    "$rename": {"baz": "boo"}
};
assert.commandWorked(testDb.runCommand(Object.assign(updateCommand, schema)));

// Test that a $rename with one encrypted field fails.
updateCommand.query = {};
updateCommand.update = {
    "$rename": {"foo": "boo"}
};
assert.commandFailedWithCode(testDb.runCommand(Object.assign(updateCommand, schema)),
                             [51160, 6329901]);

//
// Test that a $rename between encrypted fields with the same metadata does not fail in FLE 1. Note:
// FLE 1 and FLE 2 differ in behavior here. In FLE 2, $rename with encrypted fields is forbidden.
//
updateCommand.query = {};
updateCommand.update = {
    "$rename": {"foo": "bar"}
};
if (fle2Enabled()) {
    assert.commandFailedWithCode(testDb.runCommand(Object.assign(updateCommand, schema)), 6329901);
} else {
    assert.commandWorked(testDb.runCommand(Object.assign(updateCommand, schema)));
}

// Test that a $rename between encrypted fields with different metadata fails.
schema = generateSchema({
    foo: {encrypt: {algorithm: kRandomAlgo, keyId: [UUID()], bsonType: "string"}},
    bar: encryptDoc
},
                        namespace);
updateCommand.query = {};
updateCommand.update = {
    "$rename": {"foo": "bar"}
};
assert.commandFailedWithCode(testDb.runCommand(Object.assign(updateCommand, schema)),
                             [51160, 6329901]);

// Test that a $rename fails if the source field name is a prefix of an encrypted field.
schema = generateSchema({'foo.bar': encryptDoc}, namespace);

updateCommand.query = {};
updateCommand.update = {
    "$rename": {"foo": "baz"}
};
assert.commandFailedWithCode(testDb.runCommand(Object.assign(updateCommand, schema)), 51161);

// Test that a $rename fails if the destination field name is a prefix of an encrypted field.
updateCommand.query = {};
updateCommand.update = {
    "$rename": {"baz": "foo"}
};
assert.commandFailedWithCode(testDb.runCommand(Object.assign(updateCommand, schema)), 51161);

// Test that a replacement-style update with an encrypted Timestamp(0, 0) and upsert fails.
schema = generateSchema({foo: encryptDoc}, namespace);
updateCommand.query = {};
updateCommand.update = {
    foo: Timestamp(0, 0)
};
updateCommand.upsert = true;
assert.commandFailedWithCode(testDb.runCommand(Object.assign(updateCommand, schema)), 51129);

//
// Test that an update with an encrypted _id and upsert succeeds. Note: FLE 1 and FLE 2 differ in
// behavior here. For FLE 2, any schema with encrypted _id is disallowed.
//
schema = generateSchema({foo: encryptDoc, _id: encryptDoc}, namespace);

updateCommand.query = {};
updateCommand.update = {
    _id: "7",
    foo: "5"
};
updateCommand.upsert = true;
if (fle2Enabled()) {
    assert.commandFailedWithCode(testDb.runCommand(Object.assign(updateCommand, schema)), 6316403);
} else {
    assert.commandWorked(testDb.runCommand(Object.assign(updateCommand, schema)));
}

// Test that an update with a missing encrypted _id and upsert fails.
updateCommand.query = {};
updateCommand.update = {
    foo: 5
};
updateCommand.upsert = true;
assert.commandFailedWithCode(testDb.runCommand(Object.assign(updateCommand, schema)),
                             [51130, 6316403]);

// Test that a $set with an encrypted Timestamp(0,0) and upsert succeeds since the server does
// not autogenerate the current time in this case.
schema = generateSchema({
    foo: {encrypt: {algorithm: kDeterministicAlgo, keyId: [UUID(), UUID()], bsonType: "timestamp"}}
},
                        namespace);

updateCommand.query = {};
updateCommand.update = {
    "$set": {foo: Timestamp(0, 0)}
};
updateCommand.upsert = true;
assert.commandWorked(testDb.runCommand(Object.assign(updateCommand, schema)));

// Test that arrayFilters on non-encrypted fields is allowed.
schema = generateSchema({foo: encryptDoc}, namespace);

updateCommand.update = {
    "$set": {"bar.$[i]": 1}
};
updateCommand.arrayFilters = [{i: 0}];
assert.commandWorked(testDb.runCommand(Object.assign(updateCommand, schema)));

// Test that an arrayFilter used on an encrypted field path is not allowed.
updateCommand.update = {
    "$set": {"foo.$[i]": 1}
};
updateCommand.arrayFilters = [{i: 0}];
assert.commandFailedWithCode(testDb.runCommand(Object.assign(updateCommand, schema)), 51150);

// Pipelines in findAndModify are not allowed if _id is marked for encryption and upsert is set
// to true.
schema = generateSchema({_id: encryptDoc}, namespace);
delete updateCommand.arrayFilters;
updateCommand.update = [{}];
updateCommand.upsert = true;
assert.commandFailedWithCode(testDb.runCommand(Object.assign(updateCommand, schema)),
                             [31151, 6316403]);

// Test that pipelines in findAndModify are allowed if the schema of the document flowing out of
// the pipeline matches the schema of the collection.
schema = generateSchema({foo: encryptDoc, bar: {type: "string"}}, namespace);

delete updateCommand.upsert;
updateCommand.update = [{$addFields: {bar: "good afternoon"}}];
result = assert.commandWorked(testDb.runCommand(Object.assign(updateCommand, schema)));
assert.eq(true, result.schemaRequiresEncryption, result);
assert.eq(false, result.hasEncryptionPlaceholders, result);

// Pipelines with illegal stages for findAndModify correctly fail.
updateCommand.update = [{$match: {foo: "good afternoon"}}];
assert.commandFailedWithCode(testDb.runCommand(Object.assign(updateCommand, schema)),
                             ErrorCodes.InvalidOptions);

// Pipelines which perform a comparison against an encrypted field are correctly marked for
// encryption.
updateCommand.update =
    [{$addFields: {bar: {$cond: {if: {$eq: ["$foo", "afternoon"]}, then: "good", else: "bad"}}}}];

if (fle2Enabled()) {
    assert.commandFailedWithCode(testDb.runCommand(Object.assign(updateCommand, schema)), 6331102);
} else {
    result = assert.commandWorked(testDb.runCommand(Object.assign(updateCommand, schema)));
    assert.eq(true, result.schemaRequiresEncryption, result);
    assert.eq(true, result.hasEncryptionPlaceholders, result);
    assert(result.result.update[0]["$addFields"]["bar"]["$cond"][0]["$eq"][1]["$const"] instanceof
               BinData,
           tojson(result));
}

//
// Pipelines which produce an output schema that does not match the original schema for the
// collection correctly fail. Note: FLE 2 is notably more permissive here. Unlike FLE 1, in FLE 2
// pipelines which only affect the schema of non-encrypted fields should succeed.
//
updateCommand.update = [{$addFields: {newField: 5}}];
if (fle2Enabled()) {
    result = assert.commandWorked(testDb.runCommand(Object.assign(updateCommand, schema)));
    assert.eq(true, result.schemaRequiresEncryption, result);
    assert.eq(false, result.hasEncryptionPlaceholders, result);
} else {
    assert.commandFailedWithCode(testDb.runCommand(Object.assign(updateCommand, schema)), 31146);
}

updateCommand.update = [{$addFields: {foo: 5}}];
assert.commandFailedWithCode(testDb.runCommand(Object.assign(updateCommand, schema)),
                             [31146, 6329902]);

updateCommand.update = [{$project: {bar: 1}}];
assert.commandFailedWithCode(testDb.runCommand(Object.assign(updateCommand, schema)),
                             [31146, 6329902]);

//
// Pipelines with additionalProperties and patternProperties correctly fail. Note: these options are
// only valid for FLE 1, so the following tests exercise FLE 1-only behavior.
//
if (!fle2Enabled()) {
    updateCommand.isRemoteSchema = false;
    updateCommand.jsonSchema = {type: "object", additionalProperties: encryptDoc};
    updateCommand.update = [{$addFields: {newField: "$foo"}}];
    assert.commandFailedWithCode(testDb.runCommand(updateCommand), 31146);

    updateCommand.jsonSchema = {type: "object", patternProperties: {foo: encryptDoc}};
    updateCommand.update = [{$addFields: {newField: "$foo"}}];
    assert.commandFailedWithCode(testDb.runCommand(updateCommand), 31146);
}

mongocryptd.stop();
}());
