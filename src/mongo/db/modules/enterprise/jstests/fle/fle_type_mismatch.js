/**
 * Test that mongocryptd produces an error when the type of an element being marked for encryption
 * does not comply with the schema.
 */
import {MongoCryptD} from "src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js";
import {
    fle2Enabled,
    generateSchema
} from "src/mongo/db/modules/enterprise/jstests/fle/lib/utils.js";
import {
    kDeterministicAlgo,
    kRandomAlgo
} from "src/mongo/db/modules/enterprise/jstests/fle/lib/utils.js";

const mongocryptd = new MongoCryptD();
mongocryptd.start();

const conn = mongocryptd.getConnection();
const testDb = conn.getDB("test");
const coll = testDb.fle_type_mismatch;

// A schema where the 'foo' field may either be a string or a double and is encrypted with the
// random encryption algorithm. In FLE 2, only a single bsonType is allowed.
const fooRandomEncryptionSchema = generateSchema({
    foo: {
        encrypt: {
            algorithm: kRandomAlgo,
            keyId: [UUID()],
            bsonType: fle2Enabled() ? "string" : ["string", "double"]
        }
    }
},
                                                 coll.getFullName());

let cmdRes;

// Can successfully mark a string element for encryption on insert.
cmdRes = assert.commandWorked(testDb.runCommand(Object.assign({
    insert: coll.getName(),
    documents: [{"foo": "bar"}],
},
                                                              fooRandomEncryptionSchema)));
assert.eq(cmdRes.hasEncryptionPlaceholders, true, cmdRes);
assert.eq(cmdRes.schemaRequiresEncryption, true, cmdRes);
assert(cmdRes.result.documents[0].foo instanceof BinData, cmdRes);

// FLE 2 allows only a single bsonType, inserting a non-string for the 'foo' field should fail.
if (fle2Enabled()) {
    assert.commandFailedWithCode(testDb.runCommand(Object.assign({
        insert: coll.getName(),
        documents: [{"foo": 3}],
    },
                                                                 fooRandomEncryptionSchema)),
                                 31118);
} else {
    // Can successfully mark a double element for encryption on insert.
    cmdRes = assert.commandWorked(testDb.runCommand(Object.assign({
        insert: coll.getName(),
        documents: [{"foo": 3}],
    },
                                                                  fooRandomEncryptionSchema)));
    assert.eq(cmdRes.hasEncryptionPlaceholders, true, cmdRes);
    assert.eq(cmdRes.schemaRequiresEncryption, true, cmdRes);
    assert(cmdRes.result.documents[0].foo instanceof BinData, cmdRes);
}

// Cannot mark an int for encryption on insert, since this would not comply with the schema.
assert.commandFailedWithCode(testDb.runCommand(Object.assign({
    insert: coll.getName(),
    documents: [{"foo": NumberInt(3)}],
},
                                                             fooRandomEncryptionSchema)),
                             31118);

// Can successfully mark a string element for encryption on $set-style update.
cmdRes = assert.commandWorked(testDb.runCommand(Object.assign({
    update: coll.getName(),
    updates: [{q: {}, u: {$set: {"foo": "bar"}}}],
},
                                                              fooRandomEncryptionSchema)));
assert.eq(cmdRes.hasEncryptionPlaceholders, true, cmdRes);
assert.eq(cmdRes.schemaRequiresEncryption, true, cmdRes);
assert(cmdRes.result.updates[0].u.$set.foo instanceof BinData, cmdRes);

// Cannot mark an int for encryption on $set-style update due to type mismatch.
assert.commandFailedWithCode(testDb.runCommand(Object.assign({
    update: coll.getName(),
    updates: [{q: {}, u: {$set: {"foo": NumberInt(3)}}}],
},
                                                             fooRandomEncryptionSchema)),
                             31118);

// Can successfully mark a string element for encryption on replacement-style update.
cmdRes = assert.commandWorked(testDb.runCommand(Object.assign({
    update: coll.getName(),
    updates: [{q: {}, u: {"foo": "bar"}}],
},
                                                              fooRandomEncryptionSchema)));
assert.eq(cmdRes.hasEncryptionPlaceholders, true, cmdRes);
assert.eq(cmdRes.schemaRequiresEncryption, true, cmdRes);
assert(cmdRes.result.updates[0].u.foo instanceof BinData, cmdRes);

// Cannot mark an int for encryption on replacement-style update due to type mismatch.
assert.commandFailedWithCode(testDb.runCommand(Object.assign({
    update: coll.getName(),
    updates: [{q: {}, u: {"foo": NumberInt(3)}}],
},
                                                             fooRandomEncryptionSchema)),
                             31118);

// Cannot mark an int for encryption on update expressed with findAndModify due to type
// mismatch.
assert.commandFailedWithCode(testDb.runCommand(Object.assign({
    findAndModify: coll.getName(),
    query: {},
    update: {$set: {foo: NumberInt(3)}},
},
                                                             fooRandomEncryptionSchema)),
                             31118);

// Type mismatch for a query constant doesn't matter for this schema, since equality queries
// against a field encrypted with the random algorithm are illegal.
assert.commandFailedWithCode(testDb.runCommand(Object.assign({
    find: coll.getName(),
    filter: {foo: {$eq: NumberInt(3)}},
},
                                                             fooRandomEncryptionSchema)),
                             [51158, 63165]);

// A schema where the 'foo' field must be a long and is encrypted with the deterministic
// encryption algorithm or marked with queryType "equality" for FLE 2.
const deterministicEncryptionSchema = generateSchema({
    foo: {
        encrypt: {
            algorithm: kDeterministicAlgo,
            keyId: [UUID()],
            bsonType: "long",
        }
    }
},
                                                     coll.getFullName());

// Can successfully mark a long for encryption on insert.
cmdRes = assert.commandWorked(testDb.runCommand(Object.assign({
    insert: coll.getName(),
    documents: [{"foo": NumberLong(3)}],
},
                                                              deterministicEncryptionSchema)));
assert.eq(cmdRes.hasEncryptionPlaceholders, true, cmdRes);
assert.eq(cmdRes.schemaRequiresEncryption, true, cmdRes);
assert(cmdRes.result.documents[0].foo instanceof BinData, cmdRes);

// Cannot mark an int for encryption on insert, since this would not comply with the schema.
assert.commandFailedWithCode(testDb.runCommand(Object.assign({
    insert: coll.getName(),
    documents: [{"foo": NumberInt(3)}],
},
                                                             deterministicEncryptionSchema)),
                             31118);

// Can successfully mark a long for encryption on $set-style update.
cmdRes = assert.commandWorked(testDb.runCommand(Object.assign({
    update: coll.getName(),
    updates: [{q: {}, u: {$set: {"foo": NumberLong(3)}}}],
},
                                                              deterministicEncryptionSchema)));
assert.eq(cmdRes.hasEncryptionPlaceholders, true, cmdRes);
assert.eq(cmdRes.schemaRequiresEncryption, true, cmdRes);
assert(cmdRes.result.updates[0].u.$set.foo instanceof BinData, cmdRes);

// Cannot mark an int for encryption on $set-style update due to type mismatch.
assert.commandFailedWithCode(testDb.runCommand(Object.assign({
    update: coll.getName(),
    updates: [{q: {}, u: {$set: {"foo": NumberInt(3)}}}],
},
                                                             deterministicEncryptionSchema)),
                             31118);

// Can successfully compare the encrypted field to a long in a find command.
cmdRes = assert.commandWorked(testDb.runCommand(Object.assign({
    find: coll.getName(),
    filter: {foo: {$eq: NumberLong(3)}},
},
                                                              deterministicEncryptionSchema)));
assert.eq(cmdRes.hasEncryptionPlaceholders, true, cmdRes);
assert.eq(cmdRes.schemaRequiresEncryption, true, cmdRes);
assert(cmdRes.result.filter.foo.$eq instanceof BinData, cmdRes);

// Cannot compare the encrypted field to an int in a find command.
assert.commandFailedWithCode(testDb.runCommand(Object.assign({
    find: coll.getName(),
    filter: {foo: {$eq: NumberInt(3)}},
},
                                                             deterministicEncryptionSchema)),
                             31118);

// Cannot compare the encrypted field to an int in a count command.
assert.commandFailedWithCode(testDb.runCommand(Object.assign({
    count: coll.getName(),
    query: {foo: {$eq: NumberInt(3)}},
},
                                                             deterministicEncryptionSchema)),
                             31118);

// Distinct command is not supported in FLE 2.
if (!fle2Enabled()) {
    // Cannot compare the encrypted field to an int in a distinct command.
    assert.commandFailedWithCode(testDb.runCommand(Object.assign({
        distinct: coll.getName(),
        key: "key",
        query: {foo: {$eq: NumberInt(3)}},
    },
                                                                 deterministicEncryptionSchema)),
                                 31118);
}

// Cannot compare the encrypted field to an int in an aggregate command.
assert.commandFailedWithCode(testDb.runCommand(Object.assign({
    aggregate: coll.getName(),
    pipeline: [{$match: {foo: {$eq: NumberInt(3)}}}],
    cursor: {},
},
                                                             deterministicEncryptionSchema)),
                             31118);

// Cannot compare the encrypted field to an int in an update command.
assert.commandFailedWithCode(testDb.runCommand(Object.assign({
    update: coll.getName(),
    updates: [{q: {foo: {$eq: NumberInt(3)}}, u: {$set: {other: 1}}}],
},
                                                             deterministicEncryptionSchema)),
                             31118);

// Cannot compare the encrypted field to an int in a delete command.
assert.commandFailedWithCode(testDb.runCommand(Object.assign({
    delete: coll.getName(),
    deletes: [{q: {foo: {$eq: NumberInt(3)}}, limit: 1}],
},
                                                             deterministicEncryptionSchema)),
                             31118);

// Cannot compare the encrypted field to an int in a findAndModify command.
assert.commandFailedWithCode(testDb.runCommand(Object.assign({
    findAndModify: coll.getName(),
    query: {foo: {$eq: NumberInt(3)}},
    update: {$set: {other: 1}},
},
                                                             deterministicEncryptionSchema)),
                             31118);

if (!fle2Enabled()) {
    // A schema where fields beginning with "foo" are deterministically encrypted longs, and all
    // other fields are deterministically encrypted strings.
    const deterministicEncryptionPatternPropertiesSchema = {
        type: "object",
        encryptMetadata: {
            algorithm: kDeterministicAlgo,
            keyId: [UUID()],
        },
        patternProperties: {"^foo": {encrypt: {bsonType: "long"}}},
        additionalProperties: {encrypt: {bsonType: "string"}}
    };

    // Cannot compare a field beginning with "foo" to an int.
    assert.commandFailedWithCode(testDb.runCommand({
        find: coll.getName(),
        filter: {foobar: {$eq: NumberInt(3)}},
        jsonSchema: deterministicEncryptionPatternPropertiesSchema,
        isRemoteSchema: false
    }),
                                 31118);

    // Can compare a field beginning with "foo" to a long.
    cmdRes = assert.commandWorked(testDb.runCommand({
        find: coll.getName(),
        filter: {foobar: {$eq: NumberLong(3)}},
        jsonSchema: deterministicEncryptionPatternPropertiesSchema,
        isRemoteSchema: false
    }));
    assert.eq(cmdRes.hasEncryptionPlaceholders, true, cmdRes);
    assert.eq(cmdRes.schemaRequiresEncryption, true, cmdRes);
    assert(cmdRes.result.filter.foobar.$eq instanceof BinData, cmdRes);

    // Cannot compare a field which does not begin with "foo" to a long.
    assert.commandFailedWithCode(testDb.runCommand({
        find: coll.getName(),
        filter: {barfoo: {$eq: NumberLong(3)}},
        jsonSchema: deterministicEncryptionPatternPropertiesSchema,
        isRemoteSchema: false
    }),
                                 31118);

    // Can compare a field which does not begin with "foo" to a string.
    cmdRes = assert.commandWorked(testDb.runCommand({
        find: coll.getName(),
        filter: {barfoo: {$eq: "string"}},
        jsonSchema: deterministicEncryptionPatternPropertiesSchema,
        isRemoteSchema: false
    }));
    assert.eq(cmdRes.hasEncryptionPlaceholders, true, cmdRes);
    assert.eq(cmdRes.schemaRequiresEncryption, true, cmdRes);
    assert(cmdRes.result.filter.barfoo.$eq instanceof BinData, cmdRes);
}

mongocryptd.stop();
