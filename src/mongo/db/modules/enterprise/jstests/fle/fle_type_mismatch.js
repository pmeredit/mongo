/**
 * Test that mongocryptd produces an error when the type of an element being marked for encryption
 * does not comply with the schema.
 */
(function() {
    "use strict";

    load("src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js");

    const mongocryptd = new MongoCryptD();
    mongocryptd.start();

    const conn = mongocryptd.getConnection();
    const testDb = conn.getDB("test");
    const coll = testDb.fle_type_mismatch;

    // A schema where the 'foo' field may either be a string or a double and is encrypted with the
    // random encryption algorithm.
    const multipleTypesRandomEncryptionSchema = {
        type: "object",
        properties: {
            foo: {
                encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                    keyId: [UUID()],
                    bsonType: ["string", "double"]
                }
            }
        }
    };

    let cmdRes;

    // Can successfully mark a string element for encryption on insert.
    cmdRes = assert.commandWorked(testDb.runCommand({
        insert: coll.getName(),
        documents: [{"foo": "bar"}],
        jsonSchema: multipleTypesRandomEncryptionSchema
    }));
    assert.eq(cmdRes.hasEncryptionPlaceholders, true, cmdRes);
    assert.eq(cmdRes.schemaRequiresEncryption, true, cmdRes);
    assert(cmdRes.result.documents[0].foo instanceof BinData, cmdRes);

    // Can successfully mark a double element for encryption on insert.
    cmdRes = assert.commandWorked(testDb.runCommand({
        insert: coll.getName(),
        documents: [{"foo": 3}],
        jsonSchema: multipleTypesRandomEncryptionSchema
    }));
    assert.eq(cmdRes.hasEncryptionPlaceholders, true, cmdRes);
    assert.eq(cmdRes.schemaRequiresEncryption, true, cmdRes);
    assert(cmdRes.result.documents[0].foo instanceof BinData, cmdRes);

    // Cannot mark an int for encryption on insert, since this would not comply with the schema.
    assert.commandFailedWithCode(testDb.runCommand({
        insert: coll.getName(),
        documents: [{"foo": NumberInt(3)}],
        jsonSchema: multipleTypesRandomEncryptionSchema
    }),
                                 31118);

    // Can successfully mark a string element for encryption on $set-style update.
    cmdRes = assert.commandWorked(testDb.runCommand({
        update: coll.getName(),
        updates: [{q: {}, u: {$set: {"foo": "bar"}}}],
        jsonSchema: multipleTypesRandomEncryptionSchema
    }));
    assert.eq(cmdRes.hasEncryptionPlaceholders, true, cmdRes);
    assert.eq(cmdRes.schemaRequiresEncryption, true, cmdRes);
    assert(cmdRes.result.updates[0].u.$set.foo instanceof BinData, cmdRes);

    // Cannot mark an int for encryption on $set-style update due to type mismatch.
    assert.commandFailedWithCode(testDb.runCommand({
        update: coll.getName(),
        updates: [{q: {}, u: {$set: {"foo": NumberInt(3)}}}],
        jsonSchema: multipleTypesRandomEncryptionSchema
    }),
                                 31118);

    // Can successfully mark a string element for encryption on replacement-style update.
    cmdRes = assert.commandWorked(testDb.runCommand({
        update: coll.getName(),
        updates: [{q: {}, u: {"foo": "bar"}}],
        jsonSchema: multipleTypesRandomEncryptionSchema
    }));
    assert.eq(cmdRes.hasEncryptionPlaceholders, true, cmdRes);
    assert.eq(cmdRes.schemaRequiresEncryption, true, cmdRes);
    assert(cmdRes.result.updates[0].u.foo instanceof BinData, cmdRes);

    // Cannot mark an int for encryption on replacement-style update due to type mismatch.
    assert.commandFailedWithCode(testDb.runCommand({
        update: coll.getName(),
        updates: [{q: {}, u: {"foo": NumberInt(3)}}],
        jsonSchema: multipleTypesRandomEncryptionSchema
    }),
                                 31118);

    // Cannot mark an int for encryption on update expressed with findAndModify due to type
    // mismatch.
    assert.commandFailedWithCode(testDb.runCommand({
        findAndModify: coll.getName(),
        query: {},
        update: {$set: {foo: NumberInt(3)}},
        jsonSchema: multipleTypesRandomEncryptionSchema
    }),
                                 31118);

    // Type mismatch for a query constant doesn't matter for this schema, since equality queries
    // against a field encrypted with the random algorithm are illegal.
    assert.commandFailedWithCode(testDb.runCommand({
        find: coll.getName(),
        filter: {foo: {$eq: NumberInt(3)}},
        jsonSchema: multipleTypesRandomEncryptionSchema
    }),
                                 51158);

    // A schema where the 'foo' field must be double and is encrypted with the deterministic
    // encryption algorithm.
    const deterministicEncryptionSchema = {
        type: "object",
        properties: {
            foo: {
                encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
                    keyId: [UUID()],
                    bsonType: ["double"],
                }
            }
        }
    };

    // Can successfully mark a double element for encryption on insert.
    cmdRes = assert.commandWorked(testDb.runCommand({
        insert: coll.getName(),
        documents: [{"foo": 3}],
        jsonSchema: deterministicEncryptionSchema
    }));
    assert.eq(cmdRes.hasEncryptionPlaceholders, true, cmdRes);
    assert.eq(cmdRes.schemaRequiresEncryption, true, cmdRes);
    assert(cmdRes.result.documents[0].foo instanceof BinData, cmdRes);

    // Cannot mark an int for encryption on insert, since this would not comply with the schema.
    assert.commandFailedWithCode(testDb.runCommand({
        insert: coll.getName(),
        documents: [{"foo": NumberInt(3)}],
        jsonSchema: deterministicEncryptionSchema
    }),
                                 31118);

    // Can successfully mark a double for encryption on $set-style update.
    cmdRes = assert.commandWorked(testDb.runCommand({
        update: coll.getName(),
        updates: [{q: {}, u: {$set: {"foo": 3}}}],
        jsonSchema: deterministicEncryptionSchema
    }));
    assert.eq(cmdRes.hasEncryptionPlaceholders, true, cmdRes);
    assert.eq(cmdRes.schemaRequiresEncryption, true, cmdRes);
    assert(cmdRes.result.updates[0].u.$set.foo instanceof BinData, cmdRes);

    // Cannot mark an int for encryption on $set-style update due to type mismatch.
    assert.commandFailedWithCode(testDb.runCommand({
        update: coll.getName(),
        updates: [{q: {}, u: {$set: {"foo": NumberInt(3)}}}],
        jsonSchema: deterministicEncryptionSchema
    }),
                                 31118);

    // Can successfully compare the encrypted field to a double in a find command.
    cmdRes = assert.commandWorked(testDb.runCommand({
        find: coll.getName(),
        filter: {foo: {$eq: 3}},
        jsonSchema: deterministicEncryptionSchema
    }));
    assert.eq(cmdRes.hasEncryptionPlaceholders, true, cmdRes);
    assert.eq(cmdRes.schemaRequiresEncryption, true, cmdRes);
    assert(cmdRes.result.filter.foo.$eq instanceof BinData, cmdRes);

    // Cannot compare the encrypted field to an int in a find command.
    assert.commandFailedWithCode(testDb.runCommand({
        find: coll.getName(),
        filter: {foo: {$eq: NumberInt(3)}},
        jsonSchema: deterministicEncryptionSchema
    }),
                                 31118);

    // Cannot compare the encrypted field to an int in a count command.
    assert.commandFailedWithCode(testDb.runCommand({
        count: coll.getName(),
        query: {foo: {$eq: NumberInt(3)}},
        jsonSchema: deterministicEncryptionSchema
    }),
                                 31118);

    // Cannot compare the encrypted field to an int in a distinct command.
    assert.commandFailedWithCode(testDb.runCommand({
        distinct: coll.getName(),
        key: "key",
        query: {foo: {$eq: NumberInt(3)}},
        jsonSchema: deterministicEncryptionSchema
    }),
                                 31118);

    // Cannot compare the encrypted field to an int in an aggregate command.
    assert.commandFailedWithCode(testDb.runCommand({
        aggregate: coll.getName(),
        pipeline: [{$match: {foo: {$eq: NumberInt(3)}}}],
        cursor: {},
        jsonSchema: deterministicEncryptionSchema
    }),
                                 31118);

    // Cannot compare the encrypted field to an int in an update command.
    assert.commandFailedWithCode(testDb.runCommand({
        update: coll.getName(),
        updates: [{q: {foo: {$eq: NumberInt(3)}}, u: {$set: {other: 1}}}],
        jsonSchema: deterministicEncryptionSchema
    }),
                                 31118);

    // Cannot compare the encrypted field to an int in a delete command.
    assert.commandFailedWithCode(testDb.runCommand({
        delete: coll.getName(),
        deletes: [{q: {foo: {$eq: NumberInt(3)}}, limit: 1}],
        jsonSchema: deterministicEncryptionSchema
    }),
                                 31118);

    // Cannot compare the encrypted field to an int in a findAndModify command.
    assert.commandFailedWithCode(testDb.runCommand({
        findAndModify: coll.getName(),
        query: {foo: {$eq: NumberInt(3)}},
        update: {$set: {other: 1}},
        jsonSchema: deterministicEncryptionSchema
    }),
                                 31118);

    // A schema where fields beginning with "foo" are deterministically encrypted doubles, and all
    // other fields are deterministically encrypted strings.
    const deterministicEncryptionPatternPropertiesSchema = {
        type: "object",
        encryptMetadata: {
            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
            keyId: [UUID()],
        },
        patternProperties: {"^foo": {encrypt: {bsonType: "double"}}},
        additionalProperties: {encrypt: {bsonType: "string"}}
    };

    // Cannot compare a field beginning with "foo" to an int.
    assert.commandFailedWithCode(testDb.runCommand({
        find: coll.getName(),
        filter: {foobar: {$eq: NumberInt(3)}},
        jsonSchema: deterministicEncryptionPatternPropertiesSchema
    }),
                                 31118);

    // Can compare a field beginning with "foo" to a double.
    cmdRes = assert.commandWorked(testDb.runCommand({
        find: coll.getName(),
        filter: {foobar: {$eq: 3}},
        jsonSchema: deterministicEncryptionPatternPropertiesSchema
    }));
    assert.eq(cmdRes.hasEncryptionPlaceholders, true, cmdRes);
    assert.eq(cmdRes.schemaRequiresEncryption, true, cmdRes);
    assert(cmdRes.result.filter.foobar.$eq instanceof BinData, cmdRes);

    // Cannot compare a field which does not begin with "foo" to a double.
    assert.commandFailedWithCode(testDb.runCommand({
        find: coll.getName(),
        filter: {barfoo: {$eq: 3}},
        jsonSchema: deterministicEncryptionPatternPropertiesSchema
    }),
                                 31118);

    // Can compare a field which does not begin with "foo" to a string.
    cmdRes = assert.commandWorked(testDb.runCommand({
        find: coll.getName(),
        filter: {barfoo: {$eq: "string"}},
        jsonSchema: deterministicEncryptionPatternPropertiesSchema
    }));
    assert.eq(cmdRes.hasEncryptionPlaceholders, true, cmdRes);
    assert.eq(cmdRes.schemaRequiresEncryption, true, cmdRes);
    assert(cmdRes.result.filter.barfoo.$eq instanceof BinData, cmdRes);

    mongocryptd.stop();
}());
