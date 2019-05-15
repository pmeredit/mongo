/**
 * Test that mongocrypt's analysis works correctly for collation.
 */
(function() {
    "use strict";

    load("src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js");

    const mongocryptd = new MongoCryptD();
    mongocryptd.start();

    const conn = mongocryptd.getConnection();
    const testDb = conn.getDB("test");
    const coll = testDb.fle_collation;

    /**
     * Construct a JSON Schema indicating that "foo.bar" should be encrypted with the deterministic
     * algoritm, and such that the bsonType of the encrypted field must be 'bsonTypeAlias'.
     */
    function makeFooDotBarEncryptedSchemaWithBsonType(bsonTypeAlias) {
        return {
            type: "object",
            properties: {
                foo: {
                    type: "object",
                    properties: {
                        bar: {
                            encrypt: {
                                algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
                                keyId: [UUID()],
                                bsonType: bsonTypeAlias
                            }
                        }
                    }
                }
            }
        };
    }

    const encryptedStringSchema = makeFooDotBarEncryptedSchemaWithBsonType("string");
    const encryptedIntSchema = makeFooDotBarEncryptedSchemaWithBsonType("int");

    let cmdRes;

    // Test that a find command throws if there is a $eq comparison to an encrypted string using a
    // non-simple collation.
    assert.commandFailedWithCode(testDb.runCommand({
        find: coll.getName(),
        filter: {"foo.bar": "string"},
        collation: {locale: "fr_CA"},
        jsonSchema: encryptedStringSchema
    }),
                                 31054);

    // Test that a find command succeeds if the $eq comparison to an encrypted field does not
    // compare to a string, even if the command specifies a non-simple collation.
    cmdRes = assert.commandWorked(testDb.runCommand({
        find: coll.getName(),
        filter: {"foo.bar": NumberInt(4)},
        collation: {locale: "fr_CA"},
        jsonSchema: encryptedIntSchema
    }));
    assert.eq(true, cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);
    assert(cmdRes.result.filter["foo.bar"].$eq instanceof BinData, cmdRes);

    // Test that a find command throws if there is a $in comparison to an encrypted string using a
    // non-simple collation.
    assert.commandFailedWithCode(testDb.runCommand({
        find: coll.getName(),
        filter: {"foo.bar": {$in: ["string1", "string2"]}},
        collation: {locale: "fr_CA"},
        jsonSchema: encryptedStringSchema
    }),
                                 31054);

    // Test that a find command succeeds if there is a $in comparison to an encrypted field using a
    // non-simple collation, but none of the $in elements are strings.
    cmdRes = assert.commandWorked(testDb.runCommand({
        find: coll.getName(),
        filter: {"foo.bar": {$in: [NumberInt(1), NumberInt(2), NumberInt(3), NumberInt(4)]}},
        collation: {locale: "fr_CA"},
        jsonSchema: encryptedIntSchema
    }));
    assert.eq(true, cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);
    for (let element of cmdRes.result.filter["foo.bar"].$in) {
        assert(element instanceof BinData, cmdRes);
    }

    // Find should fail if the collation-aware comparison to an encrypted string is done via an
    // equality to an object predicate.
    assert.commandFailedWithCode(testDb.runCommand({
        find: coll.getName(),
        filter: {foo: {bar: "string"}},
        collation: {locale: "fr_CA"},
        jsonSchema: encryptedStringSchema
    }),
                                 31054);

    // Find should fail if the collation-aware comparison to an encrypted string is done via an
    // equality to an object predicate inside a $in.
    assert.commandFailedWithCode(testDb.runCommand({
        find: coll.getName(),
        filter: {foo: {$in: [1, {bar: "string"}, 3]}},
        collation: {locale: "fr_CA"},
        jsonSchema: encryptedStringSchema
    }),
                                 31054);

    // Test that a distinct command throws if there is a $eq comparison to an encrypted string using
    // a non-simple collation.
    assert.commandFailedWithCode(testDb.runCommand({
        distinct: coll.getName(),
        key: "baz",
        query: {"foo.bar": "string"},
        collation: {locale: "fr_CA"},
        jsonSchema: encryptedStringSchema
    }),
                                 31054);

    // Distinct should throw if the distinct key is an encrypted field and there is a non-simple
    // collation.
    assert.commandFailedWithCode(testDb.runCommand({
        distinct: coll.getName(),
        key: "foo.bar",
        collation: {locale: "fr_CA"},
        jsonSchema: encryptedStringSchema
    }),
                                 31058);

    // Distinct should not throw if the distinct key is an encrypted field but the collation is
    // simple.
    cmdRes = assert.commandWorked(testDb.runCommand({
        distinct: coll.getName(),
        key: "foo.bar",
        collation: {locale: "simple"},
        jsonSchema: encryptedStringSchema
    }));
    assert.eq(false, cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);

    // Test that a delete command throws if there is a $eq comparison to an encrypted string using
    // a non-simple collation.
    assert.commandFailedWithCode(testDb.runCommand({
        delete: coll.getName(),
        deletes: [
            {q: {"foo.bar": "string"}, limit: 1, collation: {locale: "fr_CA"}},
        ],
        jsonSchema: encryptedStringSchema
    }),
                                 31054);

    // Test that a delete command succeeds if no individual delete statement ever makes a
    // collation-aware string comparison.
    cmdRes = assert.commandWorked(testDb.runCommand({
        delete: coll.getName(),
        deletes: [
            {q: {"foo.bar": "string"}, limit: 1, collation: {locale: "simple"}},
            {q: {"foo.bar": "string"}, limit: 1},
            {q: {}, limit: 1, collation: {locale: "fr_CA"}},
        ],
        jsonSchema: encryptedStringSchema
    }));
    assert.eq(true, cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);

    // Test that an update command throws if there is a $eq comparison to an encrypted string using
    // a non-simple collation.
    assert.commandFailedWithCode(testDb.runCommand({
        update: coll.getName(),
        updates: [
            {q: {"foo.bar": "string"}, u: {$set: {other: 1}}, collation: {locale: "fr_CA"}},
        ],
        jsonSchema: encryptedStringSchema
    }),
                                 31054);

    // Test that an update command succeeds if no individual update statement ever makes a
    // collation-aware string comparison.
    cmdRes = assert.commandWorked(testDb.runCommand({
        update: coll.getName(),
        updates: [
            {q: {"foo.bar": "string"}, u: {$set: {other: 1}}, collation: {locale: "simple"}},
            {q: {"foo.bar": "string"}, u: {$set: {other: 1}}},
            {q: {}, u: {$set: {other: 1}}, collation: {locale: "fr_CA"}},
        ],
        jsonSchema: encryptedStringSchema
    }));
    assert.eq(true, cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);

    // $set an encrypted field to a string with a non-simple collation is legal.
    cmdRes = assert.commandWorked(testDb.runCommand({
        update: coll.getName(),
        updates: [
            {q: {}, u: {$set: {"foo.bar": "string"}}, collation: {locale: "fr_CA"}},
        ],
        jsonSchema: encryptedStringSchema
    }));
    assert.eq(true, cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);

    // Replacement update which places a string inside an encrypted field with a non-simple
    // collation is legal.
    cmdRes = assert.commandWorked(testDb.runCommand({
        update: coll.getName(),
        updates: [
            {q: {}, u: {foo: {bar: "string"}}, collation: {locale: "fr_CA"}},
        ],
        jsonSchema: encryptedStringSchema
    }));
    assert.eq(true, cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);

    // $set which places a string inside an encrypted field with a non-simple collation is legal.
    cmdRes = assert.commandWorked(testDb.runCommand({
        update: coll.getName(),
        updates: [
            {q: {}, u: {$set: {"foo.bar": "string"}}, collation: {locale: "fr_CA"}},
        ],
        jsonSchema: encryptedStringSchema
    }));
    assert.eq(true, cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);

    // $set to a nested object which places a string inside an encrypted field with a non-simple
    // collation is legal.
    cmdRes = assert.commandWorked(testDb.runCommand({
        update: coll.getName(),
        updates: [
            {q: {}, u: {$set: {foo: {bar: "string"}}}, collation: {locale: "fr_CA"}},
        ],
        jsonSchema: encryptedStringSchema
    }));
    assert.eq(true, cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);

    // Test that a count command throws if there is a $eq comparison to an encrypted string using a
    // non-simple collation.
    assert.commandFailedWithCode(testDb.runCommand({
        count: coll.getName(),
        query: {"foo.bar": "string"},
        collation: {locale: "fr_CA"},
        jsonSchema: encryptedStringSchema
    }),
                                 31054);

    // Test that a count command succeeds if there is a $eq comparison to an encrypted non-string
    // using a non-simple collation.
    cmdRes = assert.commandWorked(testDb.runCommand({
        count: coll.getName(),
        query: {"foo.bar": NumberInt(1)},
        collation: {locale: "fr_CA"},
        jsonSchema: encryptedIntSchema
    }));
    assert.eq(true, cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);

    // Test that explain of a find command fails when the find command would need to make a
    // collation-aware comparison to a string.
    assert.commandFailedWithCode(testDb.runCommand({
        explain:
            {find: coll.getName(), filter: {"foo.bar": "string"}, collation: {locale: "fr_CA"}},
        jsonSchema: encryptedStringSchema
    }),
                                 31054);

    // Test that explain of a find command succeeds when the find command makes a comparison to an
    // encrypted field and has a non-simple collation, but the encrypted field is not a string.
    cmdRes = assert.commandWorked(testDb.runCommand({
        explain:
            {find: coll.getName(), filter: {"foo.bar": NumberInt(1)}, collation: {locale: "fr_CA"}},
        jsonSchema: encryptedIntSchema
    }));
    assert.eq(true, cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);

    // Test that a findAndModify command fails if there is a $eq comparison to an encrypted string
    // using a non-simple collation.
    assert.commandFailedWithCode(testDb.runCommand({
        findAndModify: coll.getName(),
        query: {"foo.bar": "string"},
        update: {$set: {baz: "other"}},
        collation: {locale: "fr_CA"},
        jsonSchema: encryptedStringSchema
    }),
                                 31054);

    // Test that a findAndModify command succeeds if there is a $eq comparison to an encrypted
    // non-string using a non-simple collation.
    cmdRes = assert.commandWorked(testDb.runCommand({
        findAndModify: coll.getName(),
        query: {"foo.bar": NumberInt(1)},
        update: {$set: {baz: "other"}},
        collation: {locale: "fr_CA"},
        jsonSchema: encryptedIntSchema
    }));
    assert.eq(true, cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);

    // Using findAndModify to $set an encrypted field to a string with a non-simple collation is
    // legal.
    cmdRes = assert.commandWorked(testDb.runCommand({
        findAndModify: coll.getName(),
        query: {},
        update: {$set: {"foo.bar": "string"}},
        collation: {locale: "fr_CA"},
        jsonSchema: encryptedStringSchema
    }));
    assert.eq(true, cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);

    // Test that an aggregate command throws if there is a $eq comparison to an encrypted string
    // using a non-simple collation.
    assert.commandFailedWithCode(testDb.runCommand({
        aggregate: coll.getName(),
        pipeline: [{$match: {"foo.bar": "string"}}],
        cursor: {},
        collation: {locale: "fr_CA"},
        jsonSchema: encryptedStringSchema
    }),
                                 31054);

    // Test that an aggregate command succeeds if there is a $eq comparison to an encrypted
    // non-string using a non-simple collation.
    cmdRes = assert.commandWorked(testDb.runCommand({
        aggregate: coll.getName(),
        pipeline: [{$match: {"foo.bar": NumberInt(1)}}],
        cursor: {},
        collation: {locale: "fr_CA"},
        jsonSchema: encryptedIntSchema
    }));
    assert.eq(true, cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);

    // Test that mongocryptd returns an error when the collation parameter is not an object.
    assert.commandFailedWithCode(
        testDb.runCommand({find: coll.getName(), collation: 1, jsonSchema: encryptedStringSchema}),
        31084);
    assert.commandFailedWithCode(testDb.runCommand({
        distinct: coll.getName(),
        key: "baz",
        collation: 1,
        jsonSchema: encryptedStringSchema
    }),
                                 31084);
    assert.commandFailed(testDb.runCommand({
        update: coll.getName(),
        updates: [{q: {}, u: {$set: {other: 1}}, collation: 1}],
        jsonSchema: encryptedStringSchema
    }));
    assert.commandFailed(testDb.runCommand({
        delete: coll.getName(),
        deletes: [{q: {}, limit: 1, collation: 1}],
        jsonSchema: encryptedStringSchema
    }));
    assert.commandFailedWithCode(testDb.runCommand({
        count: coll.getName(),
        query: {"foo.bar": "string"},
        collation: 1,
        jsonSchema: encryptedStringSchema
    }),
                                 31084);
    assert.commandFailedWithCode(testDb.runCommand({
        findAndModify: coll.getName(),
        query: {"foo.bar": "string"},
        update: {$set: {baz: "other"}},
        collation: 1,
        jsonSchema: encryptedStringSchema
    }),
                                 31084);

    // Test that mongocryptd returns an error when the collation is specified as a top-level
    // parameter to an update or delete rather than with each individual write statement.
    assert.commandFailed(testDb.runCommand({
        update: coll.getName(),
        updates: [{q: {}, u: {$set: {other: 1}}}],
        collation: {locale: "simple"},
        jsonSchema: encryptedStringSchema
    }));
    assert.commandFailed(testDb.runCommand({
        delete: coll.getName(),
        deletes: [{q: {}, limit: 1}],
        collation: {locale: "simple"},
        jsonSchema: encryptedStringSchema
    }));

    // Mongocryptd does not actually understand the collation specification, and is not expected to
    // error if the collation is invalid. Mongod retains sole responsibility for raising an error if
    // the collation is invalid. Characterize that behavior here.
    assert.commandWorked(testDb.runCommand({
        find: coll.getName(),
        collation: {locale: "unknown_locale"},
        jsonSchema: encryptedStringSchema
    }));
    assert.commandWorked(testDb.runCommand(
        {find: coll.getName(), collation: {}, jsonSchema: encryptedStringSchema}));
    assert.commandWorked(testDb.runCommand(
        {find: coll.getName(), collation: {unknown: 1}, jsonSchema: encryptedStringSchema}));

    mongocryptd.stop();
}());
