/**
 * Test that mongocryptd errors if users either:
 *  - Issue writes that would store a whole array in an encrypted field when
      a deterministic algorithm is used.
 *  - Issue writes that would put an encrypted field inside of an array.
 *  - Issue reads that imply there can be a whole array stored inside an encrypted field.
 *  - Issue reads that imply encrypted fields can be nested beneath an array.
 */
(function() {
    "use strict";

    load("src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js");

    const mongocryptd = new MongoCryptD();
    mongocryptd.start();

    const conn = mongocryptd.getConnection();
    const testDb = conn.getDB("test");
    const coll = testDb.encrypt_with_arrays;

    const encryptObj = {
        encrypt: {
            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
            keyId: [UUID(), UUID()],
            bsonType: "int"
        }
    };
    const fooEncryptedSchema = {type: "object", properties: {foo: encryptObj}};
    const fooDotBarEncryptedSchema = {
        type: "object",
        properties: {foo: {type: "object", properties: {bar: encryptObj}}}
    };
    const fooDotBarDotBazEncryptedSchema = {
        type: "object",
        properties: {
            foo: {
                type: "object",
                properties: {bar: {type: "object", properties: {baz: encryptObj}}}
            }
        }
    };

    // Verify that an insert command where 'foo' is an array fails when 'foo' is marked for
    // encryption.
    assert.commandFailedWithCode(testDb.runCommand({
        insert: coll.getName(),
        documents: [{_id: 1, foo: [1, 2, 3]}],
        jsonSchema: fooEncryptedSchema
    }),
                                 31009);
    assert.commandFailedWithCode(testDb.runCommand({
        insert: coll.getName(),
        documents: [{_id: 1, foo: [{bar: 1}, {bar: 2}, {bar: 3}]}],
        jsonSchema: fooEncryptedSchema
    }),
                                 31009);

    // Verify that an insert command where 'foo.bar' is an array fails when 'foo.bar' is marked for
    // encryption.
    assert.commandFailedWithCode(testDb.runCommand({
        insert: coll.getName(),
        documents: [{_id: 1, foo: {bar: [1, 2, 3]}}],
        jsonSchema: fooDotBarEncryptedSchema
    }),
                                 31009);
    assert.commandFailedWithCode(testDb.runCommand({
        insert: coll.getName(),
        documents: [{_id: 1, foo: {bar: [{baz: 1}, {baz: 2}, {baz: 3}]}}],
        jsonSchema: fooDotBarEncryptedSchema
    }),
                                 31009);

    // Verify that an insert command where 'foo' is an array fails when 'foo.bar' is marked for
    // encryption.
    assert.commandFailedWithCode(testDb.runCommand({
        insert: coll.getName(),
        documents: [{_id: 1, foo: [{bar: 1}, {bar: 2}, {bar: 3}]}],
        jsonSchema: fooDotBarEncryptedSchema
    }),
                                 31006);

    // The insert command should fail when 'foo' is an array and 'foo.bar' is marked for encryption
    // even if the path 'foo.bar' does not exist in the document to be inserted.
    assert.commandFailedWithCode(testDb.runCommand({
        insert: coll.getName(),
        documents: [{_id: 1, foo: [{a: 1}, 2, {b: 3}]}],
        jsonSchema: fooDotBarEncryptedSchema
    }),
                                 31006);

    // Verify that an insert command where 'foo' is an array fails when 'foo.bar.baz' is marked for
    // encryption.
    assert.commandFailedWithCode(testDb.runCommand({
        insert: coll.getName(),
        documents: [{_id: 1, foo: [{bar: {baz: 1}}, {bar: {baz: 2}}]}],
        jsonSchema: fooDotBarDotBazEncryptedSchema
    }),
                                 31006);

    // Verify that an insert command where 'foo.bar' is an array fails when 'foo.bar.baz' is marked
    // for encryption.
    assert.commandFailedWithCode(testDb.runCommand({
        insert: coll.getName(),
        documents: [{_id: 1, foo: {bar: [{baz: 1}, {baz: 2}]}}],
        jsonSchema: fooDotBarDotBazEncryptedSchema
    }),
                                 31006);

    // Verify that a $set inside an update cannot create an array along an encrypted path.
    assert.commandFailedWithCode(testDb.runCommand({
        update: coll.getName(),
        updates: [{q: {}, u: {$set: {foo: [1, 2, 3]}}}],
        jsonSchema: fooEncryptedSchema
    }),
                                 31009);
    assert.commandFailedWithCode(testDb.runCommand({
        update: coll.getName(),
        updates: [{q: {}, u: {$set: {foo: {bar: [1, 2, 3]}}}}],
        jsonSchema: fooDotBarEncryptedSchema
    }),
                                 31009);
    assert.commandFailedWithCode(testDb.runCommand({
        update: coll.getName(),
        updates: [{q: {}, u: {$set: {foo: [{bar: 1}]}}}],
        jsonSchema: fooDotBarEncryptedSchema
    }),
                                 51159);

    // Verify that a $set inside an update cannot create an array along an encrypted path when the
    // upsert flag is true.
    assert.commandFailedWithCode(testDb.runCommand({
        update: coll.getName(),
        updates: [{q: {}, u: {$set: {foo: {bar: [1, 2, 3]}}}, upsert: true}],
        jsonSchema: fooDotBarEncryptedSchema
    }),
                                 31009);
    assert.commandFailedWithCode(testDb.runCommand({
        update: coll.getName(),
        updates: [{q: {}, u: {$set: {foo: [{bar: 1}]}}, upsert: true}],
        jsonSchema: fooDotBarEncryptedSchema
    }),
                                 51159);

    // Verify that a replacement style update cannot create an array along an encrypted path.
    assert.commandFailedWithCode(testDb.runCommand({
        update: coll.getName(),
        updates: [{q: {}, u: {foo: [1, 2, 3]}}],
        jsonSchema: fooEncryptedSchema
    }),
                                 31009);
    assert.commandFailedWithCode(testDb.runCommand({
        update: coll.getName(),
        updates: [{q: {}, u: {foo: {bar: [1, 2, 3]}}}],
        jsonSchema: fooDotBarEncryptedSchema
    }),
                                 31009);
    assert.commandFailedWithCode(testDb.runCommand({
        update: coll.getName(),
        updates: [{q: {}, u: {foo: [{bar: 1}]}}],
        jsonSchema: fooDotBarEncryptedSchema
    }),
                                 31006);

    // An update command whose query predicate implies the existence of an array along an encrypted
    // path should fail.
    assert.commandFailedWithCode(testDb.runCommand({
        update: coll.getName(),
        updates: [{q: {foo: {$eq: {bar: {baz: [1, 2, 3]}}}}, u: {$set: {notEncrypted: 1}}}],
        jsonSchema: fooDotBarDotBazEncryptedSchema
    }),
                                 31009);
    assert.commandFailedWithCode(testDb.runCommand({
        update: coll.getName(),
        updates: [{q: {foo: {$eq: {bar: [{baz: 1}]}}}, u: {$set: {notEncrypted: 1}}}],
        jsonSchema: fooDotBarDotBazEncryptedSchema
    }),
                                 31006);

    // A find command whose predicate implies the existence of an array along an encrypted path
    // should fail.
    assert.commandFailedWithCode(testDb.runCommand({
        find: coll.getName(),
        filter: {foo: {$eq: {bar: {baz: [1, 2, 3]}}}},
        jsonSchema: fooDotBarDotBazEncryptedSchema
    }),
                                 31009);
    assert.commandFailedWithCode(testDb.runCommand({
        find: coll.getName(),
        filter: {foo: {$eq: {bar: [{baz: 1}, {baz: 2}]}}},
        jsonSchema: fooDotBarDotBazEncryptedSchema
    }),
                                 31006);

    // A delete command whose predicate implies the existence of an array along an encrypted path
    // should fail.
    assert.commandFailedWithCode(testDb.runCommand({
        delete: coll.getName(),
        deletes: [{q: {foo: {$eq: {bar: {baz: [1, 2, 3]}}}}, limit: 1}],
        jsonSchema: fooDotBarDotBazEncryptedSchema
    }),
                                 31009);
    assert.commandFailedWithCode(testDb.runCommand({
        delete: coll.getName(),
        deletes: [{q: {foo: {$eq: {bar: [{baz: 1}, {baz: 2}]}}}, limit: 1}],
        jsonSchema: fooDotBarDotBazEncryptedSchema
    }),
                                 31006);

    // An equality to an array predicate should result in an error from mongocryptd if the
    // predicate's path is the prefix of an encrypted path.
    assert.commandFailedWithCode(testDb.runCommand({
        find: coll.getName(),
        filter: {foo: {$eq: [{bar: 1}, {bar: 2}]}},
        jsonSchema: fooDotBarEncryptedSchema
    }),
                                 31007);
    assert.commandFailedWithCode(testDb.runCommand({
        update: coll.getName(),
        updates: [{q: {foo: {$eq: [{bar: 1}, {bar: 2}]}}, u: {$set: {notEncrypted: 1}}}],
        jsonSchema: fooDotBarEncryptedSchema
    }),
                                 31007);

    // An $in element that is itself an array should result in an error from mongocryptd if the
    // $in predicate's path is the prefix of an encrypted path.
    assert.commandFailedWithCode(testDb.runCommand({
        find: coll.getName(),
        filter: {foo: {$in: [1, [{bar: 2}]]}},
        jsonSchema: fooDotBarEncryptedSchema
    }),
                                 31008);
    assert.commandFailedWithCode(testDb.runCommand({
        update: coll.getName(),
        updates: [{q: {foo: {$in: [1, [{bar: 2}]]}}, u: {$set: {notEncrypted: 1}}}],
        jsonSchema: fooDotBarEncryptedSchema
    }),
                                 31008);

    // An equality-to-array predicate against an encrypted path should result in an error.
    assert.commandFailedWithCode(testDb.runCommand({
        find: coll.getName(),
        filter: {foo: {$eq: [1, 2, 3]}},
        jsonSchema: fooEncryptedSchema
    }),
                                 31009);
    assert.commandFailedWithCode(testDb.runCommand({
        find: coll.getName(),
        filter: {foo: {$in: [[1, 2, 3]]}},
        jsonSchema: fooEncryptedSchema
    }),
                                 31009);

    // Deterministic encryption of an object is not legal, since the schema specifies that 'foo'
    // must be of type "int".
    assert.commandFailedWithCode(testDb.runCommand({
        insert: coll.getName(),
        documents: [{_id: 1, foo: {bar: [1, 2, 3]}}],
        jsonSchema: fooEncryptedSchema
    }),
                                 31118);

    // Can insert an encrypted object, even if that object contains a nested array, when the
    // encryption algorithm is random.
    const fooEncryptedRandomSchema = {
        type: "object",
        properties: {
            foo: {
                encrypt: {
                    algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                    keyId: [UUID(), UUID()],
                }
            }
        }
    };
    assert.commandWorked(testDb.runCommand({
        insert: coll.getName(),
        documents: [{_id: 1, foo: {bar: [1, 2, 3]}}],
        jsonSchema: fooEncryptedRandomSchema
    }));

    // Cannot evaluate an $eq-to-object predicate when the object is encrypted with the random
    // algorithm.
    assert.commandFailedWithCode(testDb.runCommand({
        find: coll.getName(),
        filter: {foo: {$eq: {bar: [1, 2, 3]}}},
        jsonSchema: fooEncryptedRandomSchema
    }),
                                 51158);

    // The schema cannot specify the 'array' bsonType with deterministic encryption. Verify that
    // this results in an error for an insert that does not involve the encrypted field.
    assert.commandFailedWithCode(testDb.runCommand({
        insert: coll.getName(),
        documents: [{_id: 1}],
        jsonSchema: {
            type: "object",
            properties: {
                foo: {
                    encrypt: {
                        algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
                        keyId: [UUID(), UUID()],
                        bsonType: "array"
                    }
                }
            }
        }
    }),
                                 31122);

    // The schema is allowed to specify the 'array' bsonType with random encryption.
    assert.commandWorked(testDb.runCommand({
        insert: coll.getName(),
        documents: [{_id: 1}],
        jsonSchema: {
            type: "object",
            properties: {
                foo: {
                    encrypt: {
                        algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                        keyId: [UUID(), UUID()],
                        bsonType: ["int", "array"]
                    }
                }
            }
        }
    }));

    const fooRandomEncryptedSchema = {
        type: "object",
        properties: {
            foo: {
                encrypt:
                    {algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random", keyId: [UUID(), UUID()]}
            }
        }
    };

    // Verify that an insert command where 'foo' is an array succeeds when 'foo' is marked for
    // encryption with the random algorithm.
    assert.commandWorked(testDb.runCommand({
        insert: coll.getName(),
        documents: [{_id: 1, foo: [1, 2, 3]}],
        jsonSchema: fooRandomEncryptedSchema
    }));

    assert.commandWorked(testDb.runCommand({
        insert: coll.getName(),
        documents: [{_id: 1, foo: [{bar: 1}, {bar: 2}, {bar: 3}]}],
        jsonSchema: fooRandomEncryptedSchema
    }));

    mongocryptd.stop();
}());
