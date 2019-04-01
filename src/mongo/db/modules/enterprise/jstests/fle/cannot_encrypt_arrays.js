/**
 * Test that mongocryptd errors if users either:
 *  - Issue writes that would store a whole array in an encrypted field.
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
    const coll = testDb.cannot_encrypt_arrays;

    const encryptObj = {
        encrypt: {
            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
            keyId: [UUID(), UUID()],
            initializationVector: BinData(0, "ASNFZ4mrze/ty6mHZUMhAQ==")
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
                                 31005);
    assert.commandFailedWithCode(testDb.runCommand({
        insert: coll.getName(),
        documents: [{_id: 1, foo: [{bar: 1}, {bar: 2}, {bar: 3}]}],
        jsonSchema: fooEncryptedSchema
    }),
                                 31005);

    // Verify that an insert command where 'foo.bar' is an array fails when 'foo.bar' is marked for
    // encryption.
    assert.commandFailedWithCode(testDb.runCommand({
        insert: coll.getName(),
        documents: [{_id: 1, foo: {bar: [1, 2, 3]}}],
        jsonSchema: fooDotBarEncryptedSchema
    }),
                                 31005);
    assert.commandFailedWithCode(testDb.runCommand({
        insert: coll.getName(),
        documents: [{_id: 1, foo: {bar: [{baz: 1}, {baz: 2}, {baz: 3}]}}],
        jsonSchema: fooDotBarEncryptedSchema
    }),
                                 31005);

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
        updates: [{q: {}, u: {$set: {foo: {bar: [1, 2, 3]}}}}],
        jsonSchema: fooDotBarEncryptedSchema
    }),
                                 31005);
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
                                 31005);
    assert.commandFailedWithCode(testDb.runCommand({
        update: coll.getName(),
        updates: [{q: {}, u: {$set: {foo: [{bar: 1}]}}, upsert: true}],
        jsonSchema: fooDotBarEncryptedSchema
    }),
                                 51159);

    // Verify that a replacement style update cannot create an array along an encrypted path.
    assert.commandFailedWithCode(testDb.runCommand({
        update: coll.getName(),
        updates: [{q: {}, u: {foo: {bar: [1, 2, 3]}}}],
        jsonSchema: fooDotBarEncryptedSchema
    }),
                                 31005);
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
                                 31005);
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
                                 31005);
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
                                 31005);
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

    // A document can be inserted successfully if an entire object should be encrypted, and that
    // object contains a nested array.
    assert.commandWorked(testDb.runCommand({
        insert: coll.getName(),
        documents: [{_id: 1, foo: {bar: [1, 2, 3]}}],
        jsonSchema: fooEncryptedSchema
    }));

    // Can evaluate an $eq-to-object predicate on an encrypted field, even if the object in the
    // query includes a nested array.
    assert.commandWorked(testDb.runCommand({
        find: coll.getName(),
        filter: {foo: {$eq: {bar: [1, 2, 3]}}},
        jsonSchema: fooEncryptedSchema
    }));

    mongocryptd.stop();
}());
