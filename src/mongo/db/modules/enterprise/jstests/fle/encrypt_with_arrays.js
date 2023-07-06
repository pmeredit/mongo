/**
 * Test that mongocryptd errors if users either:
 *  - Issue writes that would store a whole array in an encrypted field that is marked as queryable.
 *  - Issue writes that would put an encrypted field inside of an array.
 *  - Issue reads that imply there can be a whole array stored inside an encrypted field.
 *  - Issue reads that imply encrypted fields can be nested beneath an array.
 */
import {MongoCryptD} from "src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js";
import {generateSchema} from "src/mongo/db/modules/enterprise/jstests/fle/lib/utils.js";
import {
    kDeterministicAlgo,
    kRandomAlgo
} from "src/mongo/db/modules/enterprise/jstests/fle/lib/utils.js";

const mongocryptd = new MongoCryptD();
mongocryptd.start();

const conn = mongocryptd.getConnection();
const testDb = conn.getDB("test");
const coll = testDb.encrypt_with_arrays;

const encryptObj = () =>
    ({encrypt: {algorithm: kDeterministicAlgo, keyId: [UUID(), UUID()], bsonType: "int"}});
const fooEncryptedSchema = {
    foo: encryptObj()
};
const fooDotBarEncryptedSchema = {
    'foo.bar': encryptObj()
};
const fooDotBarDotBazEncryptedSchema = {
    'foo.bar.baz': encryptObj()
};

function assertInsertFails(docs, schema, errCode) {
    assert.commandFailedWithCode(
        testDb.runCommand(Object.assign({insert: coll.getName(), documents: docs},
                                        generateSchema(schema, coll.getFullName()))),
        errCode);
}

// Verify that an insert command where 'foo' is an array fails when 'foo' is marked for
// encryption.
assertInsertFails([{_id: 1, foo: [1, 2, 3]}], fooEncryptedSchema, 31041);

assertInsertFails([{_id: 1, foo: [{bar: 1}, {bar: 2}, {bar: 3}]}], fooEncryptedSchema, 31041);

// Verify that an insert command where 'foo.bar' is an array fails when 'foo.bar' is marked for
// encryption.
assertInsertFails([{_id: 1, foo: {bar: [1, 2, 3]}}], fooDotBarEncryptedSchema, 31041);
assertInsertFails(
    [{_id: 1, foo: {bar: [{baz: 1}, {baz: 2}, {baz: 3}]}}], fooDotBarEncryptedSchema, 31041);

// Verify that an insert command where 'foo' is an array fails when 'foo.bar' is marked for
// encryption.
assertInsertFails([{_id: 1, foo: [{bar: 1}, {bar: 2}, {bar: 3}]}], fooDotBarEncryptedSchema, 31006);

// The insert command should fail when 'foo' is an array and 'foo.bar' is marked for encryption
// even if the path 'foo.bar' does not exist in the document to be inserted.
assertInsertFails([{_id: 1, foo: [{a: 1}, 2, {b: 3}]}], fooDotBarEncryptedSchema, 31006);

// Verify that an insert command where 'foo' is an array fails when 'foo.bar.baz' is marked for
// encryption.
assertInsertFails(
    [{_id: 1, foo: [{bar: {baz: 1}}, {bar: {baz: 2}}]}], fooDotBarDotBazEncryptedSchema, 31006);

// Verify that an insert command where 'foo.bar' is an array fails when 'foo.bar.baz' is marked
// for encryption.
assertInsertFails(
    [{_id: 1, foo: {bar: [{baz: 1}, {baz: 2}]}}], fooDotBarDotBazEncryptedSchema, 31006);

function assertUpdateFails(updates, schema, errCode) {
    assert.commandFailedWithCode(
        testDb.runCommand(Object.assign({update: coll.getName(), updates: updates},
                                        generateSchema(schema, coll.getFullName()))),
        errCode);
}

// Verify that a $set inside an update cannot create an array along an encrypted path.
assertUpdateFails([{q: {}, u: {$set: {foo: [1, 2, 3]}}}], fooEncryptedSchema, 31041);
assertUpdateFails([{q: {}, u: {$set: {foo: {bar: [1, 2, 3]}}}}], fooDotBarEncryptedSchema, 31041);
assertUpdateFails([{q: {}, u: {$set: {foo: [{bar: 1}]}}}], fooDotBarEncryptedSchema, 51159);

// Verify that a $set inside an update cannot create an array along an encrypted path when the
// upsert flag is true.
assertUpdateFails(
    [{q: {}, u: {$set: {foo: {bar: [1, 2, 3]}}}, upsert: true}], fooDotBarEncryptedSchema, 31041);

assertUpdateFails(
    [{q: {}, u: {$set: {foo: [{bar: 1}]}}, upsert: true}], fooDotBarEncryptedSchema, 51159);

// Verify that a replacement style update cannot create an array along an encrypted path.
assertUpdateFails([{q: {}, u: {foo: [1, 2, 3]}}], fooEncryptedSchema, 31041);
assertUpdateFails([{q: {}, u: {foo: {bar: [1, 2, 3]}}}], fooDotBarEncryptedSchema, 31041);
assertUpdateFails([{q: {}, u: {foo: [{bar: 1}]}}], fooDotBarEncryptedSchema, 31006);

// An update command whose query predicate implies the existence of an array along an encrypted
// path should fail.
assertUpdateFails([{q: {foo: {$eq: {bar: {baz: [1, 2, 3]}}}}, u: {$set: {notEncrypted: 1}}}],
                  fooDotBarDotBazEncryptedSchema,
                  31041);
assertUpdateFails([{q: {foo: {$eq: {bar: [{baz: 1}]}}}, u: {$set: {notEncrypted: 1}}}],
                  fooDotBarDotBazEncryptedSchema,
                  31006);

function assertFindCmdFails(filter, schema, errCode) {
    assert.commandFailedWithCode(
        testDb.runCommand(Object.assign({find: coll.getName(), filter: filter},
                                        generateSchema(schema, coll.getFullName()))),
        errCode);
}

// A find command whose predicate implies the existence of an array along an encrypted path
// should fail.
assertFindCmdFails({foo: {$eq: {bar: {baz: [1, 2, 3]}}}}, fooDotBarDotBazEncryptedSchema, 31041);
assertFindCmdFails(
    {foo: {$eq: {bar: [{baz: 1}, {baz: 2}]}}}, fooDotBarDotBazEncryptedSchema, 31006);

function assertDeleteFails(deletes, schema, errCode) {
    assert.commandFailedWithCode(
        testDb.runCommand(Object.assign({delete: coll.getName(), deletes: deletes},
                                        generateSchema(schema, coll.getFullName()))),
        errCode);
}

// A delete command whose predicate implies the existence of an array along an encrypted path
// should fail.
assertDeleteFails(
    [{q: {foo: {$eq: {bar: {baz: [1, 2, 3]}}}}, limit: 1}], fooDotBarDotBazEncryptedSchema, 31041);
assertDeleteFails([{q: {foo: {$eq: {bar: [{baz: 1}, {baz: 2}]}}}, limit: 1}],
                  fooDotBarDotBazEncryptedSchema,
                  31006);

// An equality to an array predicate should result in an error from mongocryptd if the
// predicate's path is the prefix of an encrypted path.
assertFindCmdFails({foo: {$eq: [{bar: 1}, {bar: 2}]}}, fooDotBarEncryptedSchema, 31007);
assertUpdateFails([{q: {foo: {$eq: [{bar: 1}, {bar: 2}]}}, u: {$set: {notEncrypted: 1}}}],
                  fooDotBarEncryptedSchema,
                  31007);

// An $in element that is itself an array should result in an error from mongocryptd if the
// $in predicate's path is the prefix of an encrypted path.
assertFindCmdFails({foo: {$in: [1, [{bar: 2}]]}}, fooDotBarEncryptedSchema, 31008);
assertUpdateFails([{q: {foo: {$in: [1, [{bar: 2}]]}}, u: {$set: {notEncrypted: 1}}}],
                  fooDotBarEncryptedSchema,
                  31008);

// An equality-to-array predicate against an encrypted path should result in an error.
assertFindCmdFails({foo: {$eq: [1, 2, 3]}}, fooEncryptedSchema, 31041);
assertFindCmdFails({foo: {$in: [[1, 2, 3]]}}, fooEncryptedSchema, 31041);

// Deterministic encryption of an object is not legal, since the schema specifies that 'foo'
// must be of type "int".
assertInsertFails([{_id: 1, foo: {bar: [1, 2, 3]}}], fooEncryptedSchema, 31041);

// Can insert an encrypted object, even if that object contains a nested array, when the
// encryption algorithm is random.
// TODO SERVER-61427 remove the optional bsonType.
let fooEncryptedRandomSchema = {
    foo: {encrypt: {algorithm: kRandomAlgo, keyId: [UUID(), UUID()], bsonType: "object"}}
};
assert.commandWorked(testDb.runCommand(
    Object.assign({insert: coll.getName(), documents: [{_id: 1, foo: {bar: [1, 2, 3]}}]},
                  generateSchema(fooEncryptedRandomSchema, coll.getFullName()))));

// Cannot evaluate an $eq-to-object predicate when the object is encrypted with the random
// algorithm.
assertFindCmdFails({foo: {$eq: {bar: [1, 2, 3]}}}, fooEncryptedRandomSchema, [51158, 63165]);

// The schema cannot specify the 'array' bsonType with deterministic encryption. Verify that
// this results in an error for an insert that does not involve the encrypted field.
assertInsertFails(
    [{_id: 1}],
    {foo: {encrypt: {algorithm: kDeterministicAlgo, keyId: [UUID(), UUID()], bsonType: "array"}}},
    [31122, 6338405]);

// The schema is allowed to specify the 'array' bsonType with random encryption.
assert.commandWorked(testDb.runCommand(Object.assign(
    {insert: coll.getName(), documents: [{_id: 1}]},
    generateSchema(
        {foo: {encrypt: {algorithm: kRandomAlgo, keyId: [UUID(), UUID()], bsonType: "array"}}},
        coll.getFullName()))));

// Verify that an insert command where 'foo' is an array succeeds when 'foo' is marked for
// encryption with the random algorithm.
fooEncryptedRandomSchema = generateSchema(
    {foo: {encrypt: {algorithm: kRandomAlgo, keyId: [UUID(), UUID()], bsonType: "array"}}},
    coll.getFullName());
assert.commandWorked(testDb.runCommand(Object.assign(
    {insert: coll.getName(), documents: [{_id: 1, foo: [1, 2, 3]}]}, fooEncryptedRandomSchema)));

assert.commandWorked(testDb.runCommand(Object.assign(
    {insert: coll.getName(), documents: [{_id: 1, foo: [{bar: 1}, {bar: 2}, {bar: 3}]}]},
    fooEncryptedRandomSchema)));

mongocryptd.stop();
