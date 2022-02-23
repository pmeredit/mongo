/**
 * Test that mongocryptd can correctly mark the delete command with intent-to-encrypt placeholders.
 *
 * TODO SERVER-63828: Enable for FLE 2
 * @tags: [unsupported_fle_2]
 */
(function() {
"use strict";

load("src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js");
load("src/mongo/db/modules/enterprise/jstests/fle/lib/utils.js");

const mongocryptd = new MongoCryptD();
mongocryptd.start();

const conn = mongocryptd.getConnection();
const testDb = conn.getDB("test");
const coll = testDb.fle_delete;

function checkEncryptionMarking(deleteCmd, schema, expectedDeletes) {
    Object.assign(deleteCmd, schema);
    const cmdRes = assert.commandWorked(testDb.runCommand(deleteCmd));

    assert.eq(true, cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert(cmdRes.hasOwnProperty("result"), cmdRes);
    assert.eq(coll.getName(), cmdRes.result.delete, cmdRes);
    assert(!cmdRes.result.hasOwnProperty("bypassDocumentValidation"), cmdRes);
    assert(!cmdRes.result.hasOwnProperty("ordered"), cmdRes);
    assert(cmdRes.result.hasOwnProperty("deletes"), cmdRes);
    assert.eq(expectedDeletes.length, cmdRes.result.deletes.length, cmdRes);

    for (let i = 0; i < expectedDeletes.length; i++) {
        let actualDelete = cmdRes.result.deletes[i];
        assert.eq(expectedDeletes[i].limit, actualDelete.limit, cmdRes);
        assert(actualDelete.hasOwnProperty("q"), cmdRes);
        assert(actualDelete.q.hasOwnProperty("$and"), cmdRes);
        assert.eq(expectedDeletes[i].and.length, actualDelete.q.$and.length, cmdRes);

        for (let j = 0; j < expectedDeletes[i].and.length; j++) {
            let expectedPred = expectedDeletes[i].and[j];
            let actualPred = actualDelete.q.$and[j];

            assert(actualPred.hasOwnProperty(expectedPred.path));
            if (expectedPred.encrypted) {
                assert(actualPred[expectedPred.path]["$eq"] instanceof BinData, cmdRes);
            } else {
                assert.eq(expectedPred.eq, actualPred[expectedPred.path]["$eq"], cmdRes);
            }
        }
    }
}

// Verify that a delete command with simple encryption schema is correctly marked for encryption.
let schema = generateSchema({
    foo: {encrypt: {algorithm: kDeterministicAlgo, keyId: [UUID()], bsonType: "long"}},
    bar: {encrypt: {algorithm: kDeterministicAlgo, keyId: [UUID()], bsonType: "long"}},
},
                            coll.getFullName());

let deleteCmd = {
    delete: coll.getName(),
    deletes: [
        {q: {foo: NumberLong(1), baz: 3}, limit: 1},
        {q: {foo: NumberLong(1), bar: NumberLong(1)}, limit: 0},
    ],
    isRemoteSchema: false
};

checkEncryptionMarking(deleteCmd, schema, [
    {limit: 1, and: [{path: "foo", encrypted: true}, {path: "baz", encrypted: false, eq: 3}]},
    {limit: 0, and: [{path: "foo", encrypted: true}, {path: "bar", encrypted: true}]}
]);

// Negative test to make sure that 'hasEncryptionPlaceholders' is set to false when no fields
// are marked for encryption.
deleteCmd = {
    delete: coll.getName(),
    deletes: [
        {q: {w: 1, x: {$in: [1, 2]}}, limit: 1},
        {q: {y: 1, z: {foo: 1}}, limit: 0},
    ],
    isRemoteSchema: false
};
Object.assign(deleteCmd, schema);

let cmdRes = assert.commandWorked(testDb.runCommand(deleteCmd));
assert.eq(false, cmdRes.hasEncryptionPlaceholders, cmdRes);

// Test a delete with an empty 'deletes' array.
deleteCmd = {
    delete: coll.getName(),
    deletes: [],
    isRemoteSchema: false
};
Object.assign(deleteCmd, schema);

cmdRes = assert.commandWorked(testDb.runCommand(deleteCmd));
assert.eq(false, cmdRes.hasEncryptionPlaceholders, cmdRes);
assert.eq([], cmdRes.result.deletes, cmdRes);

// Verify that a delete command with patternProperties is correctly marked for encryption. The use
// of patternProperties is only supported in FLE v1. So, unlike the test cases above, this test case
// always hits FLE 1 behavior only.
schema = {
    jsonSchema: {
        type: "object",
        properties:
            {foo: {encrypt: {algorithm: kDeterministicAlgo, keyId: [UUID()], bsonType: "long"}}},
        patternProperties:
            {bar: {encrypt: {algorithm: kDeterministicAlgo, keyId: [UUID()], bsonType: "long"}}}
    }
};

deleteCmd = {
    delete: coll.getName(),
    deletes: [
        {q: {foo: NumberLong(1), barx: NumberLong(2), baz: 3}, limit: 1},
        {q: {x: 1, foobar: NumberLong(1)}, limit: 0},
    ],
    isRemoteSchema: false
};

checkEncryptionMarking(deleteCmd, schema, [
    {
        limit: 1,
        and: [
            {path: "foo", encrypted: true},
            {path: "barx", encrypted: true},
            {path: "baz", encrypted: false, eq: 3}
        ]
    },
    {limit: 0, and: [{path: "x", encrypted: false, eq: 1}, {path: "foobar", encrypted: true}]}
]);

// Test that a delete query on a field encrypted with the randomized algorithm fails.
const randomSchema = generateSchema({
    foo: {
        encrypt: {
            algorithm: kRandomAlgo,
            keyId: [UUID()],
        }
    }
},
                                    coll.getFullName());
deleteCmd = {
    delete: coll.getName(),
    deletes: [{q: {foo: 1}, limit: 1}],
    isRemoteSchema: false
};
Object.assign(deleteCmd, randomSchema);

assert.commandFailedWithCode(testDb.runCommand(deleteCmd), 51158);

mongocryptd.stop();
}());
