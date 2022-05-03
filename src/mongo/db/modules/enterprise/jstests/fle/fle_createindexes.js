/**
 * Test to verify that query analysis will fail if an encrypted field is mentioned in a partial
 * filter expression when creating an index.
 * @tags: [requires_fcv_60]
 */
(function() {
'use strict';

load("src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js");
load("src/mongo/db/modules/enterprise/jstests/fle/lib/utils.js");

const mongocryptd = new MongoCryptD();
mongocryptd.start();
const conn = mongocryptd.getConnection();
const testDB = conn.getDB("test");

const ssnUUID = UUID();
const accountUUID = UUID();

const schemaProto = {
    ssn: {encrypt: {algorithm: kDeterministicAlgo, keyId: [ssnUUID], bsonType: "int"}},
    "user.account":
        {encrypt: {algorithm: kDeterministicAlgo, keyId: [accountUUID], bsonType: "string"}}
};
const schema = generateSchema(schemaProto, "test.test");

// Creating an index without a partialFilterExpression works.
assert.commandWorked(testDB.runCommand(
    Object.assign({createIndexes: "test", indexes: [{key: {a: 1}, name: "a"}]}, schema)));

// Creating an index with a partialFilterExpression that references an unencrypted field works.
assert.commandWorked(testDB.runCommand(Object.assign({
    createIndexes: "test",
    indexes: [{key: {a: 1}, name: "a", partialFilterExpression: {unencrypted: "foo"}}]
},
                                                     schema)));

// Creating an index with a filter expression that references an encrypted field fails.
assert.commandFailedWithCode(testDB.runCommand(Object.assign({
    createIndexes: "test",
    indexes: [{key: {a: 1}, name: "a", partialFilterExpression: {ssn: NumberInt(123)}}]
},
                                                             schema)),
                             6491102);

// All indexes are checked for violations.
assert.commandFailedWithCode(testDB.runCommand(Object.assign({
    createIndexes: "test",
    indexes: [
        {key: {a: 1}, name: "a", partialFilterExpression: {unencrypted: "hi"}},
        {key: {b: 1}, name: "b", partialFilterExpression: {ssn: NumberInt(123)}},
    ]
},
                                                             schema)),
                             6491102);
mongocryptd.stop();
}());