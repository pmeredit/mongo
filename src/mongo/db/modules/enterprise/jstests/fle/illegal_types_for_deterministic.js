/**
 * Test the behavior of declaring queryable encrypted fields with various BSON types.
 */
(function() {
"use strict";
load("src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js");
load("src/mongo/db/modules/enterprise/jstests/fle/lib/utils.js");

const mongocryptd = new MongoCryptD();
mongocryptd.start();

const conn = mongocryptd.getConnection();
const testDb = conn.getDB("test");
const coll = testDb[jsTestName()];

const kLegalQueryableTypes = [
    "binData",
    "date",
    "dbPointer",
    "int",
    "javascript",
    "long",
    "objectId",
    "regex",
    "string",
    "symbol",
    "timestamp",
];

// Some types are illegal for encryption because the type itself is
// the only meaningful value, and the type is not hidden by encryption.
const kSingleTypeValuedErrCode = fle2Enabled() ? 6316404 : 31122;

// Some types are illegal specifically for the queryable encryption because
// equality semantics of MQL cannot be preserved after encryption.
const kProhibitedForDeterministicErrCode = fle2Enabled() ? 6316404 : 31122;

// It is illegal to encrypt data that is already encrypted.
const kEncryptedBinData = 31041;

let kIllegalTypes = [
    {type: "array", docs: [{_id: 1}], code: kProhibitedForDeterministicErrCode},
    {type: "decimal", docs: [{_id: 1}], code: kProhibitedForDeterministicErrCode},
    {type: "double", docs: [{_id: 1}], code: kProhibitedForDeterministicErrCode},
    {type: "javascriptWithScope", docs: [{_id: 1}], code: kProhibitedForDeterministicErrCode},
    {type: "maxKey", docs: [{_id: 1}], code: kSingleTypeValuedErrCode},
    {type: "minKey", docs: [{_id: 1}], code: kSingleTypeValuedErrCode},
    {type: "null", docs: [{_id: 1}], code: kSingleTypeValuedErrCode},
    {type: "object", docs: [{_id: 1}], code: kProhibitedForDeterministicErrCode},
    {type: "undefined", docs: [{_id: 1}], code: kSingleTypeValuedErrCode},
    {type: "binData", docs: [{foo: BinData(6, "data")}], code: kEncryptedBinData},
];

// In FLE 2, encrypting 'bool' is allowed regardless of whether it's queryable.
if (!fle2Enabled()) {
    kIllegalTypes.push({type: "bool", code: kProhibitedForDeterministicErrCode});
}

const schemaTemplate = {
    foo: {encrypt: {algorithm: kDeterministicAlgo, keyId: [UUID()]}}
};

// Verify that the schema is considered legal for all supported types.
for (const legalType of kLegalQueryableTypes) {
    schemaTemplate.foo.encrypt.bsonType = legalType;
    assert.commandWorked(
        testDb.runCommand(Object.assign({
            insert: coll.getName(),
            documents: [{_id: 1}],
        },
                                        generateSchema(schemaTemplate, coll.getFullName()))));
}

// Verify that the schema is prohibited for all unsupported types, even though the insert
// command does not actually attempt to insert an element with the illegal type inside the
// encrypted field.
for (const illegalType of kIllegalTypes) {
    schemaTemplate.foo.encrypt.bsonType = illegalType.type;
    assert.commandFailedWithCode(
        testDb.runCommand(Object.assign({
            insert: coll.getName(),
            documents: illegalType.docs,
        },
                                        generateSchema(schemaTemplate, coll.getFullName()))),
        illegalType.code);
}

mongocryptd.stop();
}());
