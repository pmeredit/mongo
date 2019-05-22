/**
 * Test that deterministic encryption is banned for the correct set of BSON types.
 */
(function() {
    "use strict";
    load("src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js");

    const mongocryptd = new MongoCryptD();
    mongocryptd.start();

    const conn = mongocryptd.getConnection();
    const testDb = conn.getDB("test");
    const coll = testDb.illegal_types_for_deterministic;

    const kLegalTypes = [
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

    // Some types are illegal for deterministic (and random) encryption because the type itself is
    // the only meaningful value, and the type is not hidden by encryption.
    const kSingleTypeValuedErrCode = 31041;

    // Some types are illegal specifically for the deterministic encryption algorithm because
    // equality semantics of MQL cannot be preserved after encryption.
    const kProhibitedForDeterministicErrCode = 31122;

    const kIllegalTypes = [
        {type: "array", code: kProhibitedForDeterministicErrCode},
        {type: "bool", code: kProhibitedForDeterministicErrCode},
        {type: "decimal", code: kProhibitedForDeterministicErrCode},
        {type: "double", code: kProhibitedForDeterministicErrCode},
        {type: "javascriptWithScope", code: kProhibitedForDeterministicErrCode},
        {type: "maxKey", code: kSingleTypeValuedErrCode},
        {type: "minKey", code: kSingleTypeValuedErrCode},
        {type: "null", code: kSingleTypeValuedErrCode},
        {type: "object", code: kProhibitedForDeterministicErrCode},
        {type: "undefined", code: kSingleTypeValuedErrCode},
    ];

    const schemaTemplate = {
        type: "object",
        properties: {
            foo: {
                encrypt:
                    {algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic", keyId: [UUID()]}
            }
        }
    };

    // Verify that the schema is considered legal for all supported types.
    for (const legalType of kLegalTypes) {
        schemaTemplate.properties.foo.encrypt.bsonType = legalType;
        assert.commandWorked(testDb.runCommand(
            {insert: coll.getName(), documents: [{_id: 1}], jsonSchema: schemaTemplate}));
    }

    // Verify that the schema is prohibited for all unsupported types, even though the insert
    // command does not actually attempt to insert an element with the illegal type inside the
    // deterministically encrypted field.
    for (const illegalType of kIllegalTypes) {
        schemaTemplate.properties.foo.encrypt.bsonType = illegalType.type;
        assert.commandFailedWithCode(
            testDb.runCommand(
                {insert: coll.getName(), documents: [{_id: 1}], jsonSchema: schemaTemplate}),
            illegalType.code);
    }

    mongocryptd.stop();
}());
