/**
 * Test that mongocryptd correctly sets the 'schemaRequiresEncryption' flag for schemas that use the
 * 'additionalProperties' and 'patternProperties' keywords.
 */
(function() {
    "use strict";

    load("src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js");

    const mongocryptd = new MongoCryptD();
    mongocryptd.start();

    const conn = mongocryptd.getConnection();
    const testDb = conn.getDB("test");
    const coll = testDb.schema_requires_encryption;

    const schemaEncryptObj = {
        encrypt: {
            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
            keyId: [UUID(), UUID()],
            bsonType: "int"
        }
    };

    let schema, cmdRes;

    // Verify that 'schemaRequiresEncryption' is set to true when 'encrypt' is beneath
    // 'additionalProperties'.
    schema = {
        type: "object",
        properties: {a: {type: "string"}},
        additionalProperties: schemaEncryptObj
    };
    cmdRes = assert.commandWorked(testDb.runCommand({
        insert: coll.getName(),
        documents: [{_id: NumberInt(1), a: "b"}],
        jsonSchema: schema,
        isRemoteSchema: false
    }));
    assert.eq(true, cmdRes.schemaRequiresEncryption);

    // Verify that 'schemaRequiresEncryption' is set to false when there is no 'encrypt' keyword,
    // but 'additionalProperties' is present.
    schema = {
        type: "object",
        properties: {a: {type: "string"}},
        additionalProperties: {type: "number"}
    };
    cmdRes = assert.commandWorked(testDb.runCommand({
        insert: coll.getName(),
        documents: [{_id: NumberInt(1), a: "b"}],
        jsonSchema: schema,
        isRemoteSchema: false
    }));
    assert.eq(false, cmdRes.schemaRequiresEncryption);

    // Verify that 'schemaRequiresEncryption' is set to true when 'encrypt' is beneath
    // 'patternProperties'.
    schema = {type: "object", patternProperties: {foo: schemaEncryptObj}};
    cmdRes = assert.commandWorked(testDb.runCommand({
        insert: coll.getName(),
        documents: [{_id: NumberInt(1), a: "b"}],
        jsonSchema: schema,
        isRemoteSchema: false
    }));
    assert.eq(true, cmdRes.schemaRequiresEncryption);

    // Verify that 'schemaRequiresEncryption' is false when there is no 'encrypt' keyword, but
    // 'patternProperties' is present.
    schema = {type: "object", patternProperties: {foo: {type: "string"}}};
    cmdRes = assert.commandWorked(testDb.runCommand({
        insert: coll.getName(),
        documents: [{_id: NumberInt(1), a: "b"}],
        jsonSchema: schema,
        isRemoteSchema: false
    }));
    assert.eq(false, cmdRes.schemaRequiresEncryption);

    mongocryptd.stop();
}());
