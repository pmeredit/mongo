/**
 * Verify that mongocryptd/mongocsfle accepts the encryptionInformation syntax for describing
 * encrypted fields.
 *
 * @tags: [unsupported_fle_1]
 */
import {MongoCryptD} from "src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js";

const mongocryptd = new MongoCryptD();
mongocryptd.start();
const conn = mongocryptd.getConnection();
const testDb = conn.getDB("db");

// Verify that a valid 'encryptionInformation' is parsed correctly.
let validConfigurations = [
    [{path: "a", keyId: UUID(), bsonType: "string"}],
    [{path: "a", keyId: UUID(), bsonType: "string", queries: {queryType: "equality"}}],
    [{path: "a", keyId: UUID(), bsonType: "string", queries: [{queryType: "equality"}]}],
    [{path: "a.b.c", keyId: UUID(), bsonType: "string"}],
    [
        {path: "a", keyId: UUID(), bsonType: "string"},
        {path: "b", keyId: UUID(), bsonType: "date", queries: {queryType: "equality"}}
    ]
];
for (const efc of validConfigurations) {
    const res = assert.commandWorked(testDb.runCommand({
        insert: "test",
        documents: [{"foo": "bar"}],
        encryptionInformation: {
            type: 1,
            schema: {
                "db.test": {
                    "escCollection": "enxcol_.test.esc",
                    "ecocCollection": "enxcol_.test.ecoc",
                    fields: efc
                }
            }
        }
    }));

    // For FLE 2, 'encryptionInformation' should not be stripped from the command.
    assert(res["result"].hasOwnProperty("encryptionInformation"), tojson(res));
}

// Test that encryptionInformation is mutually exclusive with jsonSchema, but at least one must be
// provided.
assert.commandFailedWithCode(testDb.runCommand({
    insert: "test",
    documents: [{"foo": "bar"}],
    encryptionInformation: {type: 1, schema: {"db.test": {}}},
    jsonSchema: {}
}),
                             6327500);

// Test that encryptionInformation is mutually exclusive with isRemoteSchema.
assert.commandFailedWithCode(testDb.runCommand({
    insert: "test",
    documents: [{"foo": "bar"}],
    encryptionInformation: {type: 1, schema: {"db.test": {}}},
    isRemoteSchema: false
}),
                             6327502);

// Test that 'schema' must be an object.
assert.commandFailedWithCode(testDb.runCommand({
    insert: "test",
    documents: [{"foo": "bar"}],
    encryptionInformation: {type: 1, schema: "secret"},
}),
                             ErrorCodes.TypeMismatch);

// Test that 'schema' must contain exactly one namespace.
assert.commandFailedWithCode(testDb.runCommand({
    insert: "test",
    documents: [{"foo": "bar"}],
    encryptionInformation: {type: 1, schema: {"db.test": {}, "db.test2": {}}},
}),
                             6327503);
assert.commandFailedWithCode(testDb.runCommand({
    insert: "test",
    documents: [{"foo": "bar"}],
    encryptionInformation: {type: 1, schema: {}},
}),
                             6327503);

// Test that 'encryptionInformation' must be an object.
assert.commandFailedWithCode(
    testDb.runCommand(
        {insert: "test", documents: [{"foo": "bar"}], encryptionInformation: "secret"}),
    6327501);

mongocryptd.stop();
