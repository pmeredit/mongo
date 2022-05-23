/**
 * Test to verify that query analysis will fail if an encrypted field is mentioned in a validator
 * for the create and collMod commands.
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

for (const cmd of ["create", "collMod"]) {
    // A validator that references an unencrypted field is acceptable.
    assert.commandWorked(testDB.runCommand(
        Object.assign({[cmd]: "test", validator: {unencrypted: NumberInt(123)}}, schema)));

    // Comparing to an encrypted field isn't allowed.
    assert.commandFailedWithCode(
        testDB.runCommand(Object.assign({[cmd]: "test", validator: {ssn: NumberInt(123)}}, schema)),
        6491100);

    if (!fle2Enabled()) {
        // When creating an encrypted collection with a $jsonSchema validator (as is recommended in
        // FLE 1), the JSON Schema in the validator must be equal to the JSON schema passed directly
        // to query analysis.
        assert.commandWorked(testDB.runCommand(Object.assign(
            {[cmd]: "test", validator: {$jsonSchema: generateSchemaV1(schemaProto).jsonSchema}},
            schema)));

        assert.commandFailedWithCode(testDB.runCommand(Object.assign({
            [cmd]: "test",
            validator: {$jsonSchema: generateSchemaV1(schemaProto).jsonSchema, ssn: "foo"}
        },
                                                                     schema)),
                                     51092);

        // TODO: SERVER-66657 This validator is currently rejected but it could be allowed with more
        // advanced query analysis.
        assert.commandFailedWithCode(testDB.runCommand(Object.assign({
            [cmd]: "test",
            validator: {
                $or: [
                    {$jsonSchema: generateSchemaV1(schemaProto).jsonSchema},
                    {ssn: {$exists: false}},
                ]
            }
        },
                                                                     schema)),
                                     51092);

        // TODO: SERVER-66657 This validator is currently rejected but it could be allowed with more
        // advanced query analysis.
        assert.commandFailedWithCode(testDB.runCommand(Object.assign({
            [cmd]: "test",
            validator: {
                $and: [
                    {$jsonSchema: generateSchemaV1(schemaProto).jsonSchema},
                    {ssn: {$exists: true}},
                ]
            }
        },
                                                                     schema)),
                                     51092);

        // This validator only references the ssn field and no user.account field. Since the schemas
        // don't match, creating a collection with this validator should fail.
        assert.commandFailedWithCode(testDB.runCommand(Object.assign({
            [cmd]: "test",
            validator: {
                $jsonSchema: {
                    type: "object",
                    properties: {
                        ssn: {
                            encrypt: {
                                algorithm: kDeterministicAlgo,
                                keyId: [ssnUUID],
                                bsonType: "int",
                            },
                        },
                    }
                }
            }
        },
                                                                     schema)),
                                     6491101);
    } else {
        // In FLE2, both commands should fail because $jsonSchema is unsupported in query analysis.
        assert.commandFailedWithCode(
            testDB.runCommand(Object.assign(
                {[cmd]: "test", validator: {$jsonSchema: generateSchemaV1(schemaProto).jsonSchema}},
                schema)),
            51092);
        assert.commandFailedWithCode(testDB.runCommand(Object.assign({
            [cmd]: "test",
            validator: {
                $jsonSchema: {
                    type: "object",
                    properties: {
                        ssn: {
                            encrypt: {
                                algorithm: kDeterministicAlgo,
                                keyId: [ssnUUID],
                                bsonType: "int",
                            },
                        },
                    }
                }
            }
        },
                                                                     schema)),
                                     51092);
    }
}

if (!fle2Enabled()) {
    const extendedSchema = Object.extend({}, schema.jsonSchema, true);
    extendedSchema.properties.otherSsn = {
        encrypt: {
            algorithm: kDeterministicAlgo,
            keyId: [UUID()],
            bsonType: "int",
        },
    };

    assert.commandFailedWithCode(
        testDB.runCommand(
            Object.assign({collMod: "test", validator: {$jsonSchema: extendedSchema}}, schema)),
        51088);
}

mongocryptd.stop();
}());