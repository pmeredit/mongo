/**
 * Test that mongocryptd correctly understands the 'isRemoteSchema' flag. The test will verify that
 *   - when 'isRemoteSchema' is true, mongocryptd allows schema keywords which are only enforced on
 *     mongod.
 *   - when 'isRemoteSchema' is false, prohibits schema keywords that are only
 *     enforced on mongod, and take no effect on mongocryptd.
 *
 * @tags: [unsupported_fle_2]
 */
import {MongoCryptD} from "src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js";
import {kDeterministicAlgo} from "src/mongo/db/modules/enterprise/jstests/fle/lib/utils.js";

const mongocryptd = new MongoCryptD();
mongocryptd.start();

const conn = mongocryptd.getConnection();
const testDb = conn.getDB("test");
const coll = testDb.is_remote_schema;

const schemaWithCryptdUnsupportedKeywords = {
    type: "object",
    properties: {a: {type: "string", minLength: 1}},
    additionalProperties:
        {encrypt: {algorithm: kDeterministicAlgo, keyId: [UUID()], bsonType: "long"}},
    maxProperties: 4
};
const schemaWithCryptdSupportedKeywords = {
    type: "object",
    properties: {a: {type: "string"}},
    additionalProperties: {type: "number"}
};

function testIsRemoteSchemaFlag(command) {
    // The aggregate command has a different parsing code path with its own error codes.
    // We can't check the value of the feature flag featureFlagLookupEncryptionSchemasFLE within
    // this test execution, because MongoCryptd does not support the "getParameter" command. As a
    // workaround, we provide a list of expected errors to accommodate for both the legacy
    // parse code for agg and the new parse code path. Once we the flag is enabled by default, we
    // can update these tests to only accept the new error code for an agg command.
    const isAggCommand = command.hasOwnProperty("aggregate");

    command.jsonSchema = schemaWithCryptdUnsupportedKeywords;

    // Verify that the command fails if 'isRemoteSchema' is of type other than boolean.
    command.isRemoteSchema = 1;
    assert.commandFailedWithCode(testDb.runCommand(command), 31102);
    command.isRemoteSchema = {};
    assert.commandFailedWithCode(testDb.runCommand(command), 31102);

    // Verify that when 'isRemoteSchema' is true, we allow schema keywords (like maxProperties,
    // minLength) that are only enforced on mongod.
    command.isRemoteSchema = true;
    let cmdRes = assert.commandWorked(testDb.runCommand(command));
    assert.eq(true, cmdRes.schemaRequiresEncryption);

    // Ensure that the 'isRemoteSchema' flag is not returned back.
    assert.eq(false, cmdRes.hasOwnProperty("isRemoteSchema"));

    // Verify that when 'isRemoteSchema' is false, we disallow schema keywords that are only
    // enforced on mongod.
    command.isRemoteSchema = false;
    assert.commandFailedWithCode(testDb.runCommand(command), 31068);

    // Verify that the 'isRemoteSchema' field is required.
    delete command.isRemoteSchema;
    assert.commandFailedWithCode(testDb.runCommand(command), isAggCommand ? 9686706 : 31104);

    // Verify that when 'isRemoteSchema' is true, we don't return encryption placeholders when
    // schema doesn't have encrypt fields.
    command.jsonSchema = schemaWithCryptdSupportedKeywords;
    command.isRemoteSchema = true;
    cmdRes = assert.commandWorked(testDb.runCommand(command));
    assert.eq(false, cmdRes.schemaRequiresEncryption);

    // Ensure that the 'isRemoteSchema' flag is not returned back.
    assert.eq(false, cmdRes.hasOwnProperty("isRemoteSchema"));
}

// Validate for all the supported commands.
testIsRemoteSchemaFlag({insert: coll.getName(), documents: [{_id: NumberLong(1), a: "b"}]});
testIsRemoteSchemaFlag(
    {update: coll.getName(), updates: [{q: {}, u: {"$set": {foo: NumberLong(5)}}}]});
testIsRemoteSchemaFlag({delete: coll.getName(), deletes: [{q: {foo: NumberLong(1)}, limit: 1}]});
testIsRemoteSchemaFlag({find: coll.getName(), filter: {ssn: NumberLong(5)}});
testIsRemoteSchemaFlag(
    {distinct: coll.getName(), key: "someField", query: {ssn: {$eq: NumberLong(5)}}});
testIsRemoteSchemaFlag({count: coll.getName(), query: {ssn: {$eq: NumberLong(5)}}});
testIsRemoteSchemaFlag({explain: {count: coll.getName(), query: {ssn: {$eq: NumberLong(5)}}}});
testIsRemoteSchemaFlag(
    {explain: {insert: coll.getName(), documents: [{_id: NumberLong(1), a: "b"}]}});
testIsRemoteSchemaFlag(
    {aggregate: coll.getName(), pipeline: [{$match: {ssn: {$eq: NumberLong(5)}}}], cursor: {}});

// Verify that the command fails if 'isRemoteSchema' is used in the inner object of explain.
assert.commandFailedWithCode(testDb.runCommand({
    explain: {count: coll.getName(), query: {ssn: {$eq: NumberLong(5)}}, isRemoteSchema: false}
}),
                             31103);
mongocryptd.stop();
