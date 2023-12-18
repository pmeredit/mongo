/**
 * Test interaction between `$search` and Queryable Encryption.
 */
import {MongoCryptD} from "src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js";
import {
    generateSchema,
    kDeterministicAlgo
} from "src/mongo/db/modules/enterprise/jstests/fle/lib/utils.js";

// Start mongocryptd.
const mongocryptd = new MongoCryptD();
mongocryptd.start();

const conn = mongocryptd.getConnection();
const testDB = conn.getDB("test");
const coll = testDB.fle_agg;
const unColl = testDB.fle_agg2;

const encryptedStringSpec = {
    encrypt: {algorithm: kDeterministicAlgo, keyId: [UUID()], bsonType: "string"}
};

function buildAggregate(pipeline, schema) {
    return Object.assign({aggregate: coll.getName(), pipeline: pipeline, cursor: {}},
                         generateSchema(schema, coll.getFullName()));
}

function buildAggregateUnencrypted(pipeline, schema) {
    return Object.assign({aggregate: unColl.getName(), pipeline: pipeline, cursor: {}},
                         generateSchema(schema, coll.getFullName()));
}

let command, cmdRes;

// Test that a $search stage on an unencrypted collection is uninhibited.
command = buildAggregateUnencrypted([{$search: {query: "soup", path: "title"}}],
                                    {ssn: encryptedStringSpec});
cmdRes = assert.commandWorked(testDB.runCommand(command));
assert.eq(false, cmdRes.hasEncryptionPlaceholders, cmdRes);
assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);

// Test that a $search stage passes through query analysis without issue.
command = buildAggregate([{$search: {query: "soup", path: "title"}}], {ssn: encryptedStringSpec});
cmdRes = assert.commandWorked(testDB.runCommand(command));
assert.eq(false, cmdRes.hasEncryptionPlaceholders, cmdRes);
assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);

// Test that stages after $search mark their literals for encryption.
command = buildAggregate([{$search: {query: "soup", path: "title"}}, {$match: {ssn: '42'}}],
                         {ssn: encryptedStringSpec});
cmdRes = assert.commandWorked(testDB.runCommand(command));
assert.eq(true, cmdRes.hasEncryptionPlaceholders, cmdRes);
assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);

// Test that a $searchMeta stage passes through query analysis without issue.
command =
    buildAggregate([{$searchMeta: {query: "soup", path: "title"}}], {ssn: encryptedStringSpec});
cmdRes = assert.commandWorked(testDB.runCommand(command));
assert.eq(false, cmdRes.hasEncryptionPlaceholders, cmdRes);
assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);

// Test that a $search stage containing 'returnStoredSource' fails.
command = buildAggregate([{$search: {query: "soup", path: "title", returnStoredSource: true}}],
                         {ssn: encryptedStringSpec});
assert.commandFailed(testDB.runCommand(command));

mongocryptd.stop();
