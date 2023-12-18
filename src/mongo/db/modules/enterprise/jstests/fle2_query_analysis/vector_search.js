/**
 * Test interaction between `$vectorSearch` and Queryable Encryption.
 * @tags: [
 *   requires_fcv_71,
 * ]
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
                         generateSchema(schema, unColl.getFullName()));
}

let command, cmdRes;

const vectorSearchQuery = {
    queryVector: [1.0, 2.0, 3.0],
    path: "x",
    numCandidates: 10,
    limit: 5
};

// Test that a $vectorSearch stage on an unencrypted collection is uninhibited.
command =
    buildAggregateUnencrypted([{$vectorSearch: vectorSearchQuery}], {ssn: encryptedStringSpec});
cmdRes = assert.commandWorked(testDB.runCommand(command));
assert.eq(false, cmdRes.hasEncryptionPlaceholders, cmdRes);
assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);

// Test that a $vectorSearch stage passes through query analysis without issue.
command = buildAggregate([{$vectorSearch: vectorSearchQuery}], {ssn: encryptedStringSpec});
cmdRes = assert.commandWorked(testDB.runCommand(command));
assert.eq(false, cmdRes.hasEncryptionPlaceholders, cmdRes);
assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);

// Test that stages after $vectorSearch mark their literals for encryption.
command = buildAggregate([{$vectorSearch: vectorSearchQuery}, {$match: {ssn: '42'}}],
                         {ssn: encryptedStringSpec});
cmdRes = assert.commandWorked(testDB.runCommand(command));
assert.eq(true, cmdRes.hasEncryptionPlaceholders, cmdRes);
assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);

mongocryptd.stop();
