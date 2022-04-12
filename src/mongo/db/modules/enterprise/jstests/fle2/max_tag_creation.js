/**
 * Test the FLE rewrite memory limit.
 *
 * @tags: [
 *  requires_fcv_60
 * ]
 */
load("jstests/fle2/libs/encrypted_client_util.js");

(function() {
'use strict';

const dbName = 'max_tag_creation';
const dbTest = db.getSiblingDB(dbName);
const collName = 'basic';
dbTest.dropDatabase();

const client = new EncryptedClient(db.getMongo(), dbName);
// make an encrypted equality index on the "firstName" field
assert.commandWorked(client.createEncryptionCollection(collName, {
    encryptedFields: {
        "fields": [
            {"path": "firstName", "bsonType": "string", "queries": {"queryType": "equality"}},
        ]
    }
}));
const edb = client.getDB();

for (let i = 0; i < 99; i++) {
    assert.commandWorked(edb.basic.insert({num: i, firstName: "Toby"}));
}

// Setting this parameter means encrypted rewrites will generate no more than 99 encrypted tags.
assert.commandWorked(
    edb.adminCommand({setParameter: 1, internalQueryFLERewriteMemoryLimit: 99 * 32}));

const command = {
    find: collName,
    filter: {firstName: "Toby"}
};

// The FLE rewriter will generate 99 tags for this query, so the find command should pass.
assert.commandWorked(edb.basic.runCommand(command));

// Set limit to 98 tags.
assert.commandWorked(
    edb.adminCommand({setParameter: 1, internalQueryFLERewriteMemoryLimit: 98 * 32}));

// Running the same query again should fail because rewriting the filter will require creating more
// tags than the newly set internal limit.
assert.commandFailedWithCode(edb.basic.runCommand(command), 6401800);

// Delete a document
assert.commandWorked(edb.runCommand({delete: "basic", deletes: [{"q": {"num": 50}, limit: 1}]}));

// The FLE rewriter will generate 98 tags for this query, so the find command should pass.
assert.commandWorked(edb.basic.runCommand(command));
}());
