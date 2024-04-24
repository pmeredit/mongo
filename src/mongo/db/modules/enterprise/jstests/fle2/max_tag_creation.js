/**
 * Test the FLE rewrite memory limit.
 *
 * @tags: [
 *  requires_non_retryable_commands,
 *  assumes_unsharded_collection,
 *  requires_fcv_70
 * ]
 */
import {EncryptedClient} from "jstests/fle2/libs/encrypted_client_util.js";

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
    assert.commandWorked(edb.basic.einsert({num: i, firstName: "Toby"}));
}

// Setting this parameter means encrypted rewrites will generate no more than 99 encrypted tags.
assert.commandWorked(
    edb.adminCommand({setParameter: 1, internalQueryFLERewriteMemoryLimit: 10 * 40 + 89 * 41}));

const command = {
    update: collName,
    updates: [{
        q: {firstName: "Toby"},
        u: {$set: {lastName: "Parker"}},
        upsert: true,  // Use upserts because low selectivity mode will not kick in
    }]
};

// The FLE rewriter will generate 99 tags for this query, so the update command should pass.
assert.commandWorked(edb.basic.erunCommand(command));

// Set limit to 98 tags.
assert.commandWorked(
    edb.adminCommand({setParameter: 1, internalQueryFLERewriteMemoryLimit: 10 * 40 + 88 * 41}));

// Running the same query again should fail because rewriting the filter will require creating more
// tags than the newly set internal limit.
assert.commandFailedWithCode(edb.basic.erunCommand(command), ErrorCodes.FLEMaxTagLimitExceeded);

// Delete a document
assert.commandWorked(edb.erunCommand({delete: "basic", deletes: [{"q": {"num": 50}, limit: 1}]}));

// Even after deleting one value the rewriter will generate 99 tags for this query
assert.commandFailedWithCode(edb.basic.erunCommand(command), ErrorCodes.FLEMaxTagLimitExceeded);
