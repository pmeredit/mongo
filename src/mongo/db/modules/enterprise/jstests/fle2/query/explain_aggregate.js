/**
 * Test explain with aggregations on encrypted collections.
 *
 * @tags: [
 *   multiversion_incompatible,
 *   requires_fcv_70,
 *   requires_fle2_in_always,
 * ]
 */

import {EncryptedClient} from "jstests/fle2/libs/encrypted_client_util.js";

// Set up the encrypted collection.
const collName = jsTestName();
const dbName = jsTestName();
const dbTest = db.getSiblingDB(dbName);
dbTest.dropDatabase();
let client = new EncryptedClient(db.getMongo(), dbName);
assert.commandWorked(client.createEncryptionCollection(collName, {
    encryptedFields:
        {"fields": [{"path": "ssn", "bsonType": "string", "queries": {"queryType": "equality"}}]}
}));
let edb = client.getDB();

// Explain the pipeline on the provided db and collection, then run the provided asserts with the
// parsed query found in the explain plan.
const assertExplainResult = (edb, collName, pipeline, assertions) => {
    const result = assert.commandWorked(edb.runCommand({
        explain: {aggregate: collName, pipeline: pipeline, cursor: {}},
        verbosity: "queryPlanner"
    }));

    if (result.shards) {
        // Test the assertions on each shard's part of the explain.
        for (const shardName in result.shards) {
            const inputQuery = result.shards[shardName].queryPlanner.parsedQuery;
            assertions(inputQuery, result);
        }
    } else {
        assertions(result.queryPlanner.parsedQuery, result);
    }
};

assert.commandWorked(edb.getCollection(collName).einsert({_id: 0, ssn: "123"}));

client.runEncryptionOperation(() => {
    // When a query is on encrypted fields, the explain contains the rewritten query.
    assertExplainResult(edb, collName, [{$match: {ssn: "123"}}], (query, result) => {
        assert(query.hasOwnProperty("__safeContent__"), result);
        assert(!query.hasOwnProperty("ssn"), result);
    });

    // When a query is not on encrypted fields, the explain contains the original query, since no
    // rewrites should be done.
    assertExplainResult(edb, collName, [{$match: {_id: "456"}}], (query, result) => {
        assert(!query.hasOwnProperty("__safeContent__"), result);
        assert(!query.hasOwnProperty("ssn"), result);
        assert(query.hasOwnProperty("_id"), result);
    });
});
