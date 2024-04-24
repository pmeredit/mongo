/**
 * Test explain for count command over encrypted fields for FLE2.
 * @tags: [
 *   multiversion_incompatible,
 *   requires_fcv_70,
 *   requires_fle2_in_always,
 * ]
 */
import {kSafeContentField, runEncryptedTest} from "jstests/fle2/libs/encrypted_client_util.js";

const collName = jsTestName();
const encryptedFields = {
    "fields": [{"path": "ssn", "bsonType": "string", "queries": {"queryType": "equality"}}]
};

const assertExplainResult = (edb, collName, query, assertions) => {
    const result = assert.commandWorked(edb.runCommand({
        explain: {
            count: collName,
            query: query,
        },
        verbosity: "queryPlanner"
    }));
    let inputQuery;
    if (result.queryPlanner.winningPlan.shards) {
        inputQuery = result.queryPlanner.winningPlan.shards[0].parsedQuery;
    } else {
        inputQuery = result.queryPlanner.parsedQuery;
    }

    assertions(inputQuery, result);
};

runEncryptedTest(db, "count_explain", collName, encryptedFields, (edb) => {
    assert.commandWorked(edb[collName].insert({_id: 0, ssn: "123"}));
    assertExplainResult(edb, collName, {"ssn": "123"}, (query, result) => {
        assert(query.hasOwnProperty(kSafeContentField), result);
        assert(!query.hasOwnProperty("ssn"), result);
    });
    assertExplainResult(edb, collName, {_id: 0}, (query, result) => {
        assert(!query.hasOwnProperty(kSafeContentField), result);
        assert(!query.hasOwnProperty("ssn"), result);
        assert(query.hasOwnProperty("_id"), result);
    });
});
