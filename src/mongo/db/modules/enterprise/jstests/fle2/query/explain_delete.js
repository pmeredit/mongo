/**
 * Test explain for delete with encrypted fields for FLE2.
 * @tags: [
 *   assumes_unsharded_collection,
 *   requires_fcv_70,
 *   requires_fle2_in_always,
 * ]
 */
import {kSafeContentField, runEncryptedTest} from "jstests/fle2/libs/encrypted_client_util.js";
import {
    matchExpressionFLETestCases
} from "src/mongo/db/modules/enterprise/jstests/fle2/query/utils/find_utils.js";

const assertExplainResult = (edb, collName, query, assertions) => {
    const result = assert.commandWorked(edb.runCommand({
        explain: {
            delete: collName,
            deletes: [{q: query, limit: 1}],
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

const {encryptedFields, tests} = matchExpressionFLETestCases;
const collName = jsTestName();

runEncryptedTest(db, "delete_explain", collName, encryptedFields, (edb) => {
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
