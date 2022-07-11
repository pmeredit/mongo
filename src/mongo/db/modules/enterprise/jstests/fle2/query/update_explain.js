/**
 * Test explain for delete with encrypted fields for FLE2.
 * @tags: [
 *  requires_fle_in_always,
 * ]
 */
load('jstests/aggregation/extras/utils.js');  // For assertArrayEq.
load("jstests/fle2/libs/encrypted_client_util.js");
load("src/mongo/db/modules/enterprise/jstests/fle2/query/match_expression_data.js");

(function() {
const assertExplainResult = (edb, collName, query, assertions) => {
    const result = assert.commandWorked(edb.runCommand({
        explain: {
            update: collName,
            updates: [{q: query, u: {$set: {modified: true}}}],
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

const {encryptedFields} = matchExpressionFLETestCases;
const collName = jsTestName();

runEncryptedTest(db, "update_explain", collName, encryptedFields, (edb) => {
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
}());
