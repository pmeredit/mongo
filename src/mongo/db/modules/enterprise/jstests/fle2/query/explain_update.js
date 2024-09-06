/**
 * Test explain for update with encrypted fields for FLE2.
 * @tags: [
 *   assumes_unsharded_collection,
 *   requires_fcv_81,
 *   requires_fle2_in_always,
 * ]
 */
import {kSafeContentField, runEncryptedTest} from "jstests/fle2/libs/encrypted_client_util.js";
import {isExpress} from "jstests/libs/analyze_plan.js";
import {
    matchExpressionFLETestCases
} from "src/mongo/db/modules/enterprise/jstests/fle2/query/utils/find_utils.js";

const assertExplainResult =
    (edb, collName, query, assertionsOnTopLevelExplain, assertionsOnParsedQuery) => {
        const result = assert.commandWorked(edb.runCommand({
            explain: {
                update: collName,
                updates: [{q: query, u: {$set: {modified: true}}}],
            },
            verbosity: "queryPlanner"
        }));

        assertionsOnTopLevelExplain(edb, result);

        let inputQuery;
        if (result.queryPlanner.winningPlan.shards) {
            inputQuery = result.queryPlanner.winningPlan.shards[0].parsedQuery;
        } else {
            inputQuery = result.queryPlanner.parsedQuery;
        }

        assertionsOnParsedQuery(inputQuery, result);
    };

const {encryptedFields} = matchExpressionFLETestCases;
const collName = jsTestName();

runEncryptedTest(db, "update_explain", collName, encryptedFields, (edb) => {
    assert.commandWorked(edb[collName].insert({_id: 0, ssn: "123"}));
    assertExplainResult(edb, collName, {"ssn": "123"}, (_) => {}, (query, result) => {
        assert(query.hasOwnProperty(kSafeContentField), result);
        assert(!query.hasOwnProperty("ssn"), result);
    });
    assertExplainResult(edb,
                        collName,
                        {_id: 0},
                        (db, explain) => { assert(isExpress(db, explain), tojson(explain)); },
                        (_) => {});
});
