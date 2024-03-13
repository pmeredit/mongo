/**
 * Test explain for find command over encrypted fields for FLE2.
 * @tags: [
 *   assumes_read_concern_unchanged,
 *   assumes_read_preference_unchanged,
 *   multiversion_incompatible,
 *   requires_fcv_80,
 *   requires_fle2_encrypted_collscan,
 * ]
 */
import {
    isFLE2AlwaysUseCollScanModeEnabled,
    kSafeContentField,
    runEncryptedTest
} from "jstests/fle2/libs/encrypted_client_util.js";

if (!isFLE2AlwaysUseCollScanModeEnabled(db)) {
    jsTest.log(
        "Test skipped because internalQueryFLEAlwaysUseEncryptedCollScanMode is not enabled");
    quit();
}

const collName = jsTestName();
const encryptedFields = {
    "fields": [{
        "path": "age",
        "bsonType": "int",
        "queries": {
            queryType: "range",
            min: NumberInt(0),
            max: NumberInt(255),
            sparsity: 1,
        }
    }]
};

const assertExplainResult = (edb, collName, query, assertions) => {
    const result = assert.commandWorked(edb.runCommand({
        explain: {
            find: collName,
            projection: {[kSafeContentField]: 0},
            filter: query,
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

runEncryptedTest(db, "range_explain", collName, encryptedFields, (edb) => {
    assert.commandWorked(edb[collName].insert({_id: 0, age: NumberInt(25)}));
    assertExplainResult(edb,
                        collName,
                        {"age": {$gte: NumberInt(23), $lte: NumberInt(35)}},
                        (query, result) => { assert(query.$expr.$_internalFleBetween, query); });
});
