/**
 * Test find command when a document's encrypted field is updated for FLE2 in a transaction.
 * @tags: [
 *   assumes_read_concern_unchanged,
 *   assumes_read_preference_unchanged,
 *   assumes_unsharded_collection,
 *   requires_fcv_60,
 * ]
 */
load('jstests/aggregation/extras/utils.js');  // For assertArrayEq.
load("jstests/fle2/libs/encrypted_client_util.js");
load("src/mongo/db/modules/enterprise/jstests/fle2/query/utils/find_utils.js");

(function() {
// TODO: SERVER-73995 remove when v2 collscanmode works
if (isFLE2ProtocolVersion2Enabled() && isFLE2AlwaysUseCollScanModeEnabled(db)) {
    jsTest.log("Test skipped because featureFlagFLE2ProtocolVersion2 and " +
               "internalQueryFLEAlwaysUseEncryptedCollScanMode are enabled");
    return;
}

const {encryptedFields, tests, updateTests} = matchExpressionFLETestCases;

let dbName = "find";
let collName = jsTestName();
runEncryptedTest(db, dbName, collName, encryptedFields, (edb, client) => {
    print("non-transaction test cases.");
    const coll = edb[collName];

    let i = 0;
    for (const test of tests) {
        const extraInfo = {index: i++, testData: test, transaction: false};
        runTestWithColl(test, coll, extraInfo);
    }
    client.assertEncryptedCollectionCounts(collName, 4, 8, 0, 8);

    // TODO: SERVER-73303 remove when v2 is enabled by default & update ECOC expected counts
    if (isFLE2ProtocolVersion2Enabled()) {
        client.ecocCountMatchesEscCount = true;
    }

    for (const test of updateTests) {
        const extraInfo = {index: i++, testData: test, transaction: false};
        runTestWithColl(test, coll, extraInfo);
    }
    client.assertEncryptedCollectionCounts(collName, 4, 9, 1, 10);
});
}());
