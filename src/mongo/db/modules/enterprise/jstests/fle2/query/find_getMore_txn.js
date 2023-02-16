/**
 * Test find and getMore commands in a transaction over encrypted fields for FLE2.
 * @tags: [
 *   assumes_read_concern_unchanged,
 *   assumes_read_preference_unchanged,
 *   requires_fcv_60,
 *   requires_getmore,
 *   uses_transactions,
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

const {encryptedFields, tests} = matchExpressionFLETestCases;

let dbName = "find_transaction";
let collName = jsTestName() + "_transaction";

runEncryptedTest(db, dbName, collName, encryptedFields, (edb, client) => {
    print("transaction test cases.");
    const session = edb.getMongo().startSession({causalConsistency: false});
    const sessionDB = session.getDatabase(dbName);
    const sessionColl = sessionDB.getCollection(collName);

    let i = 0;
    for (const test of tests) {
        const extraInfo = {index: i++, testData: test, transaction: true};
        session.startTransaction();
        runTestWithColl(test, sessionColl, extraInfo, true);
        session.commitTransaction();
    }
    client.assertEncryptedCollectionCounts(collName, 4, 8, 0, 8);
});
}());
