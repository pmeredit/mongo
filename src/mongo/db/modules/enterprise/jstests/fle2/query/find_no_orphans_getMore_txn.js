/**
 * Test find and getMore commands when a document's encrypted field is updated inside a transaction
 * for FLE2.
 * @tags: [
 *   assumes_read_concern_unchanged,
 *   assumes_read_preference_unchanged,
 *   assumes_unsharded_collection,
 *   requires_fcv_60,
 *   requires_getmore,
 *   uses_transactions,
 * ]
 */
load('jstests/aggregation/extras/utils.js');  // For assertArrayEq.
load("jstests/fle2/libs/encrypted_client_util.js");
load("src/mongo/db/modules/enterprise/jstests/fle2/query/utils/find_utils.js");

(function() {
const {encryptedFields, tests, updateTests} = matchExpressionFLETestCases;

let dbName = "find";
let collName = jsTestName();
runEncryptedTest(db, dbName, collName, encryptedFields, (edb, client) => {
    print("non-transaction test cases.");
    const session = edb.getMongo().startSession({causalConsistency: false});
    const sessionDB = session.getDatabase(dbName);
    const sessionColl = sessionDB.getCollection(collName);

    let i = 0;
    for (const test of tests) {
        const extraInfo = {index: i++, testData: test, transaction: false};
        session.startTransaction();
        runTestWithColl(test, sessionColl, extraInfo, true);
        session.commitTransaction();
    }
    for (const test of updateTests) {
        const extraInfo = {index: i++, testData: test, transaction: false};
        session.startTransaction();
        runTestWithColl(test, sessionColl, extraInfo, true);
        session.commitTransaction();
    }
    client.assertEncryptedCollectionCounts(collName, 4, 9, 1, 10);
});
}());
