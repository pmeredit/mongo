/**
 * Test find command in a transaction over encrypted fields for FLE2.
 * @tags: [
 *   assumes_read_concern_unchanged,
 *   assumes_read_preference_unchanged,
 *   requires_fcv_70,
 * ]
 */

load('jstests/aggregation/extras/utils.js');  // For assertArrayEq.
load("jstests/fle2/libs/encrypted_client_util.js");
load("src/mongo/db/modules/enterprise/jstests/fle2/query/utils/find_utils.js");

(function() {

const {encryptedFields, tests} = matchExpressionFLETestCases;

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
});
}());
