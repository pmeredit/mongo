/**
 * Test count command over encrypted fields for FLE2.
 * @tags: [
 * requires_fcv_60
 * ]
 */
load('jstests/aggregation/extras/utils.js');  // For assertArrayEq.
load("jstests/fle2/libs/encrypted_client_util.js");
load("src/mongo/db/modules/enterprise/jstests/fle2/query/match_expression_data.js");

(function() {
/**
 *
 * @param {object} testData An object that contains the contents of the test. Namely:
 *                 - insert {object[] | undefined} Array of documents to insert before the test.
 *                 - before {(Collection) => void} Callback to perform any extra assertions or
 *                                                 operations before the test.
 *                 - ssn {string} Encrypted value to query for.
 *                 - expected {object[]} Array of documents that are expected to be returned.
 * @param {Collection} testColl User collection to operate on.
 * @param {object} message Message to display if an assertion fails during the test.
 */
const runTestWithColl = ({insert = [], before = null, query, expected}, testColl, message) => {
    if (before) {
        before(testColl);
    }

    for (const doc of insert) {
        assert.commandWorked(testColl.insert(doc), message);
    }
    const result = testColl.count(query);
    assert.eq(result, expected.length, tojson(message));
};

const {encryptedFields, tests} = matchExpressionFLETestCases;

const collName = jsTestName();

runEncryptedTest(db, "count", collName, encryptedFields, (edb) => {
    print("non-transaction test cases.");
    const coll = edb[collName];

    let i = 0;
    for (const test of tests) {
        runTestWithColl(test, coll, {index: i++, testData: test, transaction: false});
    }
});

// Note: Count command is not supported in multi-document transactions, so only run outside of a
// transaction.
}());