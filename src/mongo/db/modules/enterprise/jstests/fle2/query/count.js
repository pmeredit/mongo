/**
 * Test count command over encrypted fields for FLE2.
 * @tags: [
 *   requires_fcv_70,
 * ]
 */
import {runEncryptedTest} from "jstests/fle2/libs/encrypted_client_util.js";
import {
    matchExpressionFLETestCases
} from "src/mongo/db/modules/enterprise/jstests/fle2/query/utils/find_utils.js";

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
const runCountTestWithColl = ({insert = [], before = null, query, expected}, testColl, message) => {
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

runEncryptedTest(db, "count", collName, encryptedFields, (edb, client) => {
    print("non-transaction test cases.");
    const coll = edb.getCollection(collName);

    let i = 0;
    for (const test of tests) {
        runCountTestWithColl(test, coll, {index: i++, testData: test, transaction: false});
    }
    client.assertEncryptedCollectionCounts(collName, 4, 8, 8);
});

// Note: Count command is not supported in multi-document transactions, so only run outside of a
// transaction.