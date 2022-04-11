/**
 * Test find command over encrypted fields for FLE2.
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
    const result = testColl.find(query, {[kSafeContentField]: 0}).toArray();

    assertArrayEq({actual: result, expected, extraErrorMsg: tojson(message)});
};

// To be run immediately after runTestWithColl. Does not preform the 'before' or 'insert' actions;
// only runs the provided query and asserts on the expected documents.
const runTestWithGetMores = ({query, expected}, testColl, message) => {
    const extraErrorMsg = Object.assign({}, message, {withGetMores: true});
    const result = testColl.find(query, {[kSafeContentField]: 0}).batchSize(1).toArray();
    assertArrayEq({actual: result, expected, extraErrorMsg: tojson(extraErrorMsg)});
};

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

        runTestWithGetMores(test, coll, extraInfo);
    }
    client.assertEncryptedCollectionCounts(collName, 4, 9, 1, 10);
});

dbName = dbName + "_transaction";
collName = collName + "_transaction";
runEncryptedTest(db, dbName, collName, encryptedFields, (edb, client) => {
    print("transaction test cases.");
    const session = edb.getMongo().startSession({causalConsistency: false});
    const sessionDB = session.getDatabase(dbName);
    const sessionColl = sessionDB.getCollection(collName);

    let i = 0;
    for (const test of tests) {
        const extraInfo = {index: i++, testData: test, transaction: true};
        session.startTransaction();
        runTestWithColl(test, sessionColl, extraInfo);
        session.commitTransaction();

        session.startTransaction();
        runTestWithGetMores(test, sessionColl, extraInfo);
        session.commitTransaction();
    }
    client.assertEncryptedCollectionCounts(collName, 4, 9, 1, 10);
});
}());
