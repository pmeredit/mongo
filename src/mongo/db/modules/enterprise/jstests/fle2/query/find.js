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

let collName = jsTestName();
runEncryptedTest(db, "find", collName, encryptedFields, (edb) => {
    print("non-transaction test cases.");
    const coll = edb[collName];
    coll.drop();

    let i = 0;
    for (const test of tests) {
        const extraInfo = {index: i++, testData: test, transaction: false};
        runTestWithColl(test, coll, extraInfo);

        runTestWithGetMores(test, coll, extraInfo);
    }
});

collName = collName + "_transaction";
runEncryptedTest(db, "find_transaction", collName, encryptedFields, (edb) => {
    print("transaction test cases.");
    const session = edb.getMongo().startSession({causalConsistency: false});
    const sessionDB = session.getDatabase(db.getName());
    const sessionColl = sessionDB.getCollection(collName);
    sessionColl.drop();

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
});
}());
