load('jstests/aggregation/extras/utils.js');  // For assertArrayEq.
load("jstests/fle2/libs/encrypted_client_util.js");

(function() {
if (!isFLE2Enabled()) {
    return;
}

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

const encryptedFields = {
    "fields": [
        {"path": "ssn", "bsonType": "string", "queries": {"queryType": "equality"}},
        {"path": "age", "bsonType": "long", "queries": {"queryType": "equality"}}
    ]
};

// Documents that will be used in the following tests. The ssn and age fields have encrypted
// equality indexes.
const docs = [
    {_id: 0, ssn: "123", age: NumberLong(54)},
    {_id: 1, ssn: "456", age: NumberLong(5)},
    {_id: 2, ssn: "798", age: NumberLong(54)},
    {_id: 3, ssn: "123", age: NumberLong(37)},
];

/**
 * Test cases to run. Note that tests don't clean themselves up, so the documents inserted in one
 * test are available in the next.
 */
const tests = [
    // Insert one document, query for one document.
    {insert: [docs[0]], query: {ssn: docs[0].ssn}, expected: [docs[0]]},

    // Query for non-encrypted field.
    {query: {_id: docs[0]._id}, expected: [docs[0]]},

    // Query for non-existent field value.
    {query: {ssn: "abc"}, expected: []},

    // Insert two documents, query for one document.
    {insert: [docs[1]], query: {ssn: docs[1].ssn}, expected: [docs[1]]},

    // Insert 4 documents, query for 2 documents.
    {insert: [docs[2], docs[3]], query: {ssn: docs[1].ssn}, expected: [docs[1]]},
    {
        before: () => {
            assert.eq(docs[0].ssn, docs[3].ssn);
        },
        query: {ssn: docs[0].ssn},
        expected: [docs[0], docs[3]]
    },

    // Query for two encrypted fields.
    {
        before: () => {
            assert.eq(docs[0].ssn, docs[3].ssn);
        },
        query: {ssn: docs[0].ssn, age: docs[3].age},
        expected: [docs[3]]
    },

    // Query for an encrypted field and unencrypted field.
    {
        before: () => {
            assert.eq(docs[0].ssn, docs[3].ssn);
        },
        query: {ssn: docs[0].ssn, _id: docs[3]._id},
        expected: [docs[3]]
    },

    // $in for two distinct values should return two documents.
    {
        before: () => {
            assert.neq(docs[1].ssn, docs[2].ssn);
        },
        query: {ssn: {$in: [docs[1].ssn, docs[2].ssn]}},
        expected: [docs[1], docs[2]]
    },

    // $or for two encrypted fields.
    {
        before: () => {
            assert.eq(docs[0].ssn, docs[3].ssn);
        },
        query: {$or: [{ssn: docs[0].ssn}, {age: docs[1].age}]},
        expected: [docs[0], docs[1], docs[3]]
    },

    // Update value of document, make sure that query for old value returns 0 documents and query
    // for new value returns 1 document.
    {
        before: (coll) => {
            assert.commandWorked(coll.update({_id: docs[1]._id}, {$set: {ssn: "555"}}));
        },
        query: {ssn: docs[1].ssn},
        expected: []
    },
    {query: {ssn: "555"}, expected: [Object.assign({}, docs[1], {ssn: "555"})]}
];

const collName = jsTestName();

runEncryptedTest(db, "find", collName, encryptedFields, (edb) => {
    print("non-transaction test cases.");
    const coll = edb[collName];

    let i = 0;
    for (const test of tests) {
        runTestWithColl(test, coll, {index: i++, testData: test, transaction: false});
    }
});

runEncryptedTest(db, "find", collName, encryptedFields, (edb) => {
    print("transaction test cases.");
    const session = edb.getMongo().startSession({causalConsistency: false});
    const sessionDB = session.getDatabase(db.getName());
    const sessionColl = sessionDB.getCollection(collName);

    let i = 0;
    for (const test of tests) {
        session.startTransaction();
        runTestWithColl(test, sessionColl, {index: i++, testData: test, transaction: true});
        session.commitTransaction();
    }
});
}());