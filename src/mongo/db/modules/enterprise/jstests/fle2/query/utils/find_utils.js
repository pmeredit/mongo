// Test data for server rewrites on a MatchExpression.
// Documents that will be used in the following tests. The ssn and age fields have encrypted
// equality indexes.
import {assertArrayEq} from "jstests/aggregation/extras/utils.js";
import {kSafeContentField} from "jstests/fle2/libs/encrypted_client_util.js";

export const _docs = [
    {_id: 0, ssn: "123", age: NumberLong(54)},
    {_id: 1, ssn: "456", age: NumberLong(5)},
    {_id: 2, ssn: "798", age: NumberLong(54)},
    {_id: 3, ssn: "123", age: NumberLong(37)},
];

export const matchExpressionFLETestCases = {
    encryptedFields: {
        "fields": [
            {"path": "ssn", "bsonType": "string", "queries": {"queryType": "equality"}},
            {"path": "age", "bsonType": "long", "queries": {"queryType": "equality"}}
        ]
    },

    /**
     * Test cases to run. Note that tests don't clean themselves up, so the documents inserted
     * in one test are available in the next.
     */
    tests: [
        // Insert one document, query for one document.
        {insert: [_docs[0]], query: {ssn: _docs[0].ssn}, expected: [_docs[0]]},

        // Query for non-encrypted field.
        {query: {_id: _docs[0]._id}, expected: [_docs[0]]},

        // Query for non-existent field value.
        {query: {ssn: "abc"}, expected: []},

        // Insert two documents, query for one document.
        {insert: [_docs[1]], query: {ssn: _docs[1].ssn}, expected: [_docs[1]]},

        // Insert 4 documents, query for 2 documents.
        {insert: [_docs[2], _docs[3]], query: {ssn: _docs[1].ssn}, expected: [_docs[1]]},
        {
            before: () => { assert.eq(_docs[0].ssn, _docs[3].ssn);},
            query: {ssn: _docs[0].ssn},
            expected: [_docs[0], _docs[3]]
        },

        // Query for two encrypted fields.
        {
            before: () => { assert.eq(_docs[0].ssn, _docs[3].ssn);},
            query: {ssn: _docs[0].ssn, age: _docs[3].age},
            expected: [_docs[3]]
        },

        // Query for an encrypted field and unencrypted field.
        {
            before: () => { assert.eq(_docs[0].ssn, _docs[3].ssn);},
            query: {ssn: _docs[0].ssn, _id: _docs[3]._id},
            expected: [_docs[3]]
        },

        // $in for two distinct values should return two documents.
        {
            before: () => { assert.neq(_docs[1].ssn, _docs[2].ssn);},
            query: {ssn: {$in: [_docs[1].ssn, _docs[2].ssn]}},
            expected: [_docs[1], _docs[2]]
        },

        // $or for two encrypted fields.
        {
            before: () => { assert.eq(_docs[0].ssn, _docs[3].ssn);},
            query: {$or: [{ssn: _docs[0].ssn}, {age: _docs[1].age}]},
            expected: [_docs[0], _docs[1], _docs[3]]
        },

    ],
    // These tests must be run after the before() conditions have been run in all the above tests.
    updateTests: [
        // Update value of document, make sure that query for old value returns 0 documents and
        // query for new value returns 1 document.
        {
            before: (coll) => {
                assert.commandWorked(coll.update({_id: _docs[1]._id}, {$set: {ssn: "555"}}));
            },
            query: {ssn: _docs[1].ssn},
            expected: [],
            doesUpdate: true,
        },
        {query: {ssn: "555"}, expected: [Object.assign({}, _docs[1], {ssn: "555"})]}
    ]
};

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
export const runTestWithColl =
    ({insert = [], before = null, query, expected}, testColl, message, useGetMore) => {
        if (before) {
            before(testColl);
        }

        for (const doc of insert) {
            assert.commandWorked(testColl.insert(doc), message);
        }

        const result = useGetMore
            ? testColl.find(query, {[kSafeContentField]: 0}).batchSize(1).toArray()
            : testColl.find(query, {[kSafeContentField]: 0}).toArray();

        assertArrayEq({actual: result, expected, extraErrorMsg: tojson(message)});
    };
