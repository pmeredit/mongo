/**
 * Invalid user input tests for encrypted find range queries.
 *
 * @tags: [
 * featureFlagFLE2Range,
 * requires_fle2_in_always,
 * ]
 */

load("src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js");
load("src/mongo/db/modules/enterprise/jstests/fle/lib/utils.js");

(function() {
'use strict';

const mongocryptd = new MongoCryptD();
mongocryptd.start();
const conn = mongocryptd.getConnection();
const testDB = conn.getDB("test");
const coll = testDB.coll;

const fields = [
    {
        path: "age",
        bsonType: "int",
        keyId: UUID(),
        queries: {queryType: "range", min: NumberInt(0), max: NumberInt(255), sparsity: 1},
    },
    {
        path: "savings",
        bsonType: "long",
        keyId: UUID(),
        queries: {queryType: "range", min: NumberLong(0), max: NumberLong(2147483647), sparsity: 1}
    },
    {
        path: "zipcode",
        bsonType: "int",
        keyId: UUID(),
        "queries": {"queryType": "equality"},
    },
    {
        path: "debt",
        bsonType: "decimal",
        "queries": {"queryType": "range", sparsity: 1},
        keyId: UUID(),
    },
    {
        path: "salary",
        bsonType: "double",
        "queries": {"queryType": "range", sparsity: 1},
        keyId: UUID(),
    },
    {
        path: "birthdate",
        bsonType: "date",
        keyId: UUID(),
        queries: {
            queryType: "range",
            min: ISODate("1980-01-01T07:30:10.957Z"),
            max: ISODate("2022-01-01T07:30:10.957Z"),
            sparsity: 1
        }
    }
];

function runFind(filter) {
    return coll.runCommand({
        find: "coll",
        filter,
        encryptionInformation: {
            type: 1,
            schema: {
                "test.coll": {fields},
            }
        }
    });
}

/* -------------------------- Type Tests ---------------------------- */
function runNumericTypeTests(field, invalidTypes) {
    assert.commandFailedWithCode(runFind({[field]: {$lte: "hello"}}), 6742000);

    assert.commandFailedWithCode(runFind({[field]: {$gte: ISODate("2019-01-30T07:30:10.957Z")}}),
                                 6720002);

    invalidTypes.forEach(type => {
        assert.commandFailedWithCode(runFind({[field]: {$gte: type}}), 6742002);
    });
}
// Verify all types but long and integer errors for long/integer range indexes.
runNumericTypeTests("age", [NumberDecimal(25.6), Number(25.6)]);
runNumericTypeTests("savings", [NumberDecimal(25.6), Number(25.6)]);

// Verify all types but decimal errors for decimal range index.
runNumericTypeTests("debt", [NumberInt(25), NumberLong(-1000), Number(25.6)]);

// Verify all types but double errors for double range index.
runNumericTypeTests("salary", [NumberInt(25), NumberLong(-1000), NumberDecimal(25.6)]);

// Verify that any other type but date errors for date range index.
let invalidTypes =
    [true, NumberDecimal(25.6), NumberLong(100), NumberInt(-12), "hello", Number(104.131)];
invalidTypes.forEach(type => {
    assert.commandFailedWithCode(runFind({birthdate: {$lte: type}}), 6720002);
});

// Verify that range query without an encrypted range index fails.
assert.commandFailedWithCode(runFind({zipcode: {$lte: NumberInt(10)}}), 6721001);

assert.commandFailedWithCode(
    runFind({$or: [{"zipcode": {$lte: NumberInt(10)}}, {"zipcode": {$ne: NumberInt(5)}}]}),
    6721001);

assert.commandFailedWithCode(
    runFind({$and: [{"zipcode": {$lte: NumberInt(10)}}, {"zipcode": {$ne: NumberInt(5)}}]}),
    6720400);

// Verify unsupported operators errors.
["age", "savings", "birthdate", "debt", "salary"].forEach(field => {
    assert.commandFailedWithCode(runFind({[field]: {$type: "date"}}), 51092);
});

/* -------------------------- Bounds Tests ---------------------------- */
// Verify that open range errors when given a value greater than the bounds.
let input = {
    age: NumberInt(256),
    birthdate: ISODate("2022-01-02T07:30:10.957Z"),
    savings: NumberLong(2147483650)
};
for (let field in input) {
    assert.commandFailedWithCode(runFind({[field]: {$lte: input[field]}}), 6747900);
}

// Verify that open range errors when given a value less than the bounds.
input = {
    age: NumberInt(-1),
    birthdate: ISODate("1971-01-30T07:30:10.957Z"),
    savings: NumberLong(-200)
};
for (let field in input) {
    assert.commandFailedWithCode(runFind({[field]: {$gte: input[field]}}), 6747900);
}

//  Verify that closed range errors when given a number greater than the bounds.
input = {
    age: [NumberInt(230), NumberInt(260)],
    birthdate: [ISODate("2005-08-23T07:30:10.957Z"), ISODate("2023-01-30T07:30:10.957Z")],
    savings: [NumberLong(1000), NumberLong(2147483650)]
};

for (let field in input) {
    assert.commandFailedWithCode(runFind({[field]: {$gte: input[field][0], $lte: input[field][1]}}),
                                 6747902);
}

// Verify that closed range errors when given a number less than the bounds.
input = {
    age: [NumberInt(-1), NumberInt(200)],
    birthdate: [ISODate("1979-12-30T07:30:10.957Z"), ISODate("2016-01-30T07:30:10.957Z")],
    savings: [NumberLong(-1), NumberLong(234023)]
};
for (let field in input) {
    assert.commandFailedWithCode(runFind({[field]: {$gte: input[field][0], $lte: input[field][1]}}),
                                 6747901);
}

// Verify that closed range errors when the lower bound is less than the upper bound.
// TODO SERVER-70355. This test should raise an error or return no documents
// input = {
//     age: [NumberInt(30), NumberInt(18)],
//     birthdate:ISODate("2008-01-30T07:30:10.957Z"), [ISODate("1990-12-30T07:30:10.957Z") ],
//     savings:  NumberLong(234023), [NumberLong(100)]
// };
// for testing purposes:
// jsTestLog(runFind({age: {$gt: NumberInt(30), $lt: NumberInt(18)}}))
// for (let field in input) {
//     assert.commandFailedWithCode(
//         runFind({[field]: {$gt: input[field][0], $lt: input[field][1]}})
//     , <error code>);
// }

/* -------------------------- Coercion Tests ---------------------------- */
// Verify NumberLong parameter errors when greater than MAX_INT on NumberInt range index.
assert.commandFailedWithCode(runFind({age: {$gte: NumberLong(10), $lte: NumberLong(2147483648)}}),
                             31108);

// Verify NumberLong parameter errors when less than MIN_INT on NumberInt range index.
assert.commandFailedWithCode(runFind({age: {$gte: NumberLong(-2147483649), $lte: NumberLong(260)}}),
                             31108);
mongocryptd.stop();
}());
