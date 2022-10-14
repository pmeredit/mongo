/**
 * Invalid user input tests for encrypted find range queries.
 *
 * @tags: [
 * featureFlagFLE2Range,
 * requires_fle2_in_always,
 * ]
 */

load("jstests/fle2/libs/encrypted_client_util.js");
(function() {
'use strict';

// TODO SERVER-67760 remove once feature flag is gone
if (!isFLE2RangeEnabled()) {
    jsTest.log("Test skipped because featureFlagFLE2Range is not enabled");
    return;
}

let dbName = 'find_range_invalid_query';
let dbTest = db.getSiblingDB(dbName);
dbTest.dropDatabase();

let client = new EncryptedClient(db.getMongo(), dbName);
const collName = jsTestName();

jsTest.log("createEncryptionCollection");

assert.commandWorked(client.createEncryptionCollection(collName, {
    encryptedFields: {
        "fields": [
            {
                path: "age",
                bsonType: "int",
                queries: {queryType: "range", min: NumberInt(0), max: NumberInt(255), sparsity: 1}
            },
            {
                path: "savings",
                bsonType: "long",
                queries: {
                    queryType: "range",
                    min: NumberLong(0),
                    max: NumberLong(2147483647),
                    sparsity: 1
                }
            },
            {path: "zipcode", bsonType: "int", "queries": {"queryType": "equality"}},
            {path: "debt", bsonType: "decimal", "queries": {"queryType": "range", sparsity: 1}},
            {path: "salary", bsonType: "double", "queries": {"queryType": "range", sparsity: 1}},
            {
                path: "birthdate",
                bsonType: "date",
                queries: {
                    queryType: "range",
                    min: ISODate("1980-01-01T07:30:10.957Z"),
                    max: ISODate("2022-01-01T07:30:10.957Z"),
                    sparsity: 1
                }
            }
        ]
    }
}));

let edb = client.getDB();
const coll = edb[collName];

const docs = [
    {_id: 0, age: NumberInt(20), birthdate: ISODate("2002-12-30T07:30:10.957Z")},
    {_id: 1, age: NumberInt(45), birthdate: ISODate("1990-09-01T07:30:10.957Z")},
    {_id: 2, age: NumberInt(56), birthdate: ISODate("1980-04-15T07:30:10.957Z")},
    {_id: 3, age: NumberInt(16), birthdate: ISODate("2010-10-30T07:30:10.957Z")}
];

// Bulk inserts aren't supported in FLE2, so insert each one-by-one.
function insert(doc) {
    const res = assert.commandWorked(edb.runCommand({insert: collName, documents: [doc]}));
    assert.eq(res.n, 1);
}

docs.forEach(insert);

/* -------------------------- Type Tests ---------------------------- */
function runNumericTypeTests(field, invalidTypes) {
    assert.throwsWithCode(() => {
        coll.find({[field]: {$lte: "hello"}}).toArray();
    }, 6742000);

    assert.throwsWithCode(() => {
        coll.find({[field]: {$gte: ISODate("2019-01-30T07:30:10.957Z")}}).toArray();
    }, 6720002);

    invalidTypes.forEach(type => {
        assert.throwsWithCode(() => {
            coll.find({[field]: {$gte: type}}).toArray();
        }, 6742002);
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
    assert.throwsWithCode(() => {
        coll.find({birthdate: {$lte: type}}).toArray();
    }, 6720002);
});

// Verify that range query without an encrypted range index fails.
assert.throwsWithCode(() => {
    coll.find({zipcode: {$lte: NumberInt(10)}}).toArray();
}, 6721001);

assert.throwsWithCode(() => {
    coll.find({$or: [{"zipcode": {$lte: NumberInt(10)}}, {"zipcode": {$ne: NumberInt(5)}}]})
        .toArray();
}, 6721001);

assert.throwsWithCode(() => {
    coll.find({$and: [{"zipcode": {$lte: NumberInt(10)}}, {"zipcode": {$ne: NumberInt(5)}}]})
        .toArray();
}, 6720400);

// Verify unsupported operators errors.
let fields = ["age", "savings", "birthdate", "debt", "salary"];
fields.forEach(field => {
    assert.throwsWithCode(() => {
        coll.find({[field]: {$type: "date"}}).toArray();
    }, 51092);
});

/* -------------------------- Bounds Tests ---------------------------- */
// Verify that open range errors when given a value greater than the bounds.
let input = {
    age: NumberInt(256),
    birthdate: ISODate("2022-01-02T07:30:10.957Z"),
    savings: NumberLong(2147483650)
};
for (let field in input) {
    assert.throwsWithCode(() => {
        coll.find({[field]: {$lte: input[field]}}).toArray();
    }, 6747900);
}

// Verify that open range errors when given a value less than the bounds.
input = {
    age: NumberInt(-1),
    birthdate: ISODate("1971-01-30T07:30:10.957Z"),
    savings: NumberLong(-200)
};
for (let field in input) {
    assert.throwsWithCode(() => {
        coll.find({[field]: {$gte: input[field]}}).toArray();
    }, 6747900);
}

//  Verify that closed range errors when given a number greater than the bounds.
input = {
    age: [NumberInt(230), NumberInt(260)],
    birthdate: [ISODate("2005-08-23T07:30:10.957Z"), ISODate("2023-01-30T07:30:10.957Z")],
    savings: [NumberLong(1000), NumberLong(2147483650)]
};

for (let field in input) {
    assert.throwsWithCode(() => {
        coll.find({[field]: {$gte: input[field][0], $lte: input[field][1]}}).toArray();
    }, 6747902);
}

// Verify that closed range errors when given a number less than the bounds.
input = {
    age: [NumberInt(-1), NumberInt(200)],
    birthdate: [ISODate("1979-12-30T07:30:10.957Z"), ISODate("2016-01-30T07:30:10.957Z")],
    savings: [NumberLong(-1), NumberLong(234023)]
};
for (let field in input) {
    assert.throwsWithCode(() => {
        coll.find({[field]: {$gte: input[field][0], $lte: input[field][1]}}).toArray();
    }, 6747901);
}

// Verify that closed range errors when the lower bound is less than the upper bound.
// TODO SERVER-70355. This test should raise an error or return no documents
// input = {
//     age: [NumberInt(30), NumberInt(18)],
//     birthdate:ISODate("2008-01-30T07:30:10.957Z"), [ISODate("1990-12-30T07:30:10.957Z") ],
//     savings:  NumberLong(234023), [NumberLong(100)]
// };
// for testing purposes:
// jsTestLog(coll.find({age: {$gt: NumberInt(30), $lt: NumberInt(18)}}).toArray());
// for (let field in input) {
//     assert.throwsWithCode(() => {
//         coll.find({[field]: {$gt: input[field][0], $lt: input[field][1]}}).toArray();
//     }, <error code>);
// }

/* -------------------------- Coercion Tests ---------------------------- */
// Verify NumberLong parameter errors when greater than MAX_INT on NumberInt range index.
assert.throwsWithCode(() => {
    coll.find({age: {$gte: NumberLong(10), $lte: NumberLong(2147483648)}}).toArray();
}, 31108);

// Verify NumberLong parameter errors when less than MIN_INT on NumberInt range index.
assert.throwsWithCode(() => {
    coll.find({age: {$gte: NumberLong(-2147483649), $lte: NumberLong(260)}}).toArray();
}, 31108);
}());
