/**
 * Invalid user input tests for encrypted find range queries.
 *
 * @tags: [
 * requires_fcv_80,
 * requires_fle2_in_always,
 * ]
 */

import {MongoCryptD} from "src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js";

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

function runExpr(expr) {
    return coll.runCommand({
        find: "coll",
        filter: {$expr: expr},
        encryptionInformation: {
            type: 1,
            schema: {
                "test.coll": {fields},
            }
        }
    });
}

const fp = (s) => `$${s}`;

/* -------------------------- Type Tests ---------------------------- */
function runNumericTypeTests(field, invalidTypes) {
    assert.commandFailedWithCode(runExpr({$lte: [fp(field), "hello"]}), 6742000);

    assert.commandFailedWithCode(runExpr({$gte: [fp(field), ISODate("2019-01-30T07:30:10.957Z")]}),
                                 6720002);

    assert.commandFailedWithCode(runExpr({$gte: [fp(field), ISODate("2019-01-30T07:30:10.957Z")]}),
                                 6720002);

    invalidTypes.forEach(type => {
        assert.commandFailedWithCode(runExpr({$gte: [fp(field), type]}), 6742002);
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
    assert.commandFailedWithCode(runExpr({$lte: ["$birthdate", type]}), 6720002);
});

// Verify that range query without an encrypted range index fails.
assert.commandFailedWithCode(runExpr({$lte: ["$zipcode", NumberInt(10)]}), 6331102);

assert.commandFailedWithCode(
    runExpr({$or: [{$lte: ["$zipcode", NumberInt(10)]}, {$ne: ["$zipcode", NumberInt(5)]}]}),
    6331102);

assert.commandFailedWithCode(
    runExpr({$and: [{$lte: ["$zipcode", NumberInt(10)]}, {$ne: ["$zipcode", NumberInt(5)]}]}),
    6331102);

// Verify that $in rejects types not valid for index.
assert.commandFailedWithCode(runExpr({$in: ["$zipcode", [NumberInt(5), "string"]]}), 31118);
assert.commandFailedWithCode(runExpr({$in: ["$age", [NumberDecimal(22), "string"]]}), 6742002);

// Verify unsupported operators errors.
["age", "savings", "birthdate", "debt", "salary"].forEach(field => {
    assert.commandFailedWithCode(runExpr({$add: [fp(field), "$hi"]}), 6331102);
});

/* -------------------------- Bounds Tests ---------------------------- */
// Verify that open range errors when given a value greater than the bounds.
let input = {
    age: NumberInt(256),
    birthdate: ISODate("2022-01-02T07:30:10.957Z"),
    savings: NumberLong(2147483650)
};
for (let field in input) {
    assert.commandFailedWithCode(runExpr({$lte: [fp(field), input[field]]}), 6720810);
}

// Verify that open range errors when given a value less than the bounds.
input = {
    age: NumberInt(-1),
    birthdate: ISODate("1971-01-30T07:30:10.957Z"),
    savings: NumberLong(-200)
};
for (let field in input) {
    assert.commandFailedWithCode(runExpr({$gte: [fp(field), input[field]]}), 6720810);
}

//  Verify that closed range errors when given a number greater than the bounds.
input = {
    age: [NumberInt(230), NumberInt(260)],
    birthdate: [ISODate("2005-08-23T07:30:10.957Z"), ISODate("2023-01-30T07:30:10.957Z")],
    savings: [NumberLong(1000), NumberLong(2147483650)]
};

for (let field in input) {
    assert.commandFailedWithCode(runExpr({
                                     $and: [
                                         {$gte: [fp(field), input[field][0]]},
                                         {$lte: [fp(field), input[field][1]]},
                                     ]
                                 }),
                                 6720811);
}

// Verify that closed range errors when given a number less than the bounds.
input = {
    age: [NumberInt(-1), NumberInt(200)],
    birthdate: [ISODate("1979-12-30T07:30:10.957Z"), ISODate("2016-01-30T07:30:10.957Z")],
    savings: [NumberLong(-1), NumberLong(234023)]
};
for (let field in input) {
    assert.commandFailedWithCode(runExpr({
                                     $and: [
                                         {$gte: [fp(field), input[field][0]]},
                                         {$lte: [fp(field), input[field][1]]},
                                     ]
                                 }),
                                 6720811);
}

/* -------------------------- Coercion Tests ---------------------------- */
// Verify NumberLong parameter errors when greater than MAX_INT on NumberInt range index.
assert.commandFailedWithCode(runExpr({
                                 $and: [
                                     {$gte: ["$age", NumberLong(10)]},
                                     {$lte: ["$age", NumberLong(2147483648)]},
                                 ]
                             }),
                             31108);

// Verify NumberLong parameter errors when less than MIN_INT on NumberInt range index.
assert.commandFailedWithCode(runExpr({
                                 $and: [
                                     {$gte: ["$age", NumberLong(-2147483649)]},
                                     {$lte: ["$age", NumberLong(260)]},
                                 ]
                             }),
                             31108);

/* -------------------------- Regression tests ---------------------------- */
assert.commandWorked(runExpr({
    $and: [
        {$gte: ["$age", NumberInt(22)]},
        {$lt: ["$age", NumberInt(25)]},
        {$in: ["$zipcode", [NumberInt(908765), NumberInt(12345), NumberInt(1257)]]}
    ]
}));
mongocryptd.stop();
