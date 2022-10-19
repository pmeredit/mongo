/**
 * Tests for encrypted find queries.
 *
 * @tags: [
 * featureFlagFLE2Range,
 * ]
 */
load("jstests/fle2/libs/encrypted_client_util.js");
load('jstests/aggregation/extras/utils.js');  // For assertArrayEq.
(function() {
'use strict';

// TODO SERVER-67760 remove once feature flag is gone
if (!isFLE2RangeEnabled()) {
    jsTest.log("Test skipped because featureFlagFLE2Range is not enabled");
    return;
}

let dbName = 'basic_insert_range';
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
            {path: "zipcode", bsonType: "string", "queries": {"queryType": "equality"}},
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
    {
        _id: 0,
        age: NumberInt(25),
        savings: NumberLong(1230500),
        debt: NumberDecimal(0.0),
        salary: Number(16000.00),
        birthdate: ISODate("1999-09-15T07:30:10.957Z"),
        zipcode: "123456789",
        karma: 500
    },
    {
        _id: 1,
        age: NumberInt(23),
        savings: NumberLong(8540),
        debt: NumberDecimal(100.43),
        salary: Number(5000.00),
        birthdate: ISODate("2004-12-04T10:45:10.957Z"),
        zipcode: "ABCDE",
        karma: 20
    },
    {
        _id: 2,
        age: NumberInt(38),
        savings: NumberLong(4126000),
        debt: NumberDecimal(10321.69),
        salary: Number(160040.22),
        birthdate: ISODate("1992-07-30T10:45:10.957Z"),
        zipcode: "123456789",
        karma: 40
    },
    {
        _id: 3,
        age: NumberInt(22),
        savings: NumberLong(400),
        debt: NumberDecimal(1250.69),
        salary: Number(10540),
        birthdate: ISODate("2000-07-30T10:45:10.957Z"),
        zipcode: "098765",
        karma: 250
    },
];

// Bulk inserts aren't supported in FLE2, so insert each one-by-one.
function insert(doc) {
    const res = assert.commandWorked(edb.runCommand({insert: collName, documents: [doc]}));
    assert.eq(res.n, 1);
}
docs.forEach(insert);

function assertQueryResults(q, expected) {
    const res = coll.find(q).toArray();
    assert.eq(res.length, expected.length, tojson(q));
    assertArrayEq({actual: res.map(d => d._id), expected, extraErrorMsg: tojson({q, res})});
}

let res = coll.find({}).toArray();
assert.eq(res.length, 4);

/* ---------------------- Open and Closed Ranges: ----------------------------------------------- */
// NumberInt
assertQueryResults({age: {$gte: NumberInt(24)}}, [0, 2]);
assertQueryResults({age: {$gt: NumberInt(39)}}, []);
assertQueryResults({age: {$not: {$gt: NumberInt(39)}}}, [0, 1, 2, 3]);
assertQueryResults({age: {$not: {$gt: NumberInt(24)}}}, [1, 3]);
assertQueryResults({age: {$gte: NumberInt(23), $lte: NumberInt(38)}}, [0, 1, 2]);
assertQueryResults({age: {$eq: NumberInt(38)}}, [2]);  // Answering equality query with range index.
assertQueryResults({age: {$ne: NumberInt(38)}},
                   [0, 1, 3]);  // Answering equality query with range index.
// TODO SERVER-70368 test should pass when $in is added.
// assertQueryResults({age: {$in: [NumberInt(38), NumberInt(22)]}}
//                    [2, 3]);  // Answering equality query with range index.

// NumberLong
assertQueryResults({savings: {$lte: NumberLong(10000)}}, [1, 3]);
assertQueryResults({savings: {$gt: NumberLong(2147483600)}}, []);
assertQueryResults({savings: {$gt: NumberLong(10000), $lt: NumberLong(2000000)}}, [0]);
assertQueryResults({savings: {$not: {$gt: NumberLong(10000), $lt: NumberLong(2000000)}}},
                   [1, 2, 3]);
assertQueryResults({savings: {$eq: NumberLong(4126000)}},
                   [2]);  // Answering equality query with range index.
assertQueryResults({savings: {$ne: NumberLong(4126000)}},
                   [0, 1, 3]);  // Answering equality query with range index.
// TODO SERVER-70368 test should pass when $in is added.
// assertQueryResults({savings: {$in: [NumberLong(1230500), NumberLong(8540), NumberLong(1203)]}}
//                    [0, 1]);  // Answering equality query with range index.

// Double
assertQueryResults({salary: {$gte: Number(10000.00)}}, [0, 2, 3]);
assertQueryResults({salary: {$lt: Number(1000.00)}}, []);
assertQueryResults({salary: {$not: {$gte: Number(10000.00)}}}, [1]);
assertQueryResults({salary: {$gt: Number(10000), $lt: Number(160040.22)}}, [0, 3]);
assertQueryResults({salary: {$ne: Number(10540)}},
                   [0, 1, 2]);  // Answering equality query with range index.
assertQueryResults({salary: {$eq: Number(160040.22)}},
                   [2]);  // Answering equality query with range index.
// TODO SERVER-70368 test should pass when $in is added.
// assertQueryResults({salary: {$in: [Number(160040.22), Number(16000.00)]}}, [0, 2]);

// Decimal128
assertQueryResults({debt: {$lt: NumberDecimal(100.44)}}, [0, 1]);
assertQueryResults({debt: {$gte: NumberDecimal(20000.44)}}, []);
assertQueryResults({debt: {$not: {$lt: NumberDecimal(100.44)}}}, [2, 3]);
assertQueryResults({debt: {$lt: NumberDecimal(5000), $gte: NumberDecimal(1250.69)}}, [3]);
assertQueryResults({debt: {$eq: NumberDecimal(0.0)}},
                   [0]);  // Answering equality query with range index.
assertQueryResults({debt: {$ne: NumberDecimal(0.0)}},
                   [1, 2, 3]);  // Answering equality query with range index.
// TODO SERVER-70368 test should pass when $in is added.
// assertQueryResults({debt: {$in: [NumberDecimal(100.43), Number(10321.69)]}}, [1, 2]);

// Date
assertQueryResults({birthdate: {$gt: ISODate("2000-01-01T10:45:10.957Z")}}, [1, 3]);
assertQueryResults({birthdate: {$lt: ISODate("2002-12-04T10:45:10.957Z")}}, [0, 2, 3]);
// TODO SERVER-70316. Fix minimum bound set on date so this test will pass.
// assertQueryResults({birthdate: {$not: {$lt: ISODate("2002-12-04T10:45:10.957Z")}}}, [1]);
assertQueryResults({
    birthdate:
        {$lte: ISODate("2000-01-01T10:45:10.957Z"), $gte: ISODate("1993-05-16T10:45:10.957Z")}
},
                   [0]);
assertQueryResults({
    birthdate: {
        $not: {$lt: ISODate("2000-01-01T10:45:10.957Z"), $gte: ISODate("1993-05-16T10:45:10.957Z")}
    }
},
                   [1, 2, 3]);
assertQueryResults({birthdate: {$eq: ISODate("1992-07-30T10:45:10.957Z")}},
                   [2]);  // Answering equality query with range index.
assertQueryResults({birthdate: {$ne: ISODate("1992-07-30T10:45:10.957Z")}},
                   [0, 1, 3]);  // Answering equality query with range index.
// TODO SERVER-70368 test should pass when $in is added.
// assertQueryResults({salary: {$in: [Number(160040.22), Number(16000.00)]}}, [0, 2]);

/* ---------------------- Range Conjunction: closed range --------------------------------------- */
assertQueryResults({
    $and: [
        {
            birthdate:
                {$gt: ISODate("1995-09-15T07:30:10.957Z"), $lt: ISODate("2005-12-04T10:45:10.957Z")}
        },
        {savings: {$gt: NumberLong(800), $lte: NumberLong(1230500)}},
    ]
},
                   [0, 1]);
assertQueryResults({
    $or: [
        {birthdate: {$gt: ISODate("1995-09-15T07:30:10.957Z")}},
        {birthdate: {$lt: ISODate("2005-12-04T10:45:10.957Z")}},
        {age: {$gt: NumberInt(22)}},
        {age: {$lte: NumberInt(30)}},
    ]
},
                   [0, 1, 2, 3]);
assertQueryResults({
    $nor: [
        {salary: {$gt: Number(5000.00)}},
        {salary: {$lt: Number(5000.00)}},
        {debt: {$gt: NumberDecimal(100.43)}},
        {debt: {$lt: NumberDecimal(100.43)}},
    ]
},
                   [1]);
assertQueryResults({
    $nor: [
        {salary: {$gt: Number(5000.00), $lt: Number(5000.00)}},
        {debt: {$gt: NumberDecimal(100.43), $lt: NumberDecimal(100.43)}},
    ]
},
                   []);
assertQueryResults({
    $and: [
        {salary: {$gte: Number(5000.00), $lt: Number(5500.00)}},
        {debt: {$gte: NumberDecimal(99.00), $lt: NumberDecimal(100.50)}},
    ]
},
                   [1]);
assertQueryResults({
    $and: [
        {age: {$gt: NumberInt(19)}},
        {age: {$lte: NumberInt(25)}},
        {debt: {$gte: NumberDecimal(0.0)}},
        {debt: {$lte: NumberDecimal(1000.00)}},
    ]
},
                   [0, 1]);

/* ---------------------- Range Conjunction: open range ----------------------------------------- */
// TODO SERVER-70316. Fix minimum bound set on date so this test will pass.
// assertQueryResults({
//     $and: [
//         {birthdate: {$lt: ISODate("2002-12-04T10:45:10.957Z")}},
//         {debt: {$lte: NumberDecimal(5000.00)}}

//     ]
// },
//                    [0, 1]);

assertQueryResults({
    $or: [
        {birthdate: {$gt: ISODate("2002-12-04T10:45:10.957Z")}},
        {debt: {$lte: NumberDecimal(5000.00)}}

    ]
},
                   [0, 1, 3]);
assertQueryResults({
    $and: [
        {birthdate: {$gt: ISODate("2002-12-04T10:45:10.957Z")}},
        {birthdate: {$lte: ISODate("2012-12-04T10:45:10.957Z")}},
        {salary: {$lte: Number(5000.00)}}

    ]
},
                   [1]);

assertQueryResults({
    $and: [
        {savings: {$lte: NumberLong(9000)}},
        {debt: {$lt: NumberDecimal(5000.00)}}

    ]
},
                   [1, 3]);
assertQueryResults({
    $and: [
        {salary: {$lte: Number(100000)}},
        {age: {$gte: NumberInt(23)}}

    ]
},
                   [0, 1]);
assertQueryResults({
    $or: [
        {savings: {$lte: NumberLong(100000)}},
        {age: {$gte: NumberInt(30)}}

    ]
},
                   [1, 2, 3]);
assertQueryResults({
    $nor: [
        {savings: {$lte: NumberLong(100000)}},
        {age: {$gte: NumberInt(30)}}

    ]
},
                   [0]);

/* ---------------------- Range + Equality Conjunction/Disjunction ------------------------------ */
assertQueryResults({
    $and: [
        {age: {$gte: NumberInt(22)}},
        {age: {$lt: NumberInt(25)}},
        {zipcode: {$in: ["098765", "ABCDE", "1257"]}}

    ]
},
                   [1, 3]);
assertQueryResults({
    $or: [
        {salary: {$lte: Number(10000.00)}},
        {zipcode: {$eq: "123456789"}},
        {debt: {$ne: NumberDecimal(1250.69)}},
    ]
},
                   [0, 1, 2]);
assertQueryResults({
    $nor: [
        {salary: {$lte: Number(10000.00)}},
        {zipcode: {$eq: "123456789"}},
        {debt: {$eq: NumberDecimal(1250.69)}},
    ]
},
                   []);
assertQueryResults({$and: [{salary: {$gt: Number(10020)}}, {savings: {$eq: NumberLong(1230500)}}]},
                   [0]);
assertQueryResults({
    $and: [
        {birthdate: {$gt: ISODate("1995-11-25T10:45:10.957Z")}},
        {birthdate: {$lt: ISODate("2000-11-25T10:45:10.957Z")}},
        {zipcode: {$eq: "123456789"}}

    ]
},
                   [0]);

/* ---------------------- Range Conjunction with an unencrypted field --------------------------- */
assertQueryResults({
    $and:
        [{salary: {$gt: Number(10000)}}, {age: {$lt: NumberInt(30)}}, {karma: {$gt: 40, $lt: 450}}]
},
                   [3]);

assertQueryResults({
    $and: [
        {debt: {$gt: NumberDecimal(1000)}},
        {debt: {$lt: NumberDecimal(1000000)}},
        {karma: {$gte: 40, $lt: 450}}
    ]
},
                   [2, 3]);

assertQueryResults({
    $and: [
        {
            birthdate:
                {$gt: ISODate("1991-12-04T10:45:10.957Z"), $lt: ISODate("2004-11-04T10:45:10.957Z")}
        },
        {debt: {$gt: NumberDecimal(20.0)}},
        {age: {$gt: NumberInt(30)}},
        {savings: {$lt: NumberLong(5000000)}},
        {salary: {$gte: Number(100040.00)}},
        {karma: {$gte: 40, $lt: 450}},
        {zipcode: {$ne: "098765"}}

    ]
},
                   [2]);
}());
