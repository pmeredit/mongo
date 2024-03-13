/**
 * Tests for encrypted aggregation queries which have two endpoints and can be expected to complete
 * in a reasonable amount of time.
 *
 * @tags: [
 * requires_fcv_80,
 * ]
 */
import {assertArrayEq} from "jstests/aggregation/extras/utils.js";
import {EncryptedClient, kSafeContentField} from "jstests/fle2/libs/encrypted_client_util.js";

(function() {
let dbName = jsTestName();
let dbTest = db.getSiblingDB(dbName);
dbTest.dropDatabase();

let client = new EncryptedClient(db.getMongo(), dbName);
const collName = "coll";

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
            },
            {
                path: "nested.age",
                bsonType: "int",
                queries: {queryType: "range", min: NumberInt(0), max: NumberInt(255), sparsity: 1}
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
        karma: 500,
        nested: {age: NumberInt(25), randomId: 209}
    },
    {
        _id: 1,
        age: NumberInt(23),
        savings: NumberLong(8540),
        debt: NumberDecimal(100.43),
        salary: Number(5000.00),
        birthdate: ISODate("2004-12-04T10:45:10.957Z"),
        zipcode: "ABCDE",
        karma: 20,
        nested: {age: NumberInt(23), randomId: 310}
    },
    {
        _id: 2,
        age: NumberInt(38),
        savings: NumberLong(4126000),
        debt: NumberDecimal(10321.69),
        salary: Number(160040.22),
        birthdate: ISODate("1992-07-30T10:45:10.957Z"),
        zipcode: "123456789",
        karma: 40,
        nested: {age: NumberInt(38), randomId: 329}

    },
    {
        _id: 3,
        age: NumberInt(22),
        savings: NumberLong(400),
        debt: NumberDecimal(1250.69),
        salary: Number(10540),
        birthdate: ISODate("2000-07-30T10:45:10.957Z"),
        zipcode: "098765",
        karma: 250,
        nested: {age: NumberInt(22), randomId: 401}

    },
];

// Bulk inserts aren't supported in FLE2, so insert each one-by-one.
function insert(doc) {
    const res = assert.commandWorked(edb.runCommand({insert: collName, documents: [doc]}));
    assert.eq(res.n, 1);
}
docs.forEach(insert);

function assertQueryResults(q, expected) {
    const res = coll.aggregate([{$match: {$expr: q}}]).toArray();
    assert.eq(res.length, expected.length, tojson(q));
    assertArrayEq({actual: res.map(d => d._id), expected, extraErrorMsg: tojson({q, res})});
}

let res = coll.find({}).toArray();
assert.eq(res.length, 4);

/* ---------------------- Open and Closed Ranges: ----------------------------------------------- */
// NumberInt
assertQueryResults({$and: [{$gte: ["$age", NumberInt(23)]}, {$lte: ["$age", NumberInt(38)]}]},
                   [0, 1, 2]);
assertQueryResults({$and: [{$gte: ["$age", NumberLong(23)]}, {$lte: ["$age", NumberLong(38)]}]},
                   [0, 1, 2]);  // Answering an 'int' index with NumberLong literals.
assertQueryResults({$eq: ["$age", NumberInt(38)]},
                   [2]);  // Answering equality query with range index.
assertQueryResults({$ne: ["$age", NumberInt(38)]},
                   [0, 1, 3]);  // Answering equality query with range index.
assertQueryResults({$in: ["$age", [NumberInt(38), NumberInt(22)]]},
                   [2, 3]);  // Answering equality query with range index.
assertQueryResults({$in: ["$nested.age", [NumberInt(38), NumberInt(22)]]},
                   [2, 3]);  // Answering equality query with range index.

// NumberLong
assertQueryResults(
    {$and: [{$gt: ["$savings", NumberLong(10000)]}, {$lt: ["$savings", NumberLong(2000000)]}]},
    [0]);
assertQueryResults(
    {$and: [{$gt: ["$savings", NumberInt(0)]}, {$lt: ["$savings", NumberInt(10000)]}]},
    [1, 3]);  // Answering a long index with int literals.
assertQueryResults({
    $not:
        [{$and: [{$gt: ["$savings", NumberLong(10000)]}, {$lt: ["$savings", NumberLong(2000000)]}]}]
},
                   [1, 2, 3]);
assertQueryResults({$eq: ["$savings", NumberLong(4126000)]},
                   [2]);  // Answering equality query with range index.
assertQueryResults({$ne: ["$savings", NumberLong(4126000)]},
                   [0, 1, 3]);  // Answering equality query with range index.
assertQueryResults({$in: ["$savings", [NumberLong(1230500), NumberLong(8540), NumberLong(1203)]]},
                   [0, 1]);  // Answering equality query with range index.

// Double
assertQueryResults(
    {$and: [{$gt: ["$salary", Number(10000)]}, {$lt: ["$salary", Number(160040.22)]}]}, [0, 3]);
assertQueryResults({$ne: ["$salary", Number(10540)]},
                   [0, 1, 2]);  // Answering equality query with range index.
assertQueryResults({$eq: ["$salary", Number(160040.22)]},
                   [2]);  // Answering equality query with range index.
assertQueryResults({$in: ["$salary", [Number(160040.22), Number(16000.00)]]}, [0, 2]);

// Decimal128
assertQueryResults(
    {$and: [{$lt: ["$debt", NumberDecimal(5000)]}, {$gte: ["$debt", NumberDecimal(1250.69)]}]},
    [3]);
assertQueryResults({$eq: ["$debt", NumberDecimal(0.0)]},
                   [0]);  // Answering equality query with range index.
assertQueryResults({$ne: ["$debt", NumberDecimal(0.0)]},
                   [1, 2, 3]);  // Answering equality query with range index.
assertQueryResults({$in: ["$debt", [NumberDecimal(100.43), NumberDecimal(10321.69)]]}, [1, 2]);

// Date
assertQueryResults({
    $and: [
        {$lte: ["$birthdate", ISODate("2000-01-01T10:45:10.957Z")]},
        {$gte: ["$birthdate", ISODate("1993-05-16T10:45:10.957Z")]}
    ]
},
                   [0]);
assertQueryResults({
    $not: {
        $and: [
            {$lt: ["$birthdate", ISODate("2000-01-01T10:45:10.957Z")]},
            {$gte: ["$birthdate", ISODate("1993-05-16T10:45:10.957Z")]}
        ]
    }
},
                   [1, 2, 3]);
assertQueryResults({$eq: ["$birthdate", ISODate("1992-07-30T10:45:10.957Z")]},
                   [2]);  // Answering equality query with range index.
assertQueryResults({$ne: ["$birthdate", ISODate("1992-07-30T10:45:10.957Z")]},
                   [0, 1, 3]);  // Answering equality query with range index.
assertQueryResults({
    $in: ["$birthdate", [ISODate("1999-09-15T07:30:10.957Z"), ISODate("1992-07-30T10:45:10.957Z")]]
},
                   [0, 2]);
/* ---------------------- Range Conjunction: closed range --------------------------------------- */
assertQueryResults({
    $and: [
        {$gt: ["$birthdate", ISODate("1995-09-15T07:30:10.957Z")]},
        {$lt: ["$birthdate", ISODate("2005-12-04T10:45:10.957Z")]},
        {$gt: ["$savings", NumberLong(800)]},
        {$lte: ["$savings", NumberLong(1230500)]}
    ]
},
                   [0, 1]);

assertQueryResults({
    $and: [
        {$gte: ["$salary", Number(5000.00)]},
        {$lt: ["$salary", Number(5500.00)]},
        {$gte: ["$debt", NumberDecimal(99.00)]},
        {$lt: ["$debt", NumberDecimal(100.50)]},
    ]
},
                   [1]);
assertQueryResults({
    $and: [
        {$gt: ["$age", NumberInt(19)]},
        {$lte: ["$age", NumberInt(25)]},
        {$gte: ["$debt", NumberDecimal(0.0)]},
        {$lte: ["$debt", NumberDecimal(1000.00)]},
    ]
},
                   [0, 1]);

/* ---------------------- Range + Equality Conjunction/Disjunction ------------------------------ */
assertQueryResults({
    $and: [
        {$gte: ["$age", NumberInt(22)]},
        {$lt: ["$age", NumberInt(25)]},
        {$in: ["$zipcode", ["098765", "ABCDE", "1257"]]}
    ]
},
                   [1, 3]);

assertQueryResults({
    $and: [
        {$gte: ["$salary", Number(100)]},
        {$lt: ["$salary", Number(11000)]},
        {$in: ["$nested.age", [NumberInt(23), NumberInt(22)]]}
    ]
},
                   [1, 3]);
assertQueryResults({
    $or: [
        {$lte: ["$salary", Number(10000.00)]},
        {$eq: ["$zipcode", "123456789"]},
        {$ne: ["$debt", NumberDecimal(1250.69)]},
    ]
},
                   [0, 1, 2]);
assertQueryResults({
    $not: [{
        $or: [
            {$lte: ["$salary", Number(10000.00)]},
            {$eq: ["$zipcode", "123456789"]},
            {$eq: ["$debt", NumberDecimal(1250.69)]},
        ]
    }]
},
                   []);
assertQueryResults({
    $and: [
        {$gt: ["$salary", Number(10020)]},
        {$eq: ["$savings", NumberLong(1230500)]},
    ]
},
                   [0]);
assertQueryResults({
    $and: [
        {$gt: ["$birthdate", ISODate("1995-11-25T10:45:10.957Z")]},
        {$lt: ["$birthdate", ISODate("2000-11-25T10:45:10.957Z")]},
        {$eq: ["$zipcode", "123456789"]}
    ]
},
                   [0]);

/* ---------------------- Range Conjunction with an unencrypted field --------------------------- */
assertQueryResults({
    $and: [
        {$gt: ["$salary", Number(10000)]},
        {$lt: ["$age", NumberInt(30)]},
        {$gt: ["$karma", 40]},
        {$lt: ["$karma", 450]},
    ]
},
                   [3]);

assertQueryResults({
    $and: [
        {$gt: ["$debt", NumberDecimal(1000)]},
        {$lt: ["$debt", NumberDecimal(1000000)]},
        {$gte: ["$karma", 40]},
        {$lt: ["$karma", 450]},
    ]
},
                   [2, 3]);

/* ---------------------- Testing other aggregation operators --------------------------- */
assertQueryResults({
    $cond: [
        {
            $and: [
                {$gte: ["$age", NumberInt(23)]},
                {$lte: ["$age", NumberInt(38)]},
            ]
        },
        true,
        false
    ]
},
                   [0, 1, 2]);
assertQueryResults({
    $cond: [
        {
            $and: [
                {$gte: ["$age", NumberInt(23)]},
                {$lte: ["$age", NumberInt(38)]},
            ]
        },
        false,
        true
    ]
},
                   [3]);
}());

(function() {
let dbName = 'basic_insert_range';
let dbTest = db.getSiblingDB(dbName);
dbTest.dropDatabase();

let client = new EncryptedClient(db.getMongo(), dbName);
const collName = jsTestName();

jsTest.log("createEncryptionCollection");
assert.commandWorked(client.createEncryptionCollection(collName, {
    encryptedFields: {
        fields: [
            {path: "ssn", bsonType: "string", queries: {queryType: "equality"}},
            {
                path: "age",
                bsonType: "long",
                queries: {
                    queryType: "range",
                    min: NumberLong(0),
                    max: NumberLong(255),
                    sparsity: 1,
                }
            }
        ]
    }

}));
let edb = client.getDB();

// Documents that will be used in the following tests. The ssn and age fields have an encrypted
// equality index.
const docs = [
    {_id: 0, ssn: "123", name: "A", manager: "B", age: NumberLong(25), location: [0, 0]},
    {_id: 1, ssn: "456", name: "B", manager: "C", age: NumberLong(35), location: [0, 1]},
    {_id: 2, ssn: "789", name: "C", manager: "D", age: NumberLong(45), location: [0, 2]},
    {_id: 3, ssn: "123", name: "D", manager: "A", age: NumberLong(55), location: [0, 3]},
];

const coll = edb[collName];
for (const doc of docs) {
    assert.commandWorked(coll.insert(doc));
}
assert.commandWorked(coll.createIndex({location: "2dsphere"}));
const runTest = (pipeline, options, collection, expected, extraInfo) => {
    const aggPipeline = pipeline.slice();
    aggPipeline.push(
        {$project: {[kSafeContentField]: 0, [`chain.${kSafeContentField}`]: 0, distance: 0}});
    const result = collection.aggregate(aggPipeline, options).toArray();
    assertArrayEq({actual: result, expected: expected, extraErrorMsg: tojson(extraInfo)});
};

const tests = [{
            pipeline: [
                {
                    $graphLookup: {
                        from: collName,
                        as: "chain",
                        connectToField: "name",
                        connectFromField: "manager",
                        startWith: "$manager",
                        restrictSearchWithMatch: {age: {$not: {$gte: NumberLong(40), $lte: NumberLong(50)}}}
                    }
                }
            ],
            expected: [
                Object.assign({chain: [docs[1]]}, docs[0]),
                Object.assign({chain: []}, docs[1]),
                Object.assign({chain: [docs[3], docs[0], docs[1]]}, docs[2]),
                Object.assign({chain: [docs[0], docs[1]]}, docs[3]),
            ]
        },
        {
            pipeline: [
                {
                    $geoNear: {
                        near: {type: "Point", coordinates: [0, 0]},
                        distanceField: "distance",
                        key: "location",
                        query: {age: {$gt: NumberLong(30), $lt: NumberLong(46)}}
                    }
                }
            ],
            expected: [
                docs[1],
                docs[2]
            ]
        },
    ];

for (const testData of tests) {
    const extraInfo = Object.assign({transaction: false}, testData);
    runTest(testData.pipeline, {}, coll, testData.expected, extraInfo);
}
}());
