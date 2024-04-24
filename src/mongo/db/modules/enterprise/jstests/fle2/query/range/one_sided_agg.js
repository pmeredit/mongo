/**
 * Tests for encrypted aggregation queries which only have one endpoint. These queries are expected
 * to take a long time to complete and so are only run against replicasets, since the added latency
 * with mongos exceeds the default timeout.
 *
 * @tags: [
 *   requires_fcv_80,
 *   fle2_no_mongos,
 * ]
 */
import {assertArrayEq} from "jstests/aggregation/extras/utils.js";
import {isMongos} from "jstests/concurrency/fsm_workload_helpers/server_types.js";
import {EncryptedClient} from "jstests/fle2/libs/encrypted_client_util.js";

if (isMongos(db)) {
    jsTest.log("Test skipped on sharded clusters");
    quit();
}

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
    const res = assert.commandWorked(edb.erunCommand({insert: collName, documents: [doc]}));
    assert.eq(res.n, 1);
}
docs.forEach(insert);

function assertQueryResults(q, expected) {
    client.runEncryptionOperation(() => {
        const res = coll.aggregate([{$match: {$expr: q}}]).toArray();
        assert.eq(res.length, expected.length, tojson(q));
        assertArrayEq({actual: res.map(d => d._id), expected, extraErrorMsg: tojson({q, res})});
    });
}

let res = coll.find({}).toArray();
assert.eq(res.length, 4);

/* ---------------------- Basic Ranges: ----------------------------------------------- */
// NumberInt
assertQueryResults({$gte: ["$age", NumberInt(24)]}, [0, 2]);
assertQueryResults({$gt: ["$age", NumberInt(39)]}, []);
assertQueryResults({$not: [{$gt: ["$age", NumberInt(39)]}]}, [0, 1, 2, 3]);
assertQueryResults({$not: [{$gt: ["$age", NumberInt(24)]}]}, [1, 3]);

// NumberLong
assertQueryResults({$lte: ["$savings", NumberLong(10000)]}, [1, 3]);
assertQueryResults({$gt: ["$savings", NumberLong(2147483600)]}, []);

// Double
assertQueryResults({$gte: ["$salary", Number(10000.00)]}, [0, 2, 3]);
assertQueryResults({$lt: ["$salary", Number(1000.00)]}, []);
assertQueryResults({$not: [{$gte: ["$salary", Number(10000.00)]}]}, [1]);

// Decimal128
assertQueryResults({$lt: ["$debt", NumberDecimal(100.44)]}, [0, 1]);
assertQueryResults({$gte: ["$debt", NumberDecimal(20000.44)]}, []);
assertQueryResults({$not: [{$lt: ["$debt", NumberDecimal(100.44)]}]}, [2, 3]);

// Date
assertQueryResults({$gt: ["$birthdate", ISODate("2000-01-01T10:45:10.957Z")]}, [1, 3]);
assertQueryResults({$lt: ["$birthdate", ISODate("2002-12-04T10:45:10.957Z")]}, [0, 2, 3]);

assertQueryResults({$not: [{$lt: ["$birthdate", ISODate("2002-12-04T10:45:10.957Z")]}]}, [1]);

/* ---------------------- Conjunction/Disjunction ----------------------------------------- */
assertQueryResults({
    $and: [
        {$lt: ["$birthdate", ISODate("2002-12-04T10:45:10.957Z")]},
        {$lte: ["$debt", NumberDecimal(5000.00)]}
    ]
},
                   [0, 3]);

assertQueryResults({
    $or: [
        {$gt: ["$birthdate", ISODate("2002-12-04T10:45:10.957Z")]},
        {$lte: ["$debt", NumberDecimal(5000.00)]}
    ]
},
                   [0, 1, 3]);

assertQueryResults({
    $and: [
        {$gt: ["$birthdate", ISODate("2002-12-04T10:45:10.957Z")]},
        {$lte: ["$birthdate", ISODate("2012-12-04T10:45:10.957Z")]},
        {$lte: ["$salary", Number(5000.00)]}
    ]
},
                   [1]);

assertQueryResults(
    {$and: [{$lte: ["$savings", NumberLong(9000)]}, {$lt: ["$debt", NumberDecimal(5000.00)]}]},
    [1, 3]);
assertQueryResults({$and: [{$lte: ["$salary", Number(100000)]}, {$gte: ["$age", NumberInt(23)]}]},
                   [0, 1]);
assertQueryResults(
    {$or: [{$lte: ["$savings", NumberLong(100000)]}, {$gte: ["$age", NumberInt(30)]}]}, [1, 2, 3]);
assertQueryResults({
    $not: [{
        $or: [
            {$lte: ["$savings", NumberLong(100000)]},
            {$gte: ["$age", NumberInt(30)]},
        ]
    }]
},
                   [0]);
assertQueryResults({
    $or: [
        {$gt: ["$birthdate", ISODate("1995-09-15T07:30:10.957Z")]},
        {$lt: ["$birthdate", ISODate("2005-12-04T10:45:10.957Z")]},
        {$gt: ["$age", NumberInt(22)]},
        {$lte: ["$age", NumberInt(30)]},
    ]
},
                   [0, 1, 2, 3]);
assertQueryResults({
    $not: [{
        $or: [
            {$gt: ["$salary", Number(5000.00)]},
            {$lt: ["$salary", Number(5000.00)]},
            {$gt: ["$debt", NumberDecimal(100.43)]},
            {$lt: ["$debt", NumberDecimal(100.43)]},
        ]
    }]
},
                   [1]);
assertQueryResults({
    $not: [{
        $or: [
            {
                $and: [
                    {$gt: ["$salary", Number(5000.00)]},
                    {$lt: ["$salary", Number(5000.00)]},
                ]
            },
            {
                $and: [
                    {$gt: ["$debt", NumberDecimal(100.43)]},
                    {$lt: ["$debt", NumberDecimal(100.43)]},
                ]
            },
        ]
    }]
},
                   [0, 1, 2, 3]);

/* ---------------------- Range + Equality Conjunction/Disjunction ------------------------------ */
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
        {$gt: ["$birthdate", ISODate("1991-12-04T10:45:10.957Z")]},
        {$lt: ["$birthdate", ISODate("2004-11-04T10:45:10.957Z")]},
        {$gt: ["$debt", NumberDecimal(20.0)]},
        {$gt: ["$age", NumberInt(30)]},
        {$lt: ["$savings", NumberLong(5000000)]},
        {$gte: ["$salary", Number(100040.00)]},
        {$gte: ["$karma", 40]},
        {$lt: ["$karma", 450]},
        {$ne: ["$zipcode", "098765"]}
    ]
},
                   [2]);
