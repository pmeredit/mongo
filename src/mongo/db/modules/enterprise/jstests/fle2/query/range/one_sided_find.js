/**
 * Tests for encrypted find queries which only have one endpoint. These queries are expected
 * to take a long time to complete and so are only run against replicasets, since the added latency
 * with mongos exceeds the default timeout.
 *
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

/* ---------------------- Basic Cases: ----------------------------------------------- */
// NumberInt
assertQueryResults({age: {$gte: NumberInt(24)}}, [0, 2]);
assertQueryResults({age: {$gt: NumberInt(39)}}, []);
assertQueryResults({age: {$not: {$gt: NumberInt(39)}}}, [0, 1, 2, 3]);
assertQueryResults({age: {$not: {$gt: NumberInt(24)}}}, [1, 3]);

// NumberLong
assertQueryResults({savings: {$lte: NumberLong(10000)}}, [1, 3]);
assertQueryResults({savings: {$gt: NumberLong(2147483600)}}, []);

// Double
assertQueryResults({salary: {$gte: Number(10000.00)}}, [0, 2, 3]);
assertQueryResults({salary: {$lt: Number(1000.00)}}, []);
assertQueryResults({salary: {$not: {$gte: Number(10000.00)}}}, [1]);

// Decimal128
assertQueryResults({debt: {$lt: NumberDecimal(100.44)}}, [0, 1]);
assertQueryResults({debt: {$gte: NumberDecimal(20000.44)}}, []);
assertQueryResults({debt: {$not: {$lt: NumberDecimal(100.44)}}}, [2, 3]);

// Date
assertQueryResults({birthdate: {$gt: ISODate("2000-01-01T10:45:10.957Z")}}, [1, 3]);
assertQueryResults({birthdate: {$lt: ISODate("2002-12-04T10:45:10.957Z")}}, [0, 2, 3]);
assertQueryResults({birthdate: {$not: {$lt: ISODate("2002-12-04T10:45:10.957Z")}}}, [1]);

/* ---------------------- Range Conjunctions ----------------------------------------- */
assertQueryResults({
    $and: [
        {birthdate: {$lt: ISODate("2002-12-04T10:45:10.957Z")}},
        {debt: {$lte: NumberDecimal(5000.00)}}
    ]
},
                   [0, 3]);

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
                   [0, 1, 2, 3]);

/* ---------------------- Range + Equality Conjunction/Disjunction ------------------------------ */
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

/* ---------------------- Range Conjunction with an unencrypted field --------------------------- */
assertQueryResults({
    $and:
        [{salary: {$gt: Number(10000)}}, {age: {$lt: NumberInt(30)}}, {karma: {$gt: 40, $lt: 450}}]
},
                   [3]);

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
