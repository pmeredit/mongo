/**
 * Tests for encrypted find queries.
 *
 * @tags: [
 * featureFlagFLE2Range,
 * requires_fle2_in_always,
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
        "fields": [{
            path: "age",
            bsonType: "int",
            queries: {queryType: "range", min: NumberInt(0), max: NumberInt(255), sparsity: 1}
        }]
    }
}));

let edb = client.getDB();
const coll = edb[collName];

const docs = [
    {_id: 0, age: NumberInt(25)},
    {_id: 1, age: NumberInt(23)},
    {_id: 2, age: NumberInt(38)},
    {_id: 3, age: NumberInt(22)},
];

// Bulk inserts aren't supported in FLE2, so insert each one-by-one.
function insert(doc) {
    const res = assert.commandWorked(edb.runCommand({insert: collName, documents: [doc]}));
    assert.eq(res.n, 1);
}
docs.forEach(insert);

function assertQueryResults(q, expected) {
    const res = coll.find({age: q}).toArray();
    assert.eq(res.length, expected.length, tojson(q));
    assertArrayEq({actual: res.map(d => d._id), expected, extraErrorMsg: tojson({q, res})});
}

let res = coll.find({}).toArray();
assert.eq(res.length, 4);

assertQueryResults({$gte: NumberInt(24)}, [0, 2]);
assertQueryResults({$gte: NumberInt(39)}, []);
assertQueryResults({$gte: NumberInt(23), $lte: NumberInt(38)}, [0, 1, 2]);
assertQueryResults({$gte: NumberInt(23), $lte: NumberInt(35)}, [0, 1]);
assertQueryResults({$gte: NumberInt(20), $lte: NumberInt(30)}, [0, 1, 3]);
assertQueryResults({$eq: NumberInt(38)}, [2]);
assertQueryResults({$ne: NumberInt(38)}, [0, 1, 3]);
}());
