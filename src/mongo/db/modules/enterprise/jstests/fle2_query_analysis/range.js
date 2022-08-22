/**
 * Basic set of tests to verify the command response from query analysis for the aggregate command.
 */
(function() {
"use strict";

load("src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js");
load("src/mongo/db/modules/enterprise/jstests/fle/lib/utils.js");

const mongocryptd = new MongoCryptD();
mongocryptd.start();
const conn = mongocryptd.getConnection();
const testDB = conn.getDB("test");
const coll = testDB.coll;

const fields = [
    {
        path: "age",
        bsonType: "int",
        queries: {
            queryType: "range",
            sparsity: 0,
            min: NumberInt(0),
            max: NumberInt(200),
        },
        keyId: UUID()
    },
    {
        path: "salary",
        bsonType: "int",
        queries: {
            queryType: "range",
            sparsity: 0,
            min: NumberInt(0),
            max: NumberInt(1000000),
        },
        keyId: UUID()
    },
    {
        path: "ssn",
        bsonType: "string",
        queries: {
            queryType: "equality",
        },
        keyId: UUID()
    }
];
const schema = {
    encryptionInformation: {
        type: 1,
        schema: {
            "test.coll": {fields},
        }
    }
};

testDB.adminCommand({setParameter: 1, featureFlagFLE2Range: true});

function assertEncryptedFieldInResponse({filter, path = "", secondPath = "", requiresEncryption}) {
    const res = assert.commandWorked(
        testDB.runCommand(Object.assign({find: "coll", filter: filter}, schema)));

    assert.eq(res.result.find, "coll", tojson(res));
    assert.eq(res.hasEncryptionPlaceholders, requiresEncryption, tojson(res));

    if (path) {
        let elt = res.result.filter;
        if (Array.isArray(path)) {
            for (const step of path) {
                assert(elt[step] !== undefined, tojson({elt, path, res}));
                elt = elt[step];
            }
        } else if (path) {
            elt = elt[path];
        }
        assert(elt instanceof BinData || elt["$encryptedBetween"] instanceof BinData, tojson(res));
    }
    if (secondPath) {
        let elt = res.result.filter;
        if (Array.isArray(secondPath)) {
            for (const step of secondPath) {
                assert(elt[step] !== undefined, tojson({elt, secondPath, res}));
                elt = elt[step];
            }
        } else if (secondPath) {
            elt = elt[secondPath];
        }
        assert(elt instanceof BinData || elt["$encryptedBetween"] instanceof BinData, tojson(res));
    }
}

const cases = [
    [{age: {$gt: 5}}, true, "age"],

    [{age: {$gte: 23, $lte: 35}}, true, "age"],
    // Verify other comparison operators.
    [{age: {$gt: 23, $lt: 35}}, true, "age"],
    [{age: {$gte: 23, $lt: 35}}, true, "age"],
    [{age: {$gt: 23, $lte: 35}}, true, "age"],

    [{$and: [{age: {$gte: 23}}, {age: {$lte: 35}}]}, true, "age"],
    [
        {
            $and: [
                {age: {$gte: 23}},
                {age: {$lte: 35}},
                {salary: {$gte: 50000}},
                {salary: {$lte: 75000}}
            ]
        },
        true,
        ["$and", "0", "age"],
        ["$and", "1", "salary"],
    ],
    // Verify other comparison operators.
    [
        {
            $and: [
                {age: {$gte: 23}},
                {age: {$lt: 35}},
                {salary: {$gt: 50000}},
                {salary: {$lte: 75000}}
            ]
        },
        true,
        ["$and", "0", "age"],
        ["$and", "1", "salary"],
    ],
    [
        {$and: [{age: {$gte: 23, $lte: 35}}, {ssn: "123456789"}]},
        true,
        ["$and", "0", "age"],
        ["$and", "1", "ssn", "$eq"],
    ],
    [
        {$and: [{age: {$gte: 23, $lte: 35}}, {ssn: {$in: ["123", "456", "789"]}}]},
        true,
        ["$and", "0", "age"],
        ["$and", "1", "ssn", "$in", "2"],
    ],
    [
        {
            $and: [
                {age: {$gte: 23}},
                {age: {$lte: 35}},
                {karma: {$gte: 50000}},
                {karma: {$lte: 75000}}
            ]
        },
        true,
        // The first two elements in the conjunction are the bounds for the unencrypted predicate.
        ["$and", "2", "age"],
    ],
    [
        {
            $and:
                [{age: {$gte: 23}}, {age: {$lt: 35}}, {karma: {$gt: 50000}}, {karma: {$lte: 75000}}]
        },
        true,
        // The first two elements in the conjunction are the bounds for the unencrypted predicate.
        ["$and", "2", "age"],
    ],
    [{$and: [{karma: {$gte: 50000}}, {karma: {$lte: 75000}}]}, false],
    [{$and: [{karma: {$gt: 50000}}, {karma: {$lt: 75000}}]}, false],
    [{$and: [{karma: {$gte: 50000}}, {karma: {$lt: 75000}}]}, false],
];

for (const testCase of cases) {
    assertEncryptedFieldInResponse({
        filter: testCase[0],
        requiresEncryption: testCase[1],
        path: testCase[2],
        secondPath: testCase[3],
    });
}

mongocryptd.stop();
}());
