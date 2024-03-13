/**
 * Basic set of tests to verify the command response from query analysis for the aggregate command.
 * @tags: [
 * requires_fcv_80,
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
        queries: {
            queryType: "range",
            sparsity: 1,
            min: NumberInt(0),
            max: NumberInt(200),
        },
        keyId: UUID()
    },
    {
        path: "long_age",
        bsonType: "long",
        queries: {
            queryType: "range",
            sparsity: 1,
            min: NumberLong(0),
            max: NumberLong(200),
        },
        keyId: UUID()
    },
    {
        path: "doubleAge",
        bsonType: "double",
        queries: {
            queryType: "range",
            sparsity: 1,
        },
        keyId: UUID()
    },
    {
        path: "salary",
        bsonType: "int",
        queries: {
            queryType: "range",
            sparsity: 1,
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
    },
    {
        path: "nested.age",
        bsonType: "int",
        queries: {
            queryType: "range",
            sparsity: 1,
            min: NumberInt(0),
            max: NumberInt(200),
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

function assertEncryptedFieldInResponse({filter, paths = [], requiresEncryption}) {
    const res = assert.commandWorked(
        testDB.runCommand(Object.assign({find: "coll", filter: filter}, schema)));

    assert.eq(res.result.find, "coll", tojson(res));
    assert.eq(res.hasEncryptionPlaceholders, requiresEncryption, tojson(res));
    if (!requiresEncryption) {
        return;
    }

    for (let path of paths) {
        let elt = res.result.filter;
        if (Array.isArray(path)) {
            for (const step of path) {
                assert(elt[step] !== undefined, tojson({input: filter, elt, path, res}));
                elt = elt[step];
            }
        } else if (path) {
            elt = elt[path];
        }
        assert(elt instanceof BinData, tojson({input: filter, path, res}));
    }
}

const cases = [
    [{age: {$gt: NumberInt(5)}}, true, ["age", "$gt"]],
    [{age: {$gt: NumberLong(5)}}, true, ["age", "$gt"]],
    [{long_age: {$gt: NumberInt(5)}}, true, ["long_age", "$gt"]],
    [{long_age: {$gt: NumberLong(5)}}, true, ["long_age", "$gt"]],
    [{age: {$gt: NumberInt(Infinity)}}, true, ["age", "$gt"]],
    [
        {age: {$gte: NumberInt(23), $lte: NumberInt(35)}},
        true,
        ["$and", "0", "age", "$gte"],
        ["$and", "1", "age", "$lte"]
    ],
    // Verify other comparison operators.
    [
        {age: {$gt: NumberInt(23), $lt: NumberInt(35)}},
        true,
        ["$and", "0", "age", "$gt"],
        ["$and", "1", "age", "$lt"]
    ],
    [
        {age: {$gte: NumberInt(23), $lt: NumberInt(35)}},
        true,
        ["$and", "0", "age", "$gte"],
        ["$and", "1", "age", "$lt"]
    ],
    [
        {age: {$gt: NumberInt(23), $lte: NumberInt(35)}},
        true,
        ["$and", "0", "age", "$gt"],
        ["$and", "1", "age", "$lte"]
    ],
    [
        {$and: [{age: {$gte: NumberInt(23)}}, {age: {$lte: NumberInt(35)}}]},
        true,
        ["$and", "0", "age", "$gte"],
        ["$and", "1", "age", "$lte"]
    ],
    [
        {
            $and: [
                {age: {$gte: NumberInt(23)}},
                {age: {$lte: NumberInt(35)}},
                {salary: {$gte: NumberInt(50000)}},
                {salary: {$lte: NumberInt(75000)}}
            ]
        },
        true,
        ["$and", "0", "$and", "0", "age", "$gte"],
        ["$and", "0", "$and", "1", "age", "$lte"],
        ["$and", "1", "$and", "0", "salary", "$gte"],
        ["$and", "1", "$and", "1", "salary", "$lte"],
    ],
    // Verify other comparison operators.
    [
        {
            $and: [
                {age: {$gte: NumberInt(23)}},
                {age: {$lt: NumberInt(35)}},
                {salary: {$gt: NumberInt(50000)}},
                {salary: {$lte: NumberInt(75000)}}
            ]
        },
        true,
        ["$and", "0", "$and", "0", "age", "$gte"],
        ["$and", "0", "$and", "1", "age", "$lt"],
        ["$and", "1", "$and", "0", "salary", "$gt"],
        ["$and", "1", "$and", "1", "salary", "$lte"],
    ],
    [
        {$and: [{age: {$gte: NumberInt(23), $lte: NumberInt(35)}}, {ssn: "123456789"}]},
        true,
        ["$and", "0", "$and", "0", "age", "$gte"],
        ["$and", "0", "$and", "1", "age", "$lte"],
        ["$and", "1", "ssn", "$eq"],
    ],
    [
        {
            $and: [
                {age: {$gte: NumberInt(23), $lte: NumberInt(35)}},
                {ssn: {$in: ["123", "456", "789"]}}
            ]
        },
        true,
        ["$and", "0", "$and", "0", "age", "$gte"],
        ["$and", "0", "$and", "1", "age", "$lte"],
        ["$and", "1", "ssn", "$in", "2"],
    ],
    [
        {
            $and: [
                {age: {$gte: NumberInt(23)}},
                {age: {$lte: NumberInt(35)}},
                {karma: {$gte: 50000}},
                {karma: {$lte: 75000}}
            ]
        },
        true,
        // The first two elements in the conjunction are the bounds for the unencrypted
        // predicate.
        ["$and", "2", "$and", "0", "age", "$gte"],
        ["$and", "2", "$and", "1", "age", "$lte"],
    ],
    [
        {
            $and: [
                {age: {$gte: NumberInt(23)}},
                {age: {$lt: NumberInt(35)}},
                {karma: {$gt: 50000}},
                {karma: {$lte: 75000}}
            ]
        },
        true,
        // The first two elements in the conjunction are the bounds for the unencrypted
        // predicate.
        ["$and", "2", "$and", "0", "age", "$gte"],
        ["$and", "2", "$and", "1", "age", "$lt"],
    ],
    [{$and: [{karma: {$gte: 50000}}, {karma: {$lte: 75000}}]}, false],
    [{$and: [{karma: {$gt: 50000}}, {karma: {$lt: 75000}}]}, false],
    [{$and: [{karma: {$gte: 50000}}, {karma: {$lt: 75000}}]}, false],
    // Verify that an equality can be encrypted if there is only a range encryption index.
    [
        {salary: {$eq: NumberInt(23)}},
        true,
        ["$and", "0", "salary", "$gte"],
        ["$and", "1", "salary", "$lte"],
    ],
    // Verify that an $eq under a $and is properly replaced with range indexes.
    [
        {$and: [{salary: {$gt: NumberInt(10020)}}, {age: {$eq: NumberInt(35)}}]},
        true,
        ["$and", "0", "$and", "0", "age", "$gte"],
        ["$and", "0", "$and", "1", "age", "$lte"],
        ["$and", "1", "salary", "$gt"],
    ],
    // Verify that one-sided date ranges work properly under a $and.
    [
        {$and: [{birthdate: {$lt: ISODate("2002-12-04T10:45:10.957Z")}}]},
        true,
        ["$and", "0", "birthdate", "$lt"]
    ],
    [
        {$and: [{birthdate: {$gt: ISODate("2002-12-04T10:45:10.957Z")}}]},
        true,
        ["$and", "0", "birthdate", "$gt"]
    ],
    [
        {$and: [{age: {$gt: NumberInt(50)}}, {age: {$lt: NumberInt(25)}}]},
        false,
    ],
    [
        {age: {$in: [NumberInt(10), NumberInt(20)]}},
        true,
        ["$or", "0", "$and", "0", "age", "$gte"],
        ["$or", "0", "$and", "1", "age", "$lte"],
        ["$or", "1", "$and", "0", "age", "$gte"],
        ["$or", "1", "$and", "1", "age", "$lte"]
    ],
    [
        {
            $or: [
                {$and: [{age: {$gt: NumberInt(20)}}, {age: {$lt: NumberInt(10)}}]},
                {$and: [{age: {$gt: NumberInt(90)}}, {age: {$lt: NumberInt(100)}}]}
            ],

        },
        true,
        ["$or", "1", "$and", "0", "age", "$gt"],
        ["$or", "1", "$and", "1", "age", "$lt"],
    ]
];

for (const testCase of cases) {
    jsTestLog("Running test " + tojson(testCase[0]));
    const paths = testCase.slice(2);
    assertEncryptedFieldInResponse({filter: testCase[0], requiresEncryption: testCase[1], paths});
}
mongocryptd.stop();
