/**
 * Check that encryption in certain contexts.
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
        path: "date",
        bsonType: "date",
        queries: {
            queryType: "range",
            sparsity: 1,
            min: ISODate("1990-01-01"),
            max: ISODate("2020-01-01"),
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

// [pipeline, expectedErrorCode]
const cases = [
    // Can't encrypt _id field.
    [[{$group: {_id: "$age", sum: {$sum: "$unencrypted"}}}],
     51222],  // 0
    // Can't encrypt accumulated field.
    [[{$group: {_id: "$unencrypted", sum: {$sum: "$age"}}}],
     51221],  // 1
    [[{$group: {_id: "$unencrypted", set: {$push: "$age"}}}],
     6331102],  // 2
    // Ensure the same behavior under other expressions.
    [
        [{
            $group: {
                _id: "$unencrypted",
                sum: {$sum: {$cond: {if: {$in: ["$age", [1, 2, 3]]}, then: 1, else: 2}}}
            }
        }],
        [6994304, 6331102]
    ],  // 3
    // TODO SERVER-70862 Decide whether this should succeed.
    [
        [{
            $group: {
                _id: "$unencrypted",
                sum: {$sum: {$cond: {if: {$gt: ["$age", NumberInt(3)]}, then: 1, else: 2}}}
            }
        }],
        7020507
    ],  // 4
    [
        [{
            $group: {
                _id: "$unencrypted",
                sum: {
                    $push: {
                        $cond: {
                            if: {$gt: ["$otherUnencrypted", NumberInt(3)]},
                            then: "$age",
                            else: false
                        }
                    }
                }
            }
        }],
        [6994304, 6331102]
    ],  // 5
    [
        [{$match: {$expr: {$in: ["$nested", [{age: 10, other: 5}, {age: 20, other: 10}]]}}}],
        7036804
    ],  // 6
    [
        [
            {$match: {age: {$gt: NumberInt(10)}}},
            {$unionWith: {coll: coll.getName(), pipeline: [{$match: {age: {$lt: NumberInt(2)}}}]}}
        ],
        [31011]
    ],
    // TODO SERVER-59284: Change expected error code to only be 9710000 after feature flag is
    // enabled by default.
    [
        [
            {$match: {age: {$gt: NumberInt(10)}}},
            {$unionWith: {coll: "other", pipeline: [{$match: {age: {$lt: NumberInt(2)}}}]}}
        ],
        [51204, 9710000]
    ]
];

for (let testNum = 0; testNum < cases.length; testNum++) {
    jsTestLog("Running test " + testNum);
    const testCase = cases[testNum];
    if (testCase.length == 2) {
        assert.commandFailedWithCode(
            testDB.runCommand(Object.assign(
                {aggregate: coll.getName(), pipeline: testCase[0], cursor: {}}, schema)),
            testCase[1]);
    } else {
        assert.commandWorked(testDB.runCommand(
            Object.assign({aggregate: coll.getName(), pipeline: testCase[0], cursor: {}}, schema)));
    }
}

mongocryptd.stop();
