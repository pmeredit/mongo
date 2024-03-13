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

function assertEncryptedFieldInResponse({pipeline, pathArray, requiresEncryption}) {
    const res = assert.commandWorked(testDB.runCommand(
        Object.assign({aggregate: coll.getName(), pipeline: pipeline, cursor: {}}, schema)));

    assert.eq(res.hasEncryptionPlaceholders, requiresEncryption, tojson(res));
    if (!requiresEncryption) {
        return;
    }
    for (let path of pathArray) {
        let elt = res.result.pipeline;
        if (Array.isArray(path)) {
            for (const step of path) {
                assert(elt[step] !== undefined, tojson({elt, path, res, step}));
                elt = elt[step];
            }
        } else if (path) {
            elt = elt[path];
        }
        assert(elt instanceof BinData, tojson({elt, path, res}));
    }
}

// [pipeline, expectEncryption, [encryptedPath1, encryptedPath2]]
const cases = [
    // $match with match exprs
    [[{$match: {age: {$gt: NumberInt(5)}}}], true, [["0", "$match", "age", "$gt"]]],  // 0
    // $match with $expr. Open intervals.
    [
        [{$match: {$expr: {$gt: ["$age", NumberInt(5)]}}}],
        true,
        [
            ["0", "$match", "$expr", "$and", "0", "$gt", "1", "$const"],
            ["0", "$match", "$expr", "$and", "1", "$lte", "1", "$const"]
        ]
    ],  // 1
    [
        [{$match: {$expr: {$gt: ["$nested.age", NumberInt(5)]}}}],
        true,
        [
            ["0", "$match", "$expr", "$and", "0", "$gt", "1", "$const"],
            ["0", "$match", "$expr", "$and", "1", "$lte", "1", "$const"]
        ]
    ],  // 2
    [
        [{
            $match: {
                $expr: {$and: [{$gt: ["$nested.age", NumberInt(5)]}, {$gt: ["$age", NumberInt(5)]}]}
            }
        }],
        true,
        [
            ["0", "$match", "$expr", "$and", "0", "$and", "0", "$gt", "1", "$const"],
            ["0", "$match", "$expr", "$and", "0", "$and", "1", "$lte", "1", "$const"],
            ["0", "$match", "$expr", "$and", "1", "$and", "0", "$gt", "1", "$const"],
            ["0", "$match", "$expr", "$and", "1", "$and", "1", "$lte", "1", "$const"]
        ],
    ],  // 3
    // $match with $expr. Closed intervals.
    [
        [{
            $match: {
                $expr: {
                    $and: [
                        {$gt: ["$nested.age", NumberInt(5)]},
                        {$lt: ["$nested.age", NumberInt(10)]}
                    ]
                }
            }
        }],
        true,
        [
            ["0", "$match", "$expr", "$and", "0", "$and", "0", "$gt", "1", "$const"],
            ["0", "$match", "$expr", "$and", "0", "$and", "1", "$lt", "1", "$const"]
        ]
    ],  // 4
    [
        [{
            $match: {
                $expr: {
                    $and: [
                        {$gt: ["$age", NumberInt(12)]},
                        {$lte: ["$age", NumberInt(15)]},
                        {$gt: ["$nested.age", NumberInt(2)]},
                        {$lte: ["$nested.age", NumberInt(25)]}
                    ]
                }
            }
        }],
        true,
        [
            ["0", "$match", "$expr", "$and", "0", "$and", "0", "$gt", "1", "$const"],
            ["0", "$match", "$expr", "$and", "0", "$and", "1", "$lte", "1", "$const"],
            ["0", "$match", "$expr", "$and", "1", "$and", "0", "$gt", "1", "$const"],
            ["0", "$match", "$expr", "$and", "1", "$and", "1", "$lte", "1", "$const"]
        ]
    ],  // 5
    // $match with both range and equality index.
    [
        [{
            $match:
                {$expr: {$and: [{$gt: ["$age", NumberInt(5)]}, {$eq: ["$ssn", "randomString"]}]}}
        }],
        true,
        [
            ["0", "$match", "$expr", "$and", "1", "$and", "0", "$gt", "1", "$const"],
            ["0", "$match", "$expr", "$and", "1", "$and", "1", "$lte", "1", "$const"],
            ["0", "$match", "$expr", "$and", "0", "$eq", "1", "$const"]
        ]
    ],  // 6
    // $and with multiple types of children.
    [
        [{
            $match: {
                $expr: {
                    $and: [
                        {$gt: ["$age", NumberInt(5)]},
                        {$cond: [{$lt: ["$age", NumberInt(25)]}, false, true]}
                    ]
                }
            }
        }],
        true,
        [
            ["0", "$match", "$expr", "$and", "1", "$and", "0", "$gt", "1", "$const"],
            ["0", "$match", "$expr", "$and", "1", "$and", "1", "$lte", "1", "$const"],
            ["0", "$match", "$expr", "$and", "0", "$cond", "0", "$and", "0", "$gte", "1", "$const"],
            ["0", "$match", "$expr", "$and", "0", "$cond", "0", "$and", "1", "$lt", "1", "$const"]
        ],
    ],  // 7
    // $match with $expr. eq.
    [
        [{$match: {$expr: {$and: [{$eq: ["$nested.age", NumberInt(10)]}]}}}],
        true,
        [
            ["0", "$match", "$expr", "$and", "0", "$and", "0", "$gte", "1", "$const"],
            ["0", "$match", "$expr", "$and", "0", "$and", "1", "$lte", "1", "$const"]
        ]
    ],  // 8
    // $match with $expr. eq.
    [
        [{
            $match: {
                $expr:
                    {$and: [{$eq: ["$nested.age", NumberInt(10)]}, {$eq: ["$age", NumberInt(30)]}]}
            }
        }],
        true,
        [
            ["0", "$match", "$expr", "$and", "0", "$and", "0", "$gte", "1", "$const"],
            ["0", "$match", "$expr", "$and", "0", "$and", "1", "$lte", "1", "$const"],
            ["0", "$match", "$expr", "$and", "1", "$and", "0", "$gte", "1", "$const"],
            ["0", "$match", "$expr", "$and", "1", "$and", "1", "$lte", "1", "$const"]
        ]
    ],  // 9
    [
        [{$match: {$expr: {$and: [{$gt: ["$date", ISODate("2020-01-01")]}]}}}],
        true,
        [
            ["0", "$match", "$expr", "$and", "0", "$and", "0", "$gt", "1", "$const"],
            ["0", "$match", "$expr", "$and", "0", "$and", "1", "$lte", "1", "$const"]
        ]
    ],  // 10
    // $match with $expr. Date closed intervals.
    [
        [{
            $match: {
                $expr: {
                    $and: [
                        {$gt: ["$date", ISODate("2001-01-01")]},
                        {$lt: ["$date", ISODate("2002-01-01")]}
                    ]
                }
            }
        }],
        true,
        [
            ["0", "$match", "$expr", "$and", "0", "$and", "0", "$gt", "1", "$const"],
            ["0", "$match", "$expr", "$and", "0", "$and", "1", "$lt", "1", "$const"]
        ]
    ],  // 11
    // Date $eq.
    [
        [{
            $match: {
                $expr: {$eq: ["$date", ISODate("2001-01-01")]},
            }
        }],
        true,
        [
            ["0", "$match", "$expr", "$and", "0", "$gte", "1", "$const"],
            ["0", "$match", "$expr", "$and", "1", "$lte", "1", "$const"]
        ]
    ],  // 12
    [
        [{
            $match: {
                $expr: {$cond: [{$lt: ["$date", ISODate("1999-09-09")]}, false, true]}

            }
        }],
        true,
        [
            ["0", "$match", "$expr", "$cond", "0", "$and", "0", "$gte", "1", "$const"],
            ["0", "$match", "$expr", "$cond", "0", "$and", "1", "$lt", "1", "$const"]
        ]
    ],  // 13
    [
        // This query matches no documents.
        [{
            $match: {
                $expr: {
                    $and: [
                        {$lte: ["$age", NumberInt(12)]},
                        {$gt: ["$age", NumberInt(15)]},
                    ]
                }
            }
        }],
        false
    ],  // 14
    [
        // This query matches no documents.
        [{
            $match: {
                $expr: {
                    $and: [
                        {$gt: ["$salary", NumberInt(20)]},
                        {$lt: ["$salary", NumberInt(200)]},
                        {$lte: ["$age", NumberInt(12)]},
                        {$gt: ["$age", NumberInt(15)]},
                    ]
                }
            }
        }],
        false
    ],  // 15
    [
        // This query matches no documents.
        [{
            $match: {
                $expr: {
                    $and: [
                        {$gt: ["$salary", NumberInt(20)]},
                        {$lte: ["$age", NumberInt(12)]},
                        {$lt: ["$salary", NumberInt(200)]},
                        {$gt: ["$age", NumberInt(15)]},
                    ]
                }
            }
        }],
        false
    ],  // 16
    [
        // This query has an or with one branch that is always false.
        [{
            $match: {
                $expr: {
                    $or: [
                        {
                            $and: [
                                {$lte: ["$age", NumberInt(12)]},
                                {$gt: ["$age", NumberInt(15)]},
                            ]
                        },
                        {
                            $and: [
                                {$lte: ["$age", NumberInt(20)]},
                                {$gt: ["$age", NumberInt(15)]},

                            ]
                        }

                    ],
                }
            }
        }],
        true,
        [
            ["0", "$match", "$expr", "$or", "1", "$and", "0", "$and", "1", "$lte", "1", "$const"],
            ["0", "$match", "$expr", "$or", "1", "$and", "0", "$and", "0", "$gt", "1", "$const"]
        ]
    ],  // 17

    // TODO SERVER-65296: Support encrypted fields below $project.
    // [
    //     [{
    //         $project:
    //             {newField: {$cond: {if: {$eq: ["$age", NumberInt(24)]}, then: true, else:
    //             false}}}
    //     }],
    //     true,
    //     [full,path, array]
    // ],  // 6
    // [
    //     [{
    //         $project: {
    //             newField: {
    //                 $switch: {
    //                     branches: [
    //                         {case: {$gt: ["$age", NumberInt(25)]}, then: 1},
    //                         {case: {$lt: ["$age", NumberInt(5)]}, then: 2}
    //                     ],
    //                     default: 5
    //                 }
    //             }
    //         }
    //     }],
    //     true,
    //     [full, path, array]
    // ],  // 7
];

for (let testNum = 0; testNum < cases.length; testNum++) {
    jsTestLog("Running test " + testNum);
    const testCase = cases[testNum];
    assertEncryptedFieldInResponse({
        pipeline: testCase[0],
        requiresEncryption: testCase[1],
        pathArray: testCase[2],
    });
}

mongocryptd.stop();
