/**
 * Set of tests to verify the response from mongocryptd for the find command with a projection.
 */
(function() {
'use strict';

load("src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js");
load("src/mongo/db/modules/enterprise/jstests/fle/lib/utils.js");

const mongocryptd = new MongoCryptD();
mongocryptd.start();
const conn = mongocryptd.getConnection();
const collName = "test";
const testDB = conn.getDB(collName);

const schema = generateSchema({
    ssn: {encrypt: {algorithm: kDeterministicAlgo, keyId: [UUID()], bsonType: "long"}},
    "user.account": {encrypt: {algorithm: kDeterministicAlgo, keyId: [UUID()], bsonType: "string"}}
},
                              collName);

// TODO: SERVER-63311 Make sure tests commands work in both FLE versions once query analysis for
// aggregation is added.
if (!fle2Enabled()) {
    // Verify that just including a projection does not cause the analysis to report
    // that the output is encrypted.
    let res = assert.commandWorked(testDB.runCommand(Object.assign({
        find: collName,
        filter: {},
        projection: {importantMath: {$add: [NumberLong(3), NumberLong(4)]}},
    },
                                                                   schema)));
    assert(!res.hasEncryptionPlaceholders, tojson(res));

    // Verify that a non-equality comparison results in an error.
    assert.commandFailedWithCode(testDB.runCommand(Object.assign({
        find: collName,
        filter: {},
        projection: {isSsnFake: {$gt: ["$ssn", NumberLong(0)]}},
    },
                                                                 schema)),
                                 31110);

    // Verify projection without query correctly marks fields.
    res = assert.commandWorked(testDB.runCommand(Object.assign({
        find: collName,
        filter: {},
        projection: {isSsnFake: {$eq: ["$ssn", NumberLong(0)]}},
    },
                                                               schema)));
    assert(res.hasEncryptionPlaceholders, tojson(res));
    assert(res.result.projection["isSsnFake"]["$eq"][1]["$const"] instanceof BinData, tojson(res));

    // Verify projection with query correctly marks fields.
    res = assert.commandWorked(testDB.runCommand(Object.assign({
        find: collName,
        filter: {"user.account": "secret"},
        projection:
            {isSsnFake: {"$in": ["$ssn", [NumberLong(0), NumberLong(111111111), NumberLong(42)]]}},
    },
                                                               schema)));
    assert(res.hasEncryptionPlaceholders, tojson(res));
    assert(res.result.projection["isSsnFake"]["$in"][1][0]["$const"] instanceof BinData,
           tojson(res));
    assert(res.result.projection["isSsnFake"]["$in"][1][1]["$const"] instanceof BinData,
           tojson(res));
    assert(res.result.projection["isSsnFake"]["$in"][1][2]["$const"] instanceof BinData,
           tojson(res));
} else {
    // Verify that just including a projection does not cause the analysis to report
    // that the output is encrypted.
    assert.commandFailedWithCode(testDB.runCommand(Object.assign({
        find: collName,
        filter: {},
        projection: {importantMath: {$add: [NumberLong(3), NumberLong(4)]}},
    },
                                                                 schema)),
                                 6329201);

    // Verify that a non-equality comparison results in an error.
    assert.commandFailedWithCode(testDB.runCommand(Object.assign({
        find: collName,
        filter: {},
        projection: {isSsnFake: {$gt: ["$ssn", NumberLong(0)]}},
    },
                                                                 schema)),
                                 6329201);

    // Verify projection without query correctly marks fields.
    assert.commandFailedWithCode(testDB.runCommand(Object.assign({
        find: collName,
        filter: {},
        projection: {isSsnFake: {$eq: ["$ssn", NumberLong(0)]}},
    },
                                                                 schema)),
                                 6329201);

    // Verify projection with query correctly marks fields.
    assert.commandFailedWithCode(testDB.runCommand(Object.assign({
        find: collName,
        filter: {"user.account": "secret"},
        projection:
            {isSsnFake: {"$in": ["$ssn", [NumberLong(0), NumberLong(111111111), NumberLong(42)]]}},
    },
                                                                 schema)),
                                 6329201);
}

mongocryptd.stop();
})();
