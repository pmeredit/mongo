/**
 * Set of tests to verify the response from mongocryptd for the find command with a projection.
 */
import {MongoCryptD} from "src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js";
import {
    fle2Enabled,
    generateSchema
} from "src/mongo/db/modules/enterprise/jstests/fle/lib/utils.js";
import {kDeterministicAlgo} from "src/mongo/db/modules/enterprise/jstests/fle/lib/utils.js";

const mongocryptd = new MongoCryptD();
mongocryptd.start();
const conn = mongocryptd.getConnection();
const testDB = conn.getDB("test");
const collName = "test";

const schema = generateSchema({
    ssn: {encrypt: {algorithm: kDeterministicAlgo, keyId: [UUID()], bsonType: "long"}},
    "user.account": {encrypt: {algorithm: kDeterministicAlgo, keyId: [UUID()], bsonType: "string"}}
},
                              "test.test");

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
                             [31110, 6331102]);

// TODO SERVER-65296 Support comparisons to encrypted fields under project.
if (fle2Enabled()) {
    // Referring to an encrypted field in the projection spec is not allowed.
    assert.commandFailedWithCode(testDB.runCommand(Object.assign({
        find: collName,
        filter: {},
        projection: {isSsnFake: {$eq: ["$ssn", NumberLong(0)]}},
    },
                                                                 schema)),
                                 6331102);

    assert.commandFailedWithCode(testDB.runCommand(Object.assign({
        find: collName,
        filter: {"user.account": "secret"},
        projection:
            {isSsnFake: {"$in": ["$ssn", [NumberLong(0), NumberLong(111111111), NumberLong(42)]]}},
    },
                                                                 schema)),
                                 6331102);

    //  Cannot reference a prefix of an encrypted field in a computed projection.
    assert.commandFailedWithCode(testDB.runCommand(Object.assign({
        find: collName,
        filter: {"user.account": "secret"},
        projection: {isUserFake: {$eq: ["$user", {fake: true}]}}
    },
                                                                 schema)),
                                 31129);
} else {
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
}

// A computed projection which does not reference any encrypted fields is allowed.
res = assert.commandWorked(testDB.runCommand(Object.assign({
    find: collName,
    filter: {},
    projection: {notEncrypted: {$eq: ["$name", "plain"]}},
},
                                                           schema)));
assert.eq(false, res.hasEncryptionPlaceholders, tojson(res));

// An inclusion projection of encrypted fields is allowed.
res = assert.commandWorked(testDB.runCommand(Object.assign({
    find: collName,
    filter: {},
    projection: {ssn: 1},
},
                                                           schema)));
assert.eq(false, res.hasEncryptionPlaceholders, tojson(res));

mongocryptd.stop();
