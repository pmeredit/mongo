/**
 * Test that mongocryptd can correctly mark the $sortByCount agg stage with intent-to-encrypt
 * placeholders.
 */
import {MongoCryptD} from "src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js";
import {
    fle2Enabled,
    generateSchema
} from "src/mongo/db/modules/enterprise/jstests/fle/lib/utils.js";
import {
    kDeterministicAlgo,
    kRandomAlgo
} from "src/mongo/db/modules/enterprise/jstests/fle/lib/utils.js";

const mongocryptd = new MongoCryptD();
mongocryptd.start();
const conn = mongocryptd.getConnection();
const testDB = conn.getDB("test");
const coll = testDB.fle_agg_sort_by_count;

const encryptedStringSpec = {
    encrypt: {algorithm: kDeterministicAlgo, keyId: [UUID()], bsonType: "string"}
};

const encryptedRandomSpec = {
    encrypt: {algorithm: kRandomAlgo, keyId: [UUID()], bsonType: "string"}
};

const encryptedQtySpec = generateSchema({qty: encryptedStringSpec}, coll.getFullName());

let command, cmdRes, expectedResult;

// Test that $sortByCount marks the projected fields '_id' and 'count' as not encrypted even if
// they override encrypted fields.
command = Object.assign(
    {
        aggregate: coll.getName(),
        pipeline: [
            {$sortByCount: "$foo"},
            {$match: {$and: [{_id: {$eq: "winterfell"}}, {count: {$eq: "winterfell"}}]}}
        ],
        cursor: {}
    },
    generateSchema({_id: encryptedStringSpec, count: encryptedStringSpec}, coll.getFullName()));

// In FLE 2, _id is not allowed to be marked for encryption.
if (fle2Enabled()) {
    assert.commandFailedWithCode(testDB.runCommand(command), 6316403);
} else {
    cmdRes = assert.commandWorked(testDB.runCommand(command));
    expectedResult = {
        aggregate: coll.getName(),
        pipeline: [
            {$group: {_id: "$foo", count: {"$sum": {"$const": 1}}}},
            {$sort: {count: -1}},
            {$match: {$and: [{_id: {$eq: "winterfell"}}, {count: {$eq: "winterfell"}}]}}
        ],
        cursor: {}
    };
    delete cmdRes.result.lsid;
    delete cmdRes.result.pipeline[0].$group.$willBeMerged;
    assert.eq(expectedResult, cmdRes.result, cmdRes);
    assert.eq(false, cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);
}

// Similar to the test above except only 'count' is encrypted.
command = Object.assign({
    aggregate: coll.getName(),
    pipeline: [{$sortByCount: "$foo"}, {$match: {count: {$eq: "winterfell"}}}],
    cursor: {}
},
                        generateSchema({count: encryptedStringSpec}, coll.getFullName()));
cmdRes = assert.commandWorked(testDB.runCommand(command));
expectedResult = {
    aggregate: coll.getName(),
    pipeline: [
        {$group: {_id: "$foo", count: {"$sum": {"$const": 1}}}},
        {$sort: {count: -1}},
        {$match: {count: {$eq: "winterfell"}}}
    ],
    cursor: {}
};
delete cmdRes.result.lsid;
delete cmdRes.result.encryptionInformation;
delete cmdRes.result.pipeline[0].$group.$willBeMerged;
assert.eq(expectedResult, cmdRes.result, cmdRes);
assert.eq(false, cmdRes.hasEncryptionPlaceholders, cmdRes);
assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);

// Test that $sortByCount specified with an expression has constants correctly marked for
// encryption.
command = Object.assign({
    aggregate: coll.getName(),
    pipeline: [{$sortByCount: {$cond: [{$eq: ["$qty", {$const: "thousand"}]}, "1000", "not1000"]}}],
    cursor: {}
},
                        encryptedQtySpec);
if (fle2Enabled()) {
    assert.commandFailedWithCode(testDB.runCommand(command), 6331102);
} else {
    cmdRes = assert.commandWorked(testDB.runCommand(command));
    assert.eq(true, cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);
    assert(cmdRes.hasOwnProperty("result"), cmdRes);
    assert.eq(coll.getName(), cmdRes.result.aggregate, cmdRes);
    assert(cmdRes.result.pipeline[0].$group._id.$cond[0].$eq[1]["$const"] instanceof BinData,
           cmdRes);
}

// Test that $sortByCount specified with an expression requires a stable output type across
// documents to allow for comparisons.
command = Object.assign({
    aggregate: coll.getName(),
    pipeline:
        [{$sortByCount: {$cond: [{$eq: ["$qty", {$const: "thousand"}]}, "$qtyOther", "$qty"]}}],
    cursor: {}
},
                        encryptedQtySpec);
assert.commandFailedWithCode(testDB.runCommand(command), 51222);

// Test that $sortByCount succeeds if it is specified with a deterministically encrypted
// field.
command = Object.assign({
    aggregate: coll.getName(),
    pipeline: [{$sortByCount: "$qty"}, {$match: {_id: {$eq: "winterfell"}}}],
    cursor: {}
},
                        encryptedQtySpec);
// FLE 2 has only limited support for references to encrypted fields in aggregate expressions.
if (fle2Enabled()) {
    assert.commandFailedWithCode(testDB.runCommand(command), 51222);
} else {
    cmdRes = assert.commandWorked(testDB.runCommand(command));
    assert.eq(true, cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);
    assert(cmdRes.hasOwnProperty("result"), cmdRes);
    assert.eq(coll.getName(), cmdRes.result.aggregate, cmdRes);
    assert(cmdRes.result.pipeline[2].$match["_id"].$eq instanceof BinData, cmdRes);
}

// Test that $sortByCount fails if it is specified with a prefix of a deterministically
// encrypted field.
command = Object.assign({aggregate: coll.getName(), pipeline: [{$sortByCount: "$foo"}], cursor: {}},
                        generateSchema({'foo.bar': encryptedStringSpec}, coll.getFullName()));
assert.commandFailedWithCode(testDB.runCommand(command), 31129);

// Test that $sortByCount fails if it is specified with a path with an encrypted prefix.
command =
    Object.assign({aggregate: coll.getName(), pipeline: [{$sortByCount: "$qty.bar"}], cursor: {}},
                  encryptedQtySpec);
assert.commandFailedWithCode(testDB.runCommand(command), 51102);

// Test that $sortByCount fails if it is specified with a field encrypted with the random
// algorithm.
command = Object.assign({aggregate: coll.getName(), pipeline: [{$sortByCount: "$foo"}], cursor: {}},
                        generateSchema({foo: encryptedRandomSpec}, coll.getFullName()));
assert.commandFailedWithCode(testDB.runCommand(command), 51222);

mongocryptd.stop();
