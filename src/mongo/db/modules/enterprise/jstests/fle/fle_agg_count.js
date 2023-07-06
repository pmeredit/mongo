/**
 * Test that mongocryptd can correctly mark the $count agg stage with intent-to-encrypt
 * placeholders.
 */
import {MongoCryptD} from "src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js";
import {generateSchema} from "src/mongo/db/modules/enterprise/jstests/fle/lib/utils.js";
import {kDeterministicAlgo} from "src/mongo/db/modules/enterprise/jstests/fle/lib/utils.js";

const mongocryptd = new MongoCryptD();
mongocryptd.start();
const conn = mongocryptd.getConnection();
const testDB = conn.getDB("test");
const coll = testDB.fle_agg_count;

const fooEncrypted = generateSchema(
    {foo: {encrypt: {algorithm: kDeterministicAlgo, keyId: [UUID()], bsonType: "string"}}},
    coll.getFullName());

let command, cmdRes, expectedResult;

// Test that $count marks the projected field as unencrypted even if it overrides an encrypted
// field.
command = Object.assign({
    aggregate: coll.getName(),
    pipeline: [{$count: "foo"}, {$match: {foo: {$eq: "winterfell"}}}],
    cursor: {}
},
                        fooEncrypted);
cmdRes = assert.commandWorked(testDB.runCommand(command));
expectedResult = {
    aggregate: coll.getName(),
    pipeline: [
        {$group: {"_id": {"$const": null}, "foo": {"$sum": {"$const": 1}}}},
        {$project: {"foo": true, "_id": false}},
        {$match: {foo: {$eq: "winterfell"}}}
    ],
    cursor: {}
};
delete cmdRes.result.lsid;
delete cmdRes.result.encryptionInformation;
assert.eq(expectedResult, cmdRes.result, cmdRes);
assert.eq(false, cmdRes.hasEncryptionPlaceholders, cmdRes);
assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);

// Test that $count marks a previously encrypted field as not encrypted even if it was not
// projected by this stage.
command.pipeline = [{$count: "bar"}, {$match: {foo: {$eq: "winterfell"}}}];
cmdRes = assert.commandWorked(testDB.runCommand(command));
expectedResult = {
    aggregate: coll.getName(),
    pipeline: [
        {$group: {"_id": {"$const": null}, "bar": {"$sum": {"$const": 1}}}},
        {$project: {"bar": true, "_id": false}},
        {$match: {foo: {$eq: "winterfell"}}}
    ],
    cursor: {}
};
delete cmdRes.result.lsid;
delete cmdRes.result.encryptionInformation;
assert.eq(expectedResult, cmdRes.result, cmdRes);
assert.eq(false, cmdRes.hasEncryptionPlaceholders, cmdRes);
assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);

mongocryptd.stop();
