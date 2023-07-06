/**
 * Basic set of tests to verify the response from mongocryptd for the explain command.
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

const basicEncryptSchema = generateSchema(
    {foo: {encrypt: {algorithm: kDeterministicAlgo, keyId: [UUID()], bsonType: "long"}}},
    "test.test");

let cmds = [
    {find: "test", filter: {foo: NumberLong(1)}},
    {count: "test", query: {foo: NumberLong(1)}},
    {findAndModify: "test", query: {foo: NumberLong(1)}, update: {$inc: {score: 1.0}}},
    // old name
    {findandmodify: "test", query: {foo: NumberLong(1)}, update: {$inc: {score: 1.0}}},
    {aggregate: "test", pipeline: [{$match: {foo: NumberLong(1)}}], cursor: {}},
    {insert: "test", documents: [{foo: NumberLong(1)}]},
    {update: "test", updates: [{q: {foo: NumberLong(1)}, u: {"$set": {a: 2}}}]},
    {delete: "test", deletes: [{q: {foo: NumberLong(1)}, limit: 1}]},
];

// Distinct is only supported with FLE 1.
if (!fle2Enabled()) {
    cmds.push({distinct: "test", query: {foo: NumberLong(1)}, key: "foo"});
}

cmds.forEach(element => {
    // Make sure no json schema fails when explaining
    const explainBad = {explain: 1, explain: element};  // eslint-disable-line no-dupe-keys
    assert.commandFailed(testDB.runCommand(explainBad));

    const explainCmd =
        Object.assign({explain: element, verbosity: "executionStats"}, basicEncryptSchema);

    const explainRes = assert.commandWorked(testDB.runCommand(explainCmd));

    Object.extend(element, basicEncryptSchema);
    const normalRes = assert.commandWorked(testDB.runCommand(element));
    // Added by the shell.
    delete normalRes["result"]["lsid"];
    assert.eq(explainRes.hasEncryptionPlaceholders, normalRes.hasEncryptionPlaceholders);
    assert.eq(explainRes.schemaRequiresEncryption, normalRes.schemaRequiresEncryption);
    assert.eq(explainRes["result"]["explain"], normalRes["result"]);
    assert.eq(explainRes["result"]["verbosity"], "executionStats");

    // Test that an explain with the schema in the explained object fails.
    const misplacedSchema = Object.assign({
        explain: element,
    },
                                          basicEncryptSchema);
    assert.commandFailedWithCode(testDB.runCommand(misplacedSchema), [30050, 6365900]);
});

mongocryptd.stop();
