/**
 * Test that mongocryptd can correctly mark the $lookup agg stage with intent-to-encrypt
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
const coll = testDB.fle_agg_lookup;

const encryptedStringSpec = () =>
    ({encrypt: {algorithm: kDeterministicAlgo, keyId: [UUID()], bsonType: "string"}});

const encryptedIntSpec = () =>
    ({encrypt: {algorithm: kDeterministicAlgo, keyId: [UUID()], bsonType: "int"}});

const encryptedRandomSpec = () =>
    ({encrypt: {algorithm: kRandomAlgo, keyId: [UUID()], bsonType: "string"}});

let command, cmdRes;

const emptySchema = generateSchema({}, coll.getFullName());

// Test that self-lookup with unencrypted equality match fields and bringing unencrypted
// fields succeeds.
command = Object.assign({
    aggregate: coll.getName(),
    pipeline:
        [{$lookup: {from: coll.getName(), as: "docs", localField: "item", foreignField: "sku"}}],
    cursor: {}
},
                        emptySchema);
cmdRes = assert.commandWorked(testDB.runCommand(command));
delete command.jsonSchema;
delete command.isRemoteSchema;
delete cmdRes.result.lsid;
assert.eq(command, cmdRes.result, cmdRes);
assert.eq(false, cmdRes.hasEncryptionPlaceholders, cmdRes);
assert.eq(false, cmdRes.schemaRequiresEncryption, cmdRes);

// Test that self-lookup bringing unencrypted fields from a subpipeline succeeds.
command = Object.assign({
    aggregate: coll.getName(),
    pipeline: [{
        $lookup:
            {from: coll.getName(), as: "docs", let : {}, pipeline: [{$match: {name: {$eq: "bob"}}}]}
    }],
    cursor: {}
},
                        emptySchema);
cmdRes = assert.commandWorked(testDB.runCommand(command));
delete command.jsonSchema;
delete command.isRemoteSchema;
delete cmdRes.result.lsid;
assert.eq(command, cmdRes.result, cmdRes);
assert.eq(false, cmdRes.hasEncryptionPlaceholders, cmdRes);
assert.eq(false, cmdRes.schemaRequiresEncryption, cmdRes);

if (!fle2Enabled()) {
    const spec = encryptedStringSpec();
    const itemAndSkuEncrypted = generateSchema({item: spec, sku: spec}, coll.getFullName());

    // Test that self-lookup with equality match fields encrypted with a deterministic algorithm
    // and matching bsonTypes succeeds.
    command = Object.assign({
        aggregate: coll.getName(),
        pipeline: [
            {$lookup: {from: coll.getName(), as: "docs", localField: "item", foreignField: "sku"}}
        ],
        cursor: {}
    },
                            itemAndSkuEncrypted);
    cmdRes = assert.commandWorked(testDB.runCommand(command));
    delete command.jsonSchema;
    delete command.isRemoteSchema;
    delete cmdRes.result.lsid;
    assert.eq(command, cmdRes.result, cmdRes);
    assert.eq(false, cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);

    // Test that referencing 'as' field from $lookup in subsequent stages fails.
    assert.commandFailedWithCode(testDB.runCommand(Object.assign({
        aggregate: coll.getName(),
        pipeline: [
            {$lookup: {from: coll.getName(), as: "docs", localField: "item", foreignField: "sku"}},
            {$match: {"docs": {$gt: "winterfell"}}}
        ],
        cursor: {}
    },
                                                                 itemAndSkuEncrypted)),
                                 [31133, 6331103]);

    // Test that self-lookup with encrypted equality match fields bringing encrypted fields from a
    // subpipeline succeeds.
    command = Object.assign({
        aggregate: coll.getName(),
        pipeline: [
            {
            $lookup: {
                from: coll.getName(),
                as: "docs",
                localField: "item",
                foreignField: "sku",
                let : {},
                pipeline: [{$match: {bob: {$eq: 1}}}]
            }
            }
        ],
        cursor: {}}, itemAndSkuEncrypted);
    cmdRes = assert.commandWorked(testDB.runCommand(command));
    delete command.jsonSchema;
    delete command.isRemoteSchema;
    delete cmdRes.result.lsid;
    assert.eq(command, cmdRes.result, cmdRes);
    assert.eq(false, cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);

    // Test that self-lookup with encrypted equality match fields bringing unencrypted fields from a
    // subpipeline succeeds and the 'as' field can be referenced afterwards.
    command = Object.assign({
        aggregate: coll.getName(),
        pipeline: [
            {
            $lookup: {
                from: coll.getName(),
                as: "docs",
                localField: "item",
                foreignField: "sku",
                let : {},
                pipeline: [{$project: {_id: true, notEncrypted: true}}]
            }
            },
            {$match: {docs: {$eq: "winterfell"}}}
        ],
        cursor: {}}, itemAndSkuEncrypted);
    cmdRes = assert.commandWorked(testDB.runCommand(command));
    delete command.jsonSchema;
    delete command.isRemoteSchema;
    delete cmdRes.result.lsid;
    assert.eq(command, cmdRes.result, cmdRes);
    assert.eq(false, cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);
}

const fooEncryptedSchema = generateSchema({foo: encryptedStringSpec()}, coll.getFullName());

// Test that self-lookup over an encrypted collection bringing encrypted fields from a
// subpipeline succeeds as long as they are not referenced afterwards. If encrypted arrays
// are supported, subsequent references will be possible as well.
command = Object.assign({
    aggregate: coll.getName(),
    pipeline: [{
        $lookup: {from: coll.getName(), as: "docs", let : {}, pipeline: [{$match: {bob: {$eq: 1}}}]}
    }],
    cursor: {}
},
                        fooEncryptedSchema);
cmdRes = assert.commandWorked(testDB.runCommand(command));
delete command.jsonSchema;
delete command.isRemoteSchema;
delete cmdRes.result.lsid;
assert.eq(command, cmdRes.result, cmdRes);
assert.eq(false, cmdRes.hasEncryptionPlaceholders, cmdRes);
assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);

// Test that self-lookup over an encrypted collection bringing unencrypted fields from a
// subpipeline succeeds and the 'as' field can be referenced afterwards.
command = Object.assign({
        aggregate: coll.getName(),
        pipeline: [
            {
              $lookup: {
                  from: coll.getName(),
                  as: "docs",
                  let : {},
                  pipeline: [{$project: {_id: true, notEncrypted: true}}]
              }
            },
            {$match: {docs: {$eq: "winterfell"}}}
        ],
        cursor: {}}, fooEncryptedSchema);
cmdRes = assert.commandWorked(testDB.runCommand(command));
delete command.jsonSchema;
delete command.isRemoteSchema;
delete cmdRes.result.lsid;
assert.eq(command, cmdRes.result, cmdRes);
assert.eq(false, cmdRes.hasEncryptionPlaceholders, cmdRes);
assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);

// Test that not self-lookup specified with an equality match fails.
assert.commandFailedWithCode(testDB.runCommand(Object.assign({
    aggregate: coll.getName(),
    pipeline: [{$lookup: {from: "foo", as: "docs", localField: "item", foreignField: "sku"}}],
    cursor: {}
},
                                                             fooEncryptedSchema)),
                             [9710000]);

// Test that not self-lookup specified with a pipeline fails.
assert.commandFailedWithCode(testDB.runCommand(Object.assign({
    aggregate: coll.getName(),
    pipeline: [{$lookup: {from: "foo", as: "docs", let : {}, pipeline: [{$match: {name: "bob"}}]}}],
    cursor: {}
},
                                                             fooEncryptedSchema)),
                             [9710000]);

// Test that self-lookup when 'localField' has an encrypted child fails.
assert.commandFailedWithCode(
    testDB.runCommand(Object.assign({
        aggregate: coll.getName(),
        pipeline: [{
            $lookup: {from: coll.getName(), as: "docs", localField: "category", foreignField: "cat"}
        }],
        cursor: {}
    },
                                    generateSchema({'category.group': encryptedStringSpec()},
                                                   coll.getFullName()))),
    51206);

// Test that self-lookup when 'foreignField' has an encrypted child fails.
assert.commandFailedWithCode(
    testDB.runCommand(Object.assign({
        aggregate: coll.getName(),
        pipeline: [{
            $lookup: {from: coll.getName(), as: "docs", localField: "cat", foreignField: "category"}
        }],
        cursor: {}
    },
                                    generateSchema({'category.group': encryptedStringSpec()},
                                                   coll.getFullName()))),
    51207);

// Test that self-lookup with unencrypted 'localField' and encrypted 'foreignField' fails.
assert.commandFailedWithCode(testDB.runCommand(Object.assign({
    aggregate: coll.getName(),
    pipeline:
        [{$lookup: {from: coll.getName(), as: "docs", localField: "bar", foreignField: "foo"}}],
    cursor: {}
},
                                                             fooEncryptedSchema)),
                             [51210, 6331103]);

// Test that self-lookup with encrypted 'localField' and unencrypted 'foreignField' fails.
assert.commandFailedWithCode(testDB.runCommand(Object.assign({
    aggregate: coll.getName(),
    pipeline:
        [{$lookup: {from: coll.getName(), as: "docs", localField: "foo", foreignField: "bar"}}],
    cursor: {}
},
                                                             fooEncryptedSchema)),
                             [51210, 6331103]);

// Test that self-lookup with encrypted 'localField' and 'foreignField' with different bsonTypes
// fails.
assert.commandFailedWithCode(
    testDB.runCommand(Object.assign({
        aggregate: coll.getName(),
        pipeline:
            [{$lookup: {from: coll.getName(), as: "docs", localField: "foo", foreignField: "bar"}}],
        cursor: {}
    },
                                    generateSchema(
                                        {foo: encryptedStringSpec(), bar: encryptedIntSpec()},
                                        coll.getFullName()))),
    [51210, 6331103]);

// Test that self-lookup with encrypted 'localField' and 'foreignField' with a random algorithm
// fails.
if (!fle2Enabled()) {
    const spec = encryptedRandomSpec();
    assert.commandFailedWithCode(testDB.runCommand(Object.assign({
        aggregate: coll.getName(),
        pipeline:
            [{$lookup: {from: coll.getName(), as: "docs", localField: "foo", foreignField: "bar"}}],
        cursor: {}
    },
                                                                 generateSchema(
                                                                     {foo: spec, bar: spec},
                                                                     coll.getFullName()))),
                                 [51211, 6331103]);
}

// Test that self-lookup specified with a non-empty 'let' field fails.
assert.commandFailedWithCode(testDB.runCommand(Object.assign({
        aggregate: coll.getName(),
        pipeline: [{
            $lookup: {
                from: coll.getName(),
                as: "docs",
                let : {item: "$parent"},
                pipeline: [{$match: {name: "bob"}}]
            }
        }],
        cursor: {}}, fooEncryptedSchema)),
                                 51208);

// Test that self-lookup with unencrypted equality match fields and bringing unencrypted fields from
// a subpipeline succeeds.
command = Object.assign({
    aggregate: coll.getName(),
    pipeline: [
        {$lookup: {from: coll.getName(), as: "docs", localField: "item", foreignField: "sku",
                    let : {}, pipeline: [{$match: {name: {$eq: "bob"}}}]}}
    ],
    cursor: {}}, emptySchema);
cmdRes = assert.commandWorked(testDB.runCommand(command));
delete command.jsonSchema;
delete command.isRemoteSchema;
delete cmdRes.result.lsid;
assert.eq(command, cmdRes.result, cmdRes);
assert.eq(false, cmdRes.hasEncryptionPlaceholders, cmdRes);
assert.eq(false, cmdRes.schemaRequiresEncryption, cmdRes);

// Ensure that we are marking constants for encryption inside $lookup subpipelines.
command = Object.assign({
    aggregate: coll.getName(),
    pipeline: [
        {$match: {foo: "1"}},
        {$lookup: {from: coll.getName(), as: "docs", pipeline: [{$match: {foo: "1"}}]}}
    ],
    cursor: {}
},
                        fooEncryptedSchema);
cmdRes = assert.commandWorked(testDB.runCommand(command));
assert(cmdRes.result.pipeline[0].$match.foo.$eq instanceof BinData, cmdRes);
assert(cmdRes.result.pipeline[1].$lookup.pipeline[0].$match.foo.$eq instanceof BinData, cmdRes);

mongocryptd.stop();
