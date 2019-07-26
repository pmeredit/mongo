/**
 * Test that mongocryptd can correctly mark the $lookup agg stage with intent-to-encrypt
 * placeholders.
 */
(function() {
"use strict";

load("src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js");

const mongocryptd = new MongoCryptD();
mongocryptd.start();
const conn = mongocryptd.getConnection();
const testDB = conn.getDB("test");
const coll = testDB.fle_agg_lookup;

const encryptedStringSpec = {
    encrypt: {
        algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
        keyId: [UUID()],
        bsonType: "string"
    }
};

const encryptedIntSpec = {
    encrypt:
        {algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic", keyId: [UUID()], bsonType: "int"}
};

const encryptedRandomSpec = {
    encrypt: {algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random", keyId: [UUID()]}
};

let command, cmdRes;

// Test that self-lookup with unencrypted equality match fields and bringing unencrypted
// fields succeeds.
command = {
    aggregate: coll.getName(),
    pipeline:
        [{$lookup: {from: coll.getName(), as: "docs", localField: "item", foreignField: "sku"}}],
    cursor: {},
    jsonSchema: {},
    isRemoteSchema: false,
};
cmdRes = assert.commandWorked(testDB.runCommand(command));
delete command.jsonSchema;
delete command.isRemoteSchema;
delete cmdRes.result.lsid;
assert.eq(command, cmdRes.result, cmdRes);
assert.eq(false, cmdRes.hasEncryptionPlaceholders, cmdRes);
assert.eq(false, cmdRes.schemaRequiresEncryption, cmdRes);

// Test that self-lookup bringing unencrypted fields from a subpipeline succeeds.
command = {
    aggregate: coll.getName(),
    pipeline: [
        {$lookup: {from: coll.getName(), as: "docs", let : {}, pipeline: [{$match: {name: "bob"}}]}}
    ],
    cursor: {},
    jsonSchema: {},
    isRemoteSchema: false,
};
cmdRes = assert.commandWorked(testDB.runCommand(command));
delete command.jsonSchema;
delete command.isRemoteSchema;
delete cmdRes.result.lsid;
assert.eq(command, cmdRes.result, cmdRes);
assert.eq(false, cmdRes.hasEncryptionPlaceholders, cmdRes);
assert.eq(false, cmdRes.schemaRequiresEncryption, cmdRes);

// Test that self-lookup with equality match fields encrypted with a deterministic algorithm
// and matching bsonTypes succeeds.
command = {
    aggregate: coll.getName(),
    pipeline:
        [{$lookup: {from: coll.getName(), as: "docs", localField: "item", foreignField: "sku"}}],
    cursor: {},
    jsonSchema: {type: "object", properties: {item: encryptedStringSpec, sku: encryptedStringSpec}},
    isRemoteSchema: false,
};
cmdRes = assert.commandWorked(testDB.runCommand(command));
delete command.jsonSchema;
delete command.isRemoteSchema;
delete cmdRes.result.lsid;
assert.eq(command, cmdRes.result, cmdRes);
assert.eq(false, cmdRes.hasEncryptionPlaceholders, cmdRes);
assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);

// Test that referencing 'as' field from $graphLookup in subsequent stages fails.
assert.commandFailedWithCode(testDB.runCommand({
    aggregate: coll.getName(),
    pipeline: [
        {$lookup: {from: coll.getName(), as: "docs", localField: "item", foreignField: "sku"}},
        {$match: {"docs": {$gt: "winterfell"}}}
    ],
    cursor: {},
    jsonSchema: {type: "object", properties: {}, additionalProperties: encryptedStringSpec},
    isRemoteSchema: false
}),
                             31133);

// Test that self-lookup over an encrypted collection bringing encrypted fields from a
// subpipeline succeeds as long as they are not referenced afterwards. If encrypted arrays
// are supported, subsequent references will be possible as well.
command = {
    aggregate: coll.getName(),
    pipeline:
        [{$lookup: {from: coll.getName(), as: "docs", let : {}, pipeline: [{$match: {bob: 1}}]}}],
    cursor: {},
    jsonSchema: {type: "object", properties: {foo: encryptedStringSpec}},
    isRemoteSchema: false,
};
cmdRes = assert.commandWorked(testDB.runCommand(command));
delete command.jsonSchema;
delete command.isRemoteSchema;
delete cmdRes.result.lsid;
assert.eq(command, cmdRes.result, cmdRes);
assert.eq(false, cmdRes.hasEncryptionPlaceholders, cmdRes);
assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);

// Test that self-lookup over an encrypted collection bringing unencrypted fields from a
// subpipeline succeeds and the 'as' field can be referenced afterwards.
command = {
        aggregate: coll.getName(),
        pipeline: [
            {
              $lookup: {
                  from: coll.getName(),
                  as: "docs",
                  let : {},
                  pipeline: [{$project: {notEncrypted: 1}}]
              }
            },
            {$match: {docs: {$eq: "winterfell"}}}
        ],
        cursor: {},
        jsonSchema: {type: "object", properties: {foo: encryptedStringSpec}},
        isRemoteSchema: false,
    };
cmdRes = assert.commandWorked(testDB.runCommand(command));
delete command.jsonSchema;
delete command.isRemoteSchema;
delete cmdRes.result.lsid;
assert.eq(command, cmdRes.result, cmdRes);
assert.eq(false, cmdRes.hasEncryptionPlaceholders, cmdRes);
assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);

// Test that not self-lookup specified with an equality match fails.
assert.commandFailedWithCode(testDB.runCommand({
    aggregate: coll.getName(),
    pipeline: [{$lookup: {from: "foo", as: "docs", localField: "item", foreignField: "sku"}}],
    cursor: {},
    jsonSchema: {},
    isRemoteSchema: false,
}),
                             51204);

// Test that not self-lookup specified with a pipeline fails.
assert.commandFailedWithCode(testDB.runCommand({
    aggregate: coll.getName(),
    pipeline: [{$lookup: {from: "foo", as: "docs", let : {}, pipeline: [{$match: {name: "bob"}}]}}],
    cursor: {},
    jsonSchema: {},
    isRemoteSchema: false,
}),
                             51204);

// Test that self-lookup when 'localField' has an encrypted child fails.
assert.commandFailedWithCode(testDB.runCommand({
    aggregate: coll.getName(),
    pipeline: [
        {$lookup: {from: coll.getName(), as: "docs", localField: "category", foreignField: "cat"}}
    ],
    cursor: {},
    jsonSchema: {
        type: "object",
        properties: {category: {type: "object", properties: {group: encryptedStringSpec}}}
    },
    isRemoteSchema: false,
}),
                             51206);

// Test that self-lookup when 'foreignField' has an encrypted child fails.
assert.commandFailedWithCode(testDB.runCommand({
    aggregate: coll.getName(),
    pipeline: [
        {$lookup: {from: coll.getName(), as: "docs", localField: "cat", foreignField: "category"}}
    ],
    cursor: {},
    jsonSchema: {
        type: "object",
        properties: {category: {type: "object", properties: {group: encryptedStringSpec}}}
    },
    isRemoteSchema: false,
}),
                             51207);

// Test that self-lookup with unencrypted 'localField' and encrypted 'foreignField' fails.
assert.commandFailedWithCode(testDB.runCommand({
    aggregate: coll.getName(),
    pipeline:
        [{$lookup: {from: coll.getName(), as: "docs", localField: "bar", foreignField: "foo"}}],
    cursor: {},
    jsonSchema: {type: "object", properties: {foo: encryptedStringSpec}},
    isRemoteSchema: false,
}),
                             51210);

// Test that self-lookup with encrypted 'localField' and unencrypted 'foreignField' fails.
assert.commandFailedWithCode(testDB.runCommand({
    aggregate: coll.getName(),
    pipeline:
        [{$lookup: {from: coll.getName(), as: "docs", localField: "foo", foreignField: "bar"}}],
    cursor: {},
    jsonSchema: {type: "object", properties: {foo: encryptedStringSpec}},
    isRemoteSchema: false,
}),
                             51210);

// Test that self-lookup with encrypted 'localField' and 'foreignField' with different bsonTypes
// fails.
assert.commandFailedWithCode(testDB.runCommand({
    aggregate: coll.getName(),
    pipeline:
        [{$lookup: {from: coll.getName(), as: "docs", localField: "foo", foreignField: "bar"}}],
    cursor: {},
    jsonSchema: {type: "object", properties: {foo: encryptedStringSpec, bar: encryptedIntSpec}},
    isRemoteSchema: false,
}),
                             51210);

// Test that self-lookup with encrypted 'localField' and 'foreignField' with a random algorithm
// fails.
assert.commandFailedWithCode(testDB.runCommand({
    aggregate: coll.getName(),
    pipeline:
        [{$lookup: {from: coll.getName(), as: "docs", localField: "foo", foreignField: "bar"}}],
    cursor: {},
    jsonSchema: {type: "object", properties: {foo: encryptedRandomSpec, bar: encryptedRandomSpec}},
    isRemoteSchema: false,
}),
                             51211);

// Test that self-lookup specified with a non-empty 'let' field fails.
assert.commandFailedWithCode(testDB.runCommand({
        aggregate: coll.getName(),
        pipeline: [{
            $lookup: {
                from: coll.getName(),
                as: "docs",
                let : {item: "$parent"},
                pipeline: [{$match: {name: "bob"}}]
            }
        }],
        cursor: {},
        jsonSchema: {type: "object", properties: {foo: encryptedStringSpec}},
        isRemoteSchema: false,
    }),
                                 51208);

mongocryptd.stop();
})();
