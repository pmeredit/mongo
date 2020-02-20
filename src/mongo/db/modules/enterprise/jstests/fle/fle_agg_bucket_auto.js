/**
 * Test that mongocryptd can correctly mark the $bucketAuto agg stage with intent-to-encrypt
 * placeholders.
 */
(function() {
"use strict";

load("src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js");

const mongocryptd = new MongoCryptD();
mongocryptd.start();
const conn = mongocryptd.getConnection();
const testDB = conn.getDB("test");
const coll = testDB.fle_agg_bucket_auto;

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

function assertCommandUnchanged(command, hasEncryptionPlaceholders, schemaRequiresEncryption) {
    let cmdRes = assert.commandWorked(testDB.runCommand(command));
    delete command.jsonSchema;
    delete command.isRemoteSchema;
    delete cmdRes.result.lsid;
    assert.eq(command, cmdRes.result, cmdRes);
    assert.eq(hasEncryptionPlaceholders, cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert.eq(schemaRequiresEncryption, cmdRes.schemaRequiresEncryption, cmdRes);
}

let command, cmdRes, expectedResult;

// Test that $bucketAuto operating on not encrypted fields is not affected.
command = {
    aggregate: coll.getName(),
    pipeline: [{
        $bucketAuto: {
            groupBy: "$price",
            buckets: 3,
            granularity: "POWERSOF2",
            output: {"count": {$sum: {$const: 1}}, "artwork": {$push: "$title"}}
        }
    }],
    cursor: {},
    jsonSchema: {type: "object", properties: {foo: encryptedStringSpec}},
    isRemoteSchema: false
};
assertCommandUnchanged(command, false, true);

// Test that $bucketAuto operating on not encrypted fields marks the projected fields as not
// encrypted.
command = {
    aggregate: coll.getName(),
    pipeline: [
        {$bucketAuto: {groupBy: "$price", buckets: 3, output: {"artwork": {$push: "$title"}}}},
        {$match: {$and: [{_id: {$eq: "winterfell"}}, {artwork: {$eq: "winterfell"}}]}}
    ],
    cursor: {},
    jsonSchema: {type: "object", properties: {notdate: encryptedStringSpec}},
    isRemoteSchema: false
};
assertCommandUnchanged(command, false, true);

// Test that $bucketAuto operating with a 'groupBy' expression that returns an unencrypted
// result succeeds and marks encrypted fields in that expression appropriately.
command = {
    aggregate: coll.getName(),
    pipeline: [
        {
            $bucketAuto: {
                groupBy: {
                    $cond: [
                        {$eq: ["$ssn", {$const: "123-12-1212"}]},
                        {$const: "unencrypted1"},
                        {$const: "unencrypted2"}
                    ]
                },
                buckets: 3,
                output: {"artwork": {$push: "$title"}}
            }
        },
    ],
    cursor: {},
    jsonSchema: {type: "object", properties: {ssn: encryptedStringSpec}},
    isRemoteSchema: false
};
cmdRes = assert.commandWorked(testDB.runCommand(command));
assert.eq(true, cmdRes.hasEncryptionPlaceholders, cmdRes);
assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);
assert(cmdRes.hasOwnProperty("result"), cmdRes);
assert.eq(coll.getName(), cmdRes.result.aggregate, cmdRes);
assert(cmdRes.result.pipeline[0].$bucketAuto.groupBy.$cond[0].$eq[1].$const instanceof BinData,
       cmdRes);

// Test that $bucketAuto with 'groupBy' on an encrypted field fails.
command = {
    aggregate: coll.getName(),
    pipeline:
        [{$bucketAuto: {groupBy: "$price", buckets: 3, output: {"artwork": {$push: "$title"}}}}],
    cursor: {},
    jsonSchema: {type: "object", properties: {price: encryptedStringSpec}},
    isRemoteSchema: false
};
assert.commandFailedWithCode(testDB.runCommand(command), 51238);

// Test that the $addToSet accumulator in $bucketAuto succeeds if the output type is stable and
// its output schema has only deterministic nodes.
command = {
    aggregate: coll.getName(),
    pipeline: [
        {$bucketAuto: {groupBy: "$price", buckets: 3, output: {totalQuantity: {$addToSet: "$qty"}}}}
    ],
    cursor: {},
    jsonSchema: {type: "object", properties: {qty: encryptedStringSpec}},
    isRemoteSchema: false,
};
assertCommandUnchanged(command, false, true);

// Test that fields corresponding to accumulator expressions with array accumulators over
// not encrypted fields in $bucketAuto can be referenced in the query and are marked as not
// encrypted.
command = {
    aggregate: coll.getName(),
    pipeline: [
        {$bucketAuto: {groupBy: "$price", buckets: 3, output: {itemList: {}}}},
        {$match: {"itemList": {$eq: "winterfell"}}}
    ],
    cursor: {}
};
let arrayAccus = ["$addToSet", "$push"];
for (let accu of arrayAccus) {
    command.pipeline[0].$bucketAuto.output.itemList = {[accu]: "$item"};
    command.jsonSchema = {type: "object", properties: {notitem: encryptedStringSpec}};
    command.isRemoteSchema = false;

    assertCommandUnchanged(command, false, true);
}

// Test that numeric accumulators are allowed if their expression involves a comparison to an
// encrypted field but the output type is always not encrypted. Also test that in such cases the
// constants compared to the encrypted field are correctly marked for encryption.
let condExpr = {
    $cond: [{$eq: ["$qty", "thousand"]}, {$multiply: ["$price", {$const: 0.8}]}, "$price"]
};
command = {
    aggregate: coll.getName(),
    pipeline: [
        {$bucketAuto: {groupBy: "$time", buckets: 3, output: {totalPrice: {}, count: {}}}},
        {$match: {"itemList": {$eq: "winterfell"}}}
    ],
    cursor: {},
    jsonSchema: {type: "object", properties: {qty: encryptedStringSpec}},
    isRemoteSchema: false,
};
let numericAccus = ["$sum", "$min", "$max", "$avg", "$stdDevPop", "$stdDevSamp"];
for (let accu of numericAccus) {
    command.pipeline[0].$bucketAuto.output.totalPrice = {[accu]: condExpr};
    command.pipeline[0].$bucketAuto.output.count = {[accu]: {$const: 1}};

    cmdRes = assert.commandWorked(testDB.runCommand(command));
    assert.eq(true, cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);
    assert(cmdRes.hasOwnProperty("result"), cmdRes);
    assert.eq(coll.getName(), cmdRes.result.aggregate, cmdRes);
    assert(cmdRes.result.pipeline[0]
                   .$bucketAuto.output.totalPrice[accu]
                   .$cond[0]
                   .$eq[1]
                   .$const instanceof
               BinData,
           cmdRes);
    assert.eq(
        cmdRes.result.pipeline[0].$bucketAuto.output.totalPrice[accu].$cond[1].$multiply[1].$const,
        0.8,
        cmdRes);
    assert.eq(cmdRes.result.pipeline[0].$bucketAuto.output.count[accu].$const, 1, cmdRes);
}

// Test that fields corresponding to the $first, $last accumulators in $bucketAuto
// preserve encryption properties of the expression.
command = {
    aggregate: coll.getName(),
    pipeline: [
        {$bucketAuto: {groupBy: "$time", buckets: 3, output: {representative: {}}}},
        {$match: {"representative": {$eq: "winterfell"}}}
    ],
    cursor: {},
    jsonSchema: {
        type: "object",
        properties: {sales: {type: "object", properties: {region: encryptedStringSpec}}}
    },
    isRemoteSchema: false
};

let selectionAccus = ["$first", "$last"];
for (let accu of selectionAccus) {
    command.pipeline[0].$bucketAuto.output.representative = {[accu]: "$sales.region"};

    cmdRes = assert.commandWorked(testDB.runCommand(command));
    assert.eq(true, cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);
    assert(cmdRes.hasOwnProperty("result"), cmdRes);
    assert.eq(coll.getName(), cmdRes.result.aggregate, cmdRes);
    assert(cmdRes.result.pipeline[1].$match["representative"].$eq instanceof BinData, cmdRes);
}

// Test that the $mergeObjects accumulator expression aggregating not encrypted fields succeeds.
command = {
    aggregate: coll.getName(),
    pipeline: [{
        $bucketAuto: {groupBy: "$price", buckets: 3, output: {combination: {$mergeObjects: "$qty"}}}
    }],
    cursor: {},
    jsonSchema: {type: "object", properties: {otherQty: encryptedStringSpec}},
    isRemoteSchema: false
};
assertCommandUnchanged(command, false, true);

// Test that $accumulator is allowed as long as it doesn't touch any encrypted fields.
command = {
    aggregate: coll.getName(),
    pipeline: [{
        $bucketAuto: {
            groupBy: "$price",
            buckets: 3,
            output: {
                abc: {
                    $accumulator: {
                        init: 'function() {}',
                        initArgs: {$const: []},
                        accumulate: 'function() {}',
                        accumulateArgs: "$nonSecretString",
                        merge: 'function() {}',
                        finalize: 'function() {}',
                        lang: 'js',
                    }
                },
            }
        }
    }],
    cursor: {},
    jsonSchema: {type: "object", properties: {secretString: encryptedStringSpec}},
    isRemoteSchema: false,
};
assertCommandUnchanged(command, false, true);

// Test that $accumulator is not allowed to reference encrypted or possibly-encrypted fields.
command = {
    aggregate: coll.getName(),
    pipeline: [{
        $bucketAuto: {
            groupBy: "$price",
            buckets: 3,
            output: {
                abc: {
                    $accumulator: {
                        init: 'function() {}',
                        initArgs: {$const: []},
                        accumulate: 'function() {}',
                        accumulateArgs:
                            {$cond: ["$unknownBool", "a public string", "$secretString"]},
                        merge: 'function() {}',
                        finalize: 'function() {}',
                        lang: 'js',
                    }
                },
            }
        }
    }],
    cursor: {},
    jsonSchema: {type: "object", properties: {secretString: encryptedStringSpec}},
    isRemoteSchema: false,
};
assert.commandFailedWithCode(testDB.runCommand(command), 51221);

mongocryptd.stop();
})();
