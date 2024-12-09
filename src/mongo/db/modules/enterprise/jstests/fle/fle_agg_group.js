/**
 * Test that mongocryptd can correctly mark the $group agg stage with intent-to-encrypt
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
const coll = testDB.fle_agg_group;

const FLE2InvalidReferenceCode = 6331102;

const encryptedStringSpec = {
    encrypt: {algorithm: kDeterministicAlgo, keyId: [UUID()], bsonType: "string"}
};

const encryptedIntSpec = {
    encrypt: {algorithm: kDeterministicAlgo, keyId: [UUID()], bsonType: "int"}
};

const encryptedRandomSpec = {
    encrypt: {algorithm: kRandomAlgo, keyId: [UUID()], bsonType: "string"}
};

function assertCommandUnchanged(
    command, hasEncryptionPlaceholders, schemaRequiresEncryption, fle2ErrCode) {
    if (fle2Enabled() && fle2ErrCode != undefined) {
        assert.commandFailedWithCode(testDB.runCommand(command), fle2ErrCode);
    } else {
        let cmdRes = assert.commandWorked(testDB.runCommand(command));
        delete command.jsonSchema;
        delete command.isRemoteSchema;
        delete cmdRes.result.lsid;
        assert.eq(command, cmdRes.result, cmdRes);
        assert.eq(hasEncryptionPlaceholders, cmdRes.hasEncryptionPlaceholders, cmdRes);
        assert.eq(schemaRequiresEncryption, cmdRes.schemaRequiresEncryption, cmdRes);
    }
}

let command, cmdRes;

// Test that $group with an object passed for _id does not get affected when no encryption is
// required by the schema.
command = Object.assign({
    aggregate: coll.getName(),
    pipeline: [{
        $group: {
            _id: {month: {$month: {date: "$date"}}, year: {$year: {date: "$date"}}},
            totalPrice: {
                $sum: {
                    $cond: [
                        {$gte: ["$qty", {$const: 1000}]},
                        {$multiply: ["$price", {$const: 0.8}]},
                        "$price"
                    ]
                }
            },
            count: {$sum: {$const: 1}}
        }
    }],
    cursor: {}
},
                        generateSchema({}, coll.getFullName()));
assertCommandUnchanged(command, false, false);

// Test that $group with an expression passed for '_id' marks this field as not encrypted,
// because the corresponding expression is not encrypted.
command = Object.assign({
    aggregate: coll.getName(),
    pipeline: [{$group: {_id: {$const: null}}}, {$match: {"_id": {$eq: "winterfell"}}}],
    cursor: {}
},
                        generateSchema({_id: encryptedStringSpec}, coll.getFullName()));
assertCommandUnchanged(command, false, true, 6316403);

// Test that $group with an object passed for '_id' marks its field as encrypted,
// if a corresponding expression is a rename and the old field is encrypted.
command = Object.assign({
    aggregate: coll.getName(),
    pipeline: [{$group: {_id: {dt: "$date"}}}, {$match: {"_id.dt": {$eq: "winterfell"}}}],
    cursor: {}
},
                        generateSchema({date: encryptedStringSpec}, coll.getFullName()));
// FLE 2 has only limited support for referring to an encrypted field in an aggregate expression.
if (fle2Enabled()) {
    assert.commandFailedWithCode(testDB.runCommand(command), 51222);
} else {
    cmdRes = assert.commandWorked(testDB.runCommand(command));
    assert.eq(true, cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);
    assert(cmdRes.hasOwnProperty("result"), cmdRes);
    assert.eq(coll.getName(), cmdRes.result.aggregate, cmdRes);
    assert(cmdRes.result.pipeline[1].$match["_id.dt"].$eq instanceof BinData, cmdRes);
}

// Test that $group with an object passed for '_id' marks its field as not encrypted,
// if a corresponding expression is a rename and the old field is not encrypted.
command = Object.assign({
    aggregate: coll.getName(),
    pipeline: [{$group: {_id: {dt: "$date"}}}, {$match: {"_id.dt": {$eq: "winterfell"}}}],
    cursor: {}
},
                        generateSchema({notdate: encryptedStringSpec}, coll.getFullName()));
assertCommandUnchanged(command, false, true);

// Test that $group with an object passed for '_id' marks its field as not encrypted,
// if a corresponding expression is not a rename.
command = Object.assign({
    aggregate: coll.getName(),
    pipeline: [
        {$group: {_id: {year: {$year: {date: "$date"}}}}},
        {$match: {"_id.year": {$eq: "winterfell"}}}
    ],
    cursor: {}
},
                        generateSchema({year: encryptedStringSpec}, coll.getFullName()));
assertCommandUnchanged(command, false, true);

const qtyEncryptedQueryable = generateSchema({qty: encryptedStringSpec}, coll.getFullName());
const qtyEncryptedRandom = generateSchema({qty: encryptedRandomSpec}, coll.getFullName());

// Test that $group with an expression in '_id' has constants correctly marked for encryption.
command = Object.assign({
    aggregate: coll.getName(),
    pipeline:
        [{$group: {_id: {$cond: [{$eq: ["$qty", {$const: "thousand"}]}, "1000", "not1000"]}}}],
    cursor: {}
},
                        qtyEncryptedQueryable);
// FLE 2 has only limited support for referring to an encrypted field in an aggregate expression.
if (fle2Enabled()) {
    assert.commandFailedWithCode(testDB.runCommand(command), FLE2InvalidReferenceCode);
} else {
    cmdRes = assert.commandWorked(testDB.runCommand(command));
    assert.eq(true, cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);
    assert(cmdRes.hasOwnProperty("result"), cmdRes);
    assert.eq(coll.getName(), cmdRes.result.aggregate, cmdRes);
    assert(cmdRes.result.pipeline[0].$group._id.$cond[0].$eq[1]["$const"] instanceof BinData,
           cmdRes);
}

// Test that $group with an expression in '_id' requires a stable output type across documents
// to allow for comparisons. The encryption properties of $qtyOther and $qty are the same.
command = Object.assign({
    aggregate: coll.getName(),
    pipeline:
        [{$group: {_id: {$cond: [{$eq: ["$value", {$const: "thousand"}]}, "$qty", "$qtyOther"]}}}],
    cursor: {}
},
                        generateSchema({qty: encryptedStringSpec, qtyOther: encryptedStringSpec},
                                       coll.getFullName()));
assertCommandUnchanged(command, false, true, 6338401);

// Test that $group with an expression in '_id' requires a stable output type across documents
// to allow for comparisons. The encryption properties of $qtyOther and $qty are different.
command = Object.assign({
    aggregate: coll.getName(),
    pipeline:
        [{$group: {_id: {$cond: [{$eq: ["$qty", {$const: "thousand"}]}, "$qty", "$qtyOther"]}}}],
    cursor: {}
},
                        qtyEncryptedQueryable);
assert.commandFailedWithCode(testDB.runCommand(command), 51222);

// Test that $group with an expression in '_id' correctly marks literals since the evaluated
// result of the expression will be used in comparison across documents.
command = Object.assign({
    aggregate: coll.getName(),
    pipeline: [{
        $group:
            {_id: {$cond: [{$eq: ["$value", {$const: "thousand"}]}, {$const: "1000"}, "$qty"]}}
    }],
    cursor: {}
},
                        qtyEncryptedQueryable);
// FLE 2 has only limited support for referring to an encrypted field in an aggregate expression.
if (fle2Enabled()) {
    assert.commandFailedWithCode(testDB.runCommand(command), 51222);
} else {
    cmdRes = assert.commandWorked(testDB.runCommand(command));
    assert.eq(true, cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);
    assert(cmdRes.result.pipeline[0].$group._id.$cond[1].$const instanceof BinData, cmdRes);
}

// Test that literals are properly marked when $ifNull is used in the _id field.
command = Object.assign({
    aggregate: coll.getName(),
    pipeline: [{$group: {_id: {$ifNull: ["$qty", "no-ssn"]}}}],
    cursor: {}
},
                        qtyEncryptedQueryable);
// FLE 2 has only limited support for referring to an encrypted field in an aggregate expression.
if (fle2Enabled()) {
    assert.commandFailedWithCode(testDB.runCommand(command), 51222);
} else {
    cmdRes = assert.commandWorked(testDB.runCommand(command));
    assert.eq(true, cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);
    assert(cmdRes.result.pipeline[0].$group._id.$ifNull[1].$const instanceof BinData, cmdRes);
}

// Test that the $group succeeds if _id expression is a deterministically encrypted field.
command =
    Object.assign({aggregate: coll.getName(), pipeline: [{$group: {_id: "$qty"}}], cursor: {}},
                  qtyEncryptedQueryable);
assertCommandUnchanged(command, false, true, 51222);

// Test that the $group fails if _id expression is a prefix of a deterministically encrypted
// field.
command =
    Object.assign({aggregate: coll.getName(), pipeline: [{$group: {_id: "$foo"}}], cursor: {}},
                  generateSchema({'foo.bar': encryptedStringSpec}, coll.getFullName()));
assert.commandFailedWithCode(testDB.runCommand(command), 31129);

// Test that the $group fails if _id expression is a path with an encrypted prefix.
command =
    Object.assign({aggregate: coll.getName(), pipeline: [{$group: {_id: "$qty.bar"}}], cursor: {}},
                  qtyEncryptedQueryable);
assert.commandFailedWithCode(testDB.runCommand(command), 51102);

// Test that the $group fails if _id expression outputs schema with any fields encrypted with
// the random algorithm.
command =
    Object.assign({aggregate: coll.getName(), pipeline: [{$group: {_id: "$qty"}}], cursor: {}},
                  qtyEncryptedRandom);
assert.commandFailedWithCode(testDB.runCommand(command), 51222);

// Test that the $addToSet accumulator in $group succeeds if the output type is stable and
// its output schema has only deterministic nodes.
command = Object.assign({
    aggregate: coll.getName(),
    pipeline: [{$group: {_id: {$const: null}, totalQuantity: {$addToSet: "$qty"}}}],
    cursor: {}
},
                        qtyEncryptedQueryable);
assertCommandUnchanged(command, false, true, 51223);

// Test that the $addToSet accumulator in $group successfully marks constants if added along
// with an encrypted field.
command = Object.assign({
    aggregate: coll.getName(),
    pipeline: [{
        $group: {
            _id: null,
            totalQuantity: {
                $addToSet:
                    {$cond: [{$eq: ["$qty", {$const: "thousand"}]}, "$qty", "defaultQuantity"]}
            }
        }
    }],
    cursor: {}
},
                        qtyEncryptedQueryable);
// FLE 2 has only limited support for referring to an encrypted field in an aggregate expression.
if (fle2Enabled()) {
    assert.commandFailedWithCode(testDB.runCommand(command), 51223);
} else {
    cmdRes = assert.commandWorked(testDB.runCommand(command));
    assert.eq(true, cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);
    assert(
        cmdRes.result.pipeline[0].$group.totalQuantity.$addToSet.$cond[2].$const instanceof BinData,
        cmdRes);
}

// Test that the $addToSet accumulator in $group fails if the output type is unstable.
command = Object.assign({
    aggregate: coll.getName(),
    pipeline: [{
        $group: {
            _id: null,
            totalQuantity:
                {$addToSet:
                     {$cond: [{$eq: ["$qty", {$const: "thousand"}]}, "$qty", "$otherQty"]}}
        }
    }],
    cursor: {}
},
                        qtyEncryptedQueryable);
assert.commandFailedWithCode(testDB.runCommand(command), 51223);

// Test that the $addToSet accumulator in $group fails if its expression outputs schema with
// any fields encrypted with the random algorithm.
command = Object.assign({
    aggregate: coll.getName(),
    pipeline: [{$group: {_id: null, totalQuantity: {$addToSet: "$qty"}}}],
    cursor: {}
},
                        qtyEncryptedRandom);
assert.commandFailedWithCode(testDB.runCommand(command), 51223);

// Test that fields corresponding to accumulator expressions with array accumulators over
// not encrypted fields in $group can be referenced in the query and are marked as not
// encrypted.
command = {
    aggregate: coll.getName(),
    pipeline: [
        {$group: {_id: {$const: null}, itemList: {}}},
        {$match: {"itemList": {$eq: "winterfell"}}}
    ],
    cursor: {}
};
let arrayAccus = ["$addToSet", "$push"];
for (let accu of arrayAccus) {
    command.pipeline[0].$group.itemList = {[accu]: "$item"};
    Object.assign(command, qtyEncryptedRandom);
    assertCommandUnchanged(command, false, true);
}

// Test that fields corresponding to accumulator expressions with array accumulators over
// encrypted fields in $group are supported as long as they are not referenced afterwards.
command = Object.assign({
    aggregate: coll.getName(),
    pipeline: [{$group: {_id: "$castle", itemList: {}}}, {$match: {_id: {$eq: "winterfell"}}}],
    cursor: {}
},
                        generateSchema({item: encryptedStringSpec}, coll.getFullName()));
for (let accu of arrayAccus) {
    command.pipeline[0].$group.itemList = {[accu]: "$item"};

    // FLE 2 has limited support for referring to an encrypted field in an aggregate expression.
    if (fle2Enabled()) {
        assert.commandFailedWithCode(testDB.runCommand(command), [FLE2InvalidReferenceCode, 51223]);
    } else {
        cmdRes = assert.commandWorked(testDB.runCommand(command));
        assert.eq(false, cmdRes.hasEncryptionPlaceholders, cmdRes);
        assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);
        assert(cmdRes.hasOwnProperty("result"), cmdRes);
        assert.eq(coll.getName(), cmdRes.result.aggregate, cmdRes);
    }
}

// Test that fields corresponding to accumulator expressions with array accumulators over
// encrypted fields in $group cannot be referenced in subsequent stages.
command = Object.assign({
    aggregate: coll.getName(),
    pipeline: [{$group: {_id: null, itemList: {}}}, {$match: {"itemList": {$eq: "winterfell"}}}],
    cursor: {}
},
                        generateSchema({item: encryptedStringSpec}, coll.getFullName()));
for (let accu of arrayAccus) {
    command.pipeline[0].$group.itemList = {[accu]: "$item"};
    // This expected errors list may seem unusually long, but different accumulators can trigger
    // different error codes, so we do actually need each code in the list.
    assert.commandFailedWithCode(testDB.runCommand(command),
                                 [31133, 51223, FLE2InvalidReferenceCode]);
}

// Test that numeric accumulator expressions aggregating encrypted fields fail.
command = Object.assign(
    {aggregate: coll.getName(), pipeline: [{$group: {_id: null, totalPrice: {}}}], cursor: {}},
    generateSchema({price: encryptedIntSpec}, coll.getFullName()));
let numericAccus = ["$sum", "$min", "$max", "$avg", "$stdDevPop", "$stdDevSamp"];
for (let accu of numericAccus) {
    command.pipeline[0].$group.totalPrice = {[accu]: "$price"};
    assert.commandFailedWithCode(testDB.runCommand(command), 51221);
}

// Test that numeric accumulators are allowed if their expression involves a comparison to an
// encrypted field but the output type is always not encrypted. Also test that in such cases the
// constants compared to the encrypted field are correctly marked for encryption.
let condExpr = {
    $cond: [{$eq: ["$qty", "thousand"]}, {$multiply: ["$price", {$const: 0.8}]}, "$price"]
};
command = Object.assign({
    aggregate: coll.getName(),
    pipeline: [{$group: {_id: null, totalPrice: {}, count: {}}}],
    cursor: {}
},
                        qtyEncryptedQueryable);
for (let accu of numericAccus) {
    command.pipeline[0]
        .$group = {_id: null, totalPrice: {[accu]: condExpr}, count: {[accu]: {$const: 1}}};

    // FLE 2 has limited support for referring to an encrypted field in an aggregate expression.
    if (fle2Enabled()) {
        assert.commandFailedWithCode(testDB.runCommand(command), FLE2InvalidReferenceCode);
    } else {
        cmdRes = assert.commandWorked(testDB.runCommand(command));
        assert.eq(true, cmdRes.hasEncryptionPlaceholders, cmdRes);
        assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);
        assert(cmdRes.hasOwnProperty("result"), cmdRes);
        assert.eq(coll.getName(), cmdRes.result.aggregate, cmdRes);
        assert(cmdRes.result.pipeline[0].$group.totalPrice[accu].$cond[0].$eq[1].$const instanceof
                   BinData,
               cmdRes);
        assert.eq(cmdRes.result.pipeline[0].$group.totalPrice[accu].$cond[1].$multiply[1].$const,
                  0.8,
                  cmdRes);
        assert.eq(cmdRes.result.pipeline[0].$group.count[accu].$const, 1, cmdRes);
    }
}

// Test that fields corresponding to the $first, $last accumulators in $group
// preserve encryption properties of the expression.
command = Object.assign({
    aggregate: coll.getName(),
    pipeline: [
        {$group: {_id: null, representative: {}}},
        {$match: {"representative": {$eq: "winterfell"}}}
    ],
    cursor: {}
},
                        generateSchema({'sales.region': encryptedStringSpec}, coll.getFullName()));

let selectionAccus = ["$first", "$last"];
for (let accu of selectionAccus) {
    command.pipeline[0].$group.representative = {[accu]: "$sales.region"};

    // FLE 2 has limited support for referring to an encrypted field in an aggregate expression.
    if (fle2Enabled()) {
        assert.commandFailedWithCode(testDB.runCommand(command), FLE2InvalidReferenceCode);
    } else {
        cmdRes = assert.commandWorked(testDB.runCommand(command));
        assert.eq(true, cmdRes.hasEncryptionPlaceholders, cmdRes);
        assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);
        assert(cmdRes.hasOwnProperty("result"), cmdRes);
        assert.eq(coll.getName(), cmdRes.result.aggregate, cmdRes);
        assert(cmdRes.result.pipeline[1].$match["representative"].$eq instanceof BinData, cmdRes);
    }
}

// Test that accumulator expression aggregating prefixes of encrypted fields fails.
command = Object.assign(
    {aggregate: coll.getName(), pipeline: [{$group: {_id: null, totalPrice: {}}}], cursor: {}},
    generateSchema({'foo.price': encryptedIntSpec}, coll.getFullName()));
let allAccus = numericAccus.concat(selectionAccus).concat(arrayAccus).concat(["$mergeObjects"]);
for (let accu of allAccus) {
    command.pipeline[0].$group.totalPrice = {[accu]: "$foo"};
    assert.commandFailedWithCode(testDB.runCommand(command), 31129);
}

// Test that the $mergeObjects accumulator expression aggregating not encrypted fields succeeds.
command = Object.assign({
    aggregate: coll.getName(),
    pipeline: [{$group: {_id: {$const: null}, combination: {$mergeObjects: "$otherQty"}}}],
    cursor: {}
},
                        qtyEncryptedQueryable);
assertCommandUnchanged(command, false, true);

// Test that the $mergeObjects accumulator expression aggregating encrypted fields fails.
command = Object.assign({
    aggregate: coll.getName(),
    pipeline: [{$group: {_id: null, combination: {$mergeObjects: "$qty"}}}],
    cursor: {}
},
                        qtyEncryptedQueryable);
assert.commandFailedWithCode(testDB.runCommand(command), 51221);

// Test that $accumulator is allowed as long as it doesn't touch any encrypted fields.
// It's allowed to reference the group key.
command = Object.assign({
    aggregate: coll.getName(),
    pipeline: [{
        $group: {
            _id: {x: "$nonSecretString", y: {$const: "qwer"}},
            abc: {
                $accumulator: {
                    init: 'function() {}',
                    initArgs: {$add: ["$x", "$y"]},
                    accumulate: 'function() {}',
                    accumulateArgs: "$nonSecretString",
                    merge: 'function() {}',
                    finalize: 'function() {}',
                    lang: 'js',
                }
            },
        }
    }],
    cursor: {}
},
                        generateSchema({secretString: encryptedStringSpec}, coll.getFullName()));
assertCommandUnchanged(command, false, true);

// Test that $accumulator is not allowed to reference encrypted or possibly-encrypted fields.
command = Object.assign({
    aggregate: coll.getName(),
    pipeline: [{
        $group: {
            _id: {$const: null},
            abc: {
                $accumulator: {
                    init: 'function() {}',
                    accumulate: 'function() {}',
                    accumulateArgs: {$cond: ["$unknownBool", "a public string", "$secretString"]},
                    merge: 'function() {}',
                    finalize: 'function() {}',
                    lang: 'js',
                }
            },
        }
    }],
    cursor: {}
},
                        generateSchema({secretString: encryptedStringSpec}, coll.getFullName()));
assert.commandFailedWithCode(testDB.runCommand(command), 51221);

// Test that even when encrypted fields end up in the group key, $accumulator is not allowed to
// reference them.
command = Object.assign({
    aggregate: coll.getName(),
    pipeline: [{
        $group: {
            _id: {secretStringInGroupKey: "$secretString"},
            abc: {
                $accumulator: {
                    init: 'function() {}',
                    initArgs:
                        [{$cond: ["$unknownBool", "a public string", "$secretStringInGroupKey"]}],
                    accumulate: 'function() {}',
                    accumulateArgs: [],
                    merge: 'function() {}',
                    finalize: 'function() {}',
                    lang: 'js',
                }
            },
        }
    }],
    cursor: {}
},
                        generateSchema({secretString: encryptedStringSpec}, coll.getFullName()));
assert.commandFailedWithCode(testDB.runCommand(command), [4544715, 51222]);

mongocryptd.stop();
