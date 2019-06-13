/**
 * Test that mongocryptd can correctly mark the $group agg stage with intent-to-encrypt
 * placeholders.
 */
(function() {
    "use strict";

    load("src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js");

    const mongocryptd = new MongoCryptD();
    mongocryptd.start();
    const conn = mongocryptd.getConnection();
    const testDB = conn.getDB("test");
    const coll = testDB.fle_agg_group;

    const encryptedStringSpec = {
        encrypt: {
            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
            keyId: [UUID()],
            bsonType: "string"
        }
    };

    const encryptedIntSpec = {
        encrypt: {
            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
            keyId: [UUID()],
            bsonType: "int"
        }
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

    let command, cmdRes;

    // Test that $group with an object passed for _id does not get affected when no encryption is
    // required by the schema.
    command = {
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
        cursor: {},
        jsonSchema: {},
        isRemoteSchema: false,
    };
    assertCommandUnchanged(command, false, false);

    // Test that $group with an expression passed for '_id' marks this field as not encrypted,
    // because the corresponding expression is not encrypted.
    command = {
        aggregate: coll.getName(),
        pipeline: [{$group: {_id: {$const: null}}}, {$match: {"_id": {$eq: "winterfell"}}}],
        cursor: {},
        jsonSchema: {type: "object", properties: {_id: encryptedStringSpec}},
        isRemoteSchema: false
    };
    assertCommandUnchanged(command, false, true);

    // Test that $group with an object passed for '_id' marks its field as encrypted,
    // if a corresponding expression is a rename and the old field is encrypted.
    command = {
        aggregate: coll.getName(),
        pipeline: [{$group: {_id: {dt: "$date"}}}, {$match: {"_id.dt": {$eq: "winterfell"}}}],
        cursor: {},
        jsonSchema: {type: "object", properties: {date: encryptedStringSpec}},
        isRemoteSchema: false
    };
    cmdRes = assert.commandWorked(testDB.runCommand(command));
    assert.eq(true, cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);
    assert(cmdRes.hasOwnProperty("result"), cmdRes);
    assert.eq(coll.getName(), cmdRes.result.aggregate, cmdRes);
    assert(cmdRes.result.pipeline[1].$match["_id.dt"].$eq instanceof BinData, cmdRes);

    // Test that $group with an object passed for '_id' marks its field as not encrypted,
    // if a corresponding expression is a rename and the old field is not encrypted.
    command = {
        aggregate: coll.getName(),
        pipeline: [{$group: {_id: {dt: "$date"}}}, {$match: {"_id.dt": {$eq: "winterfell"}}}],
        cursor: {},
        jsonSchema: {type: "object", properties: {notdate: encryptedStringSpec}},
        isRemoteSchema: false
    };
    assertCommandUnchanged(command, false, true);

    // Test that $group with an object passed for '_id' marks its field as not encrypted,
    // if a corresponding expression is not a rename.
    command = {
        aggregate: coll.getName(),
        pipeline: [
            {$group: {_id: {year: {$year: {date: "$date"}}}}},
            {$match: {"_id.year": {$eq: "winterfell"}}}
        ],
        cursor: {},
        jsonSchema: {type: "object", properties: {year: encryptedStringSpec}},
        isRemoteSchema: false
    };
    assertCommandUnchanged(command, false, true);

    // Test that $group with an expression in '_id' has constants correctly marked for encryption.
    command = {
        aggregate: coll.getName(),
        pipeline:
            [{$group: {_id: {$cond: [{$eq: ["$qty", {$const: "thousand"}]}, "1000", "not1000"]}}}],
        cursor: {},
        jsonSchema: {type: "object", properties: {qty: encryptedStringSpec}},
        isRemoteSchema: false
    };
    cmdRes = assert.commandWorked(testDB.runCommand(command));
    assert.eq(true, cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);
    assert(cmdRes.hasOwnProperty("result"), cmdRes);
    assert.eq(coll.getName(), cmdRes.result.aggregate, cmdRes);
    assert(cmdRes.result.pipeline[0].$group._id.$cond[0].$eq[1]["$const"] instanceof BinData,
           cmdRes);

    // Test that $group with an expression in '_id' requires a stable output type across documents
    // to allow for comparisons. The encryption properties of $qtyOther and $qty are the same.
    command = {
        aggregate: coll.getName(),
        pipeline: [{
            $group:
                {_id: {$cond: [{$eq: ["$value", {$const: "thousand"}]}, "$qty", "$qtyOther"]}}
        }],
        cursor: {},
        jsonSchema:
            {type: "object", properties: {qty: encryptedStringSpec, qtyOther: encryptedStringSpec}},
        isRemoteSchema: false
    };
    assertCommandUnchanged(command, false, true);

    // Test that $group with an expression in '_id' requires a stable output type across documents
    // to allow for comparisons. The encryption properties of $qtyOther and $qty are different.
    command = {
        aggregate: coll.getName(),
        pipeline: [
            {$group: {_id: {$cond: [{$eq: ["$qty", {$const: "thousand"}]}, "$qty", "$qtyOther"]}}}
        ],
        cursor: {},
        jsonSchema: {type: "object", properties: {qty: encryptedStringSpec}},
        isRemoteSchema: false
    };
    assert.commandFailedWithCode(testDB.runCommand(command), 51234);

    // Test that $group with an expression in '_id' correctly marks literals since the evaluated
    // result of the expression will be used in comparison across documents.
    command = {
        aggregate: coll.getName(),
        pipeline: [{
            $group: {
                _id:
                    {$cond: [{$eq: ["$value", {$const: "thousand"}]}, {$const: "1000"}, "$qty"]}
            }
        }],
        cursor: {},
        jsonSchema: {type: "object", properties: {qty: encryptedStringSpec}},
        isRemoteSchema: false
    };
    cmdRes = assert.commandWorked(testDB.runCommand(command));
    assert.eq(true, cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);
    assert(cmdRes.result.pipeline[0].$group._id.$cond[1].$const instanceof BinData, cmdRes);

    // Test that the $group succeeds if _id expression is a deterministically encrypted field.
    command = {
        aggregate: coll.getName(),
        pipeline: [{$group: {_id: "$foo"}}],
        cursor: {},
        jsonSchema: {type: "object", properties: {foo: encryptedStringSpec}},
        isRemoteSchema: false
    };
    assertCommandUnchanged(command, false, true);

    // Test that the $group fails if _id expression is a prefix of a deterministically encrypted
    // field.
    command = {
        aggregate: coll.getName(),
        pipeline: [{$group: {_id: "$foo"}}],
        cursor: {},
        jsonSchema: {
            type: "object",
            properties: {foo: {type: "object", properties: {bar: encryptedStringSpec}}}
        },
        isRemoteSchema: false
    };
    assert.commandFailedWithCode(testDB.runCommand(command), 31129);

    // Test that the $group fails if _id expression is a path with an encrypted prefix.
    command = {
        aggregate: coll.getName(),
        pipeline: [{$group: {_id: "$foo.bar"}}],
        cursor: {},
        jsonSchema: {type: "object", properties: {foo: encryptedStringSpec}},
        isRemoteSchema: false
    };
    assert.commandFailedWithCode(testDB.runCommand(command), 51102);

    // Test that the $group fails if _id expression outputs schema with any fields encrypted with
    // the random algorithm.
    command = {
        aggregate: coll.getName(),
        pipeline: [{$group: {_id: "$foo"}}],
        cursor: {},
        jsonSchema: {type: "object", properties: {foo: encryptedRandomSpec}},
        isRemoteSchema: false
    };
    assert.commandFailedWithCode(testDB.runCommand(command), 51222);

    // Test that the $addToSet accumulator in $group succeeds if the output type is stable and
    // its output schema has only deterministic nodes.
    command = {
        aggregate: coll.getName(),
        pipeline: [{$group: {_id: {$const: null}, totalQuantity: {$addToSet: "$qty"}}}],
        cursor: {},
        jsonSchema: {type: "object", properties: {qty: encryptedStringSpec}},
        isRemoteSchema: false,
    };
    assertCommandUnchanged(command, false, true);

    // Test that the $addToSet accumulator in $group successfully marks constants if added along
    // with an encrypted field.
    command = {
        aggregate: coll.getName(),
        pipeline: [{
            $group: {
                _id: null,
                totalQuantity: {
                    $addToSet: {
                        $cond:
                            [{$eq: ["$qty", {$const: "thousand"}]}, "$qty", "defaultQuantity"]
                    }
                }
            }
        }],
        cursor: {},
        jsonSchema: {type: "object", properties: {qty: encryptedStringSpec}},
        isRemoteSchema: false,
    };
    cmdRes = assert.commandWorked(testDB.runCommand(command));
    assert.eq(true, cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);
    assert(
        cmdRes.result.pipeline[0].$group.totalQuantity.$addToSet.$cond[2].$const instanceof BinData,
        cmdRes);

    // Test that the $addToSet accumulator in $group fails if the output type is unstable.
    command = {
        aggregate: coll.getName(),
        pipeline: [{
            $group: {
                _id: null,
                totalQuantity: {
                    $addToSet:
                        {$cond: [{$eq: ["$qty", {$const: "thousand"}]}, "$qty", "$otherQty"]}
                }
            }
        }],
        cursor: {},
        jsonSchema: {type: "object", properties: {qty: encryptedStringSpec}},
        isRemoteSchema: false,
    };
    assert.commandFailedWithCode(testDB.runCommand(command), 31134);

    // Test that the $addToSet accumulator in $group fails if its expression outputs schema with
    // any fields encrypted with the random algorithm.
    command = {
        aggregate: coll.getName(),
        pipeline: [{$group: {_id: null, totalQuantity: {$addToSet: "$qty"}}}],
        cursor: {},
        jsonSchema: {type: "object", properties: {qty: encryptedRandomSpec}},
        isRemoteSchema: false,
    };
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
        command.jsonSchema = {type: "object", properties: {notitem: encryptedStringSpec}};
        command.isRemoteSchema = false;
        assertCommandUnchanged(command, false, true);
    }

    // Test that fields corresponding to accumulator expressions with array accumulators over
    // encrypted fields in $group are supported as long as they are not referenced afterwards.
    command = {
        aggregate: coll.getName(),
        pipeline: [{$group: {_id: "$castle", itemList: {}}}, {$match: {_id: {$eq: "winterfell"}}}],
        cursor: {},
        jsonSchema: {type: "object", properties: {item: encryptedStringSpec}},
        isRemoteSchema: false
    };
    for (let accu of arrayAccus) {
        command.pipeline[0].$group.itemList = {[accu]: "$item"};

        cmdRes = assert.commandWorked(testDB.runCommand(command));
        assert.eq(false, cmdRes.hasEncryptionPlaceholders, cmdRes);
        assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);
        assert(cmdRes.hasOwnProperty("result"), cmdRes);
        assert.eq(coll.getName(), cmdRes.result.aggregate, cmdRes);
    }

    // Test that fields corresponding to accumulator expressions with array accumulators over
    // encrypted fields in $group cannot be referenced in subsequent stages.
    command = {
        aggregate: coll.getName(),
        pipeline:
            [{$group: {_id: null, itemList: {}}}, {$match: {"itemList": {$eq: "winterfell"}}}],
        cursor: {},
        jsonSchema: {type: "object", properties: {item: encryptedStringSpec}},
        isRemoteSchema: false
    };
    for (let accu of arrayAccus) {
        command.pipeline[0].$group.itemList = {[accu]: "$item"};
        assert.commandFailedWithCode(testDB.runCommand(command), 31133);
    }

    // Test that numeric accumulator expression aggregating encrypted fields fails.
    command = {
        aggregate: coll.getName(),
        pipeline: [{$group: {_id: null, totalPrice: {}}}],
        cursor: {},
        jsonSchema: {type: "object", properties: {price: encryptedIntSpec}},
        isRemoteSchema: false,
    };
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
    command = {
        aggregate: coll.getName(),
        pipeline: [{$group: {_id: null, totalPrice: {}, count: {}}}],
        cursor: {},
        jsonSchema: {type: "object", properties: {qty: encryptedStringSpec}},
        isRemoteSchema: false,
    };
    for (let accu of numericAccus) {
        command.pipeline[0].$group = {
            _id: null,
            totalPrice: {[accu]: condExpr},
            count: {[accu]: {$const: 1}}
        };

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

    // Test that fields corresponding to the $first, $last accumulators in $group
    // preserve encryption properties of the expression.
    command = {
        aggregate: coll.getName(),
        pipeline: [
            {$group: {_id: null, representative: {}}},
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
        command.pipeline[0].$group.representative = {[accu]: "$sales.region"};

        cmdRes = assert.commandWorked(testDB.runCommand(command));
        assert.eq(true, cmdRes.hasEncryptionPlaceholders, cmdRes);
        assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);
        assert(cmdRes.hasOwnProperty("result"), cmdRes);
        assert.eq(coll.getName(), cmdRes.result.aggregate, cmdRes);
        assert(cmdRes.result.pipeline[1].$match["representative"].$eq instanceof BinData, cmdRes);
    }

    // Test that accumulator expression aggregating prefixes of encrypted fields fails.
    command = {
        aggregate: coll.getName(),
        pipeline: [{$group: {_id: null, totalPrice: {}}}],
        cursor: {},
        jsonSchema: {
            type: "object",
            properties: {foo: {type: "object", properties: {price: encryptedIntSpec}}}
        },
        isRemoteSchema: false,
    };
    let allAccus = numericAccus.concat(selectionAccus).concat(arrayAccus).concat(["$mergeObjects"]);
    for (let accu of allAccus) {
        command.pipeline[0].$group.totalPrice = {[accu]: "$foo"};
        assert.commandFailedWithCode(testDB.runCommand(command), 31129);
    }

    // Test that the $mergeObjects accumulator expression aggregating not encrypted fields succeeds.
    command = {
        aggregate: coll.getName(),
        pipeline: [{$group: {_id: {$const: null}, combination: {$mergeObjects: "$qty"}}}],
        cursor: {},
        jsonSchema: {type: "object", properties: {otherQty: encryptedStringSpec}},
        isRemoteSchema: false,
    };
    assertCommandUnchanged(command, false, true);

    // Test that the $mergeObjects accumulator expression aggregating encrypted fields fails.
    command = {
        aggregate: coll.getName(),
        pipeline: [{$group: {_id: null, combination: {$mergeObjects: "$qty"}}}],
        cursor: {},
        jsonSchema: {type: "object", properties: {qty: encryptedStringSpec}},
        isRemoteSchema: false,
    };
    assert.commandFailedWithCode(testDB.runCommand(command), 51221);

    mongocryptd.stop();
})();
