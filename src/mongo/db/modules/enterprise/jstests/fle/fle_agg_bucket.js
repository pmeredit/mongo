/**
 * Test that mongocryptd can correctly mark the $bucket agg stage with intent-to-encrypt
 * placeholders.
 */
(function() {
    "use strict";

    load("src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js");

    const mongocryptd = new MongoCryptD();
    mongocryptd.start();
    const conn = mongocryptd.getConnection();
    const testDB = conn.getDB("test");
    const coll = testDB.fle_agg_bucket;

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

    let command, cmdRes, expectedResult;

    // Test that $bucket operating on not encrypted fields is not affected except for the standard
    // translation to $group and $sort.
    command = {
        aggregate: coll.getName(),
        pipeline: [{
            $bucket: {
                groupBy: "$price",
                boundaries: [0, 200],
                default: "Other",
                output: {"count": {$sum: 1}, "artwork": {$push: "$title"}}
            }
        }],
        cursor: {},
        jsonSchema: {type: "object", properties: {foo: encryptedStringSpec}},
        isRemoteSchema: false
    };
    cmdRes = assert.commandWorked(testDB.runCommand(command));
    delete cmdRes.result.lsid;
    expectedResult = {
        aggregate: coll.getName(),
        pipeline: [
            {
              "$group": {
                  "_id": {
                      "$switch": {
                          "branches": [{
                              "case": {
                                  "$and": [
                                      {"$gte": ["$price", {"$const": 0}]},
                                      {"$lt": ["$price", {"$const": 200}]}
                                  ]
                              },
                              "then": {"$const": 0}
                          }],
                          "default": {"$const": "Other"}
                      }
                  },
                  "count": {"$sum": {"$const": 1}},
                  "artwork": {"$push": "$title"}
              }
            },
            {"$sort": {"_id": 1}}
        ],
        "cursor": {}
    };
    assert.eq(expectedResult, cmdRes.result, cmdRes);
    assert.eq(false, cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);

    // Test that $bucket operating on not encrypted fields marks the projected fields as not
    // encrypted.
    command = {
        aggregate: coll.getName(),
        pipeline: [
            {
              $bucket: {
                  groupBy: "$price",
                  boundaries: [0, 200],
                  default: "Other",
                  output: {"artwork": {$push: "$title"}}
              }
            },
            {$match: {$and: [{_id: {$eq: "winterfell"}}, {artwork: {$eq: "winterfell"}}]}}
        ],
        cursor: {},
        jsonSchema: {type: "object", properties: {notdate: encryptedStringSpec}},
        isRemoteSchema: false
    };
    cmdRes = assert.commandWorked(testDB.runCommand(command));
    delete cmdRes.result.lsid;
    expectedResult = {
        aggregate: coll.getName(),
        pipeline: [
            {
              "$group": {
                  "_id": {
                      "$switch": {
                          "branches": [{
                              "case": {
                                  "$and": [
                                      {"$gte": ["$price", {"$const": 0}]},
                                      {"$lt": ["$price", {"$const": 200}]}
                                  ]
                              },
                              "then": {"$const": 0}
                          }],
                          "default": {"$const": "Other"}
                      }
                  },
                  "artwork": {"$push": "$title"}
              }
            },
            {"$sort": {"_id": 1}},
            {$match: {$and: [{_id: {$eq: "winterfell"}}, {artwork: {$eq: "winterfell"}}]}}
        ],
        "cursor": {}
    };
    assert.eq(expectedResult, cmdRes.result, cmdRes);
    assert.eq(false, cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);

    // Test that $bucketAuto operating with a 'groupBy' expression that returns an unencrypted
    // result succeeds and marks encrypted fields in that expression appropriately.
    command = {
        aggregate: coll.getName(),
        pipeline: [
            {
              $bucket: {
                  groupBy: {
                      $cond: [
                          {$eq: ["$ssn", {$const: "123-12-1212"}]},
                          {$const: "unencrypted1"},
                          {$const: "unencrypted2"}
                      ]
                  },
                  boundaries: [0, 200],
                  default: "Other",
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
    var branch = cmdRes.result.pipeline[0].$group._id.$switch.branches[0];
    assert(branch.case.$and[0].$gte[0].$cond[0].$eq[1].$const instanceof BinData, cmdRes);

    // Test that $bucket with 'groupBy' on an encrypted field fails.
    command = {
        aggregate: coll.getName(),
        pipeline: [{
            $bucket: {
                groupBy: "$price",
                boundaries: [0, 200],
                default: "Other",
                output: {"artwork": {$push: "$title"}}
            }
        }],
        cursor: {},
        jsonSchema: {type: "object", properties: {price: encryptedStringSpec}},
        isRemoteSchema: false
    };
    assert.commandFailedWithCode(testDB.runCommand(command), 31110);

    // Test that the $addToSet accumulator in $bucket succeeds if the output type is stable and
    // its output schema has only deterministic nodes.
    command = {
        aggregate: coll.getName(),
        pipeline: [{
            $bucket: {
                groupBy: "$price",
                boundaries: [0, 200],
                output: {distinctQuantities: {$addToSet: "$qty"}}
            }
        }],
        cursor: {},
        jsonSchema: {type: "object", properties: {qty: encryptedStringSpec}},
        isRemoteSchema: false,
    };
    cmdRes = assert.commandWorked(testDB.runCommand(command));
    delete cmdRes.result.lsid;
    expectedResult = {
        aggregate: coll.getName(),
        pipeline: [
            {
              "$group": {
                  "_id": {
                      "$switch": {
                          "branches": [{
                              "case": {
                                  "$and": [
                                      {"$gte": ["$price", {"$const": 0}]},
                                      {"$lt": ["$price", {"$const": 200}]}
                                  ]
                              },
                              "then": {"$const": 0}
                          }]
                      }
                  },
                  "distinctQuantities": {$addToSet: "$qty"}
              }
            },
            {"$sort": {"_id": 1}}
        ],
        "cursor": {}
    };
    assert.eq(expectedResult, cmdRes.result, cmdRes);
    assert.eq(false, cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);

    // Test that fields corresponding to accumulator expressions with array accumulators over
    // not encrypted fields in $bucket can be referenced in the query and are marked as not
    // encrypted.
    command = {
        aggregate: coll.getName(),
        pipeline: [
            {$bucket: {groupBy: "$price", boundaries: [0, 200], output: {itemList: {}}}},
            {$match: {"itemList": {$eq: "winterfell"}}}
        ],
        cursor: {}
    };
    expectedResult = {
        aggregate: coll.getName(),
        pipeline: [
            {
              "$group": {
                  "_id": {
                      "$switch": {
                          "branches": [{
                              "case": {
                                  "$and": [
                                      {"$gte": ["$price", {"$const": 0}]},
                                      {"$lt": ["$price", {"$const": 200}]}
                                  ]
                              },
                              "then": {"$const": 0}
                          }]
                      }
                  },
                  "itemList": {}
              }
            },
            {"$sort": {"_id": 1}},
            {$match: {"itemList": {$eq: "winterfell"}}}
        ],
        "cursor": {}
    };
    let arrayAccus = ["$addToSet", "$push"];
    for (let accu of arrayAccus) {
        command.pipeline[0].$bucket.output.itemList = {[accu]: "$item"};
        command.jsonSchema = {type: "object", properties: {notitem: encryptedStringSpec}};
        command.isRemoteSchema = false;

        expectedResult.pipeline[0].$group.itemList = {[accu]: "$item"};

        cmdRes = assert.commandWorked(testDB.runCommand(command));
        delete cmdRes.result.lsid;
        assert.eq(expectedResult, cmdRes.result, cmdRes);
        assert.eq(false, cmdRes.hasEncryptionPlaceholders, cmdRes);
        assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);
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
            {
              $bucket:
                  {groupBy: "$time", boundaries: [0, 200], output: {totalPrice: {}, count: {}}}
            },
            {$match: {"itemList": {$eq: "winterfell"}}}
        ],
        cursor: {},
        jsonSchema: {type: "object", properties: {qty: encryptedStringSpec}},
        isRemoteSchema: false,
    };
    let numericAccus = ["$sum", "$min", "$max", "$avg", "$stdDevPop", "$stdDevSamp"];
    for (let accu of numericAccus) {
        command.pipeline[0].$bucket.output.totalPrice = {[accu]: condExpr};
        command.pipeline[0].$bucket.output.count = {[accu]: {$const: 1}};

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

    // Test that fields corresponding to the $first, $last accumulators in $bucket
    // preserve encryption properties of the expression.
    command = {
        aggregate: coll.getName(),
        pipeline: [
            {$bucket: {groupBy: "$time", boundaries: [0, 200], output: {representative: {}}}},
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
        command.pipeline[0].$bucket.output.representative = {[accu]: "$sales.region"};

        cmdRes = assert.commandWorked(testDB.runCommand(command));
        assert.eq(true, cmdRes.hasEncryptionPlaceholders, cmdRes);
        assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);
        assert(cmdRes.hasOwnProperty("result"), cmdRes);
        assert.eq(coll.getName(), cmdRes.result.aggregate, cmdRes);
        assert(cmdRes.result.pipeline[2].$match["representative"].$eq instanceof BinData, cmdRes);
    }

    // Test that the $mergeObjects accumulator expression aggregating not encrypted fields succeeds.
    command = {
        aggregate: coll.getName(),
        pipeline: [{
            $bucket: {
                groupBy: "$price",
                boundaries: [0, 200],
                output: {combination: {$mergeObjects: "$qty"}}
            }
        }],
        cursor: {},
        jsonSchema: {type: "object", properties: {otherQty: encryptedStringSpec}},
        isRemoteSchema: false
    };
    expectedResult = {
        aggregate: coll.getName(),
        pipeline: [
            {
              "$group": {
                  "_id": {
                      "$switch": {
                          "branches": [{
                              "case": {
                                  "$and": [
                                      {"$gte": ["$price", {"$const": 0}]},
                                      {"$lt": ["$price", {"$const": 200}]}
                                  ]
                              },
                              "then": {"$const": 0}
                          }]
                      }
                  },
                  "combination": {$mergeObjects: "$qty"}
              }
            },
            {"$sort": {"_id": 1}}
        ],
        "cursor": {}
    };
    cmdRes = assert.commandWorked(testDB.runCommand(command));
    delete cmdRes.result.lsid;
    assert.eq(expectedResult, cmdRes.result, cmdRes);
    assert.eq(false, cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);

    mongocryptd.stop();
})();
