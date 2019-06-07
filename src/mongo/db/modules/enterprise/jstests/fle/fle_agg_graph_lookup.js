/**
 * Test that mongocryptd can correctly mark the $graphLookup agg stage with intent-to-encrypt
 * placeholders.
 */
(function() {
    "use strict";

    load("src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js");

    const mongocryptd = new MongoCryptD();
    mongocryptd.start();
    const conn = mongocryptd.getConnection();
    const testDB = conn.getDB("test");
    const coll = testDB.fle_agg_graph_lookup;

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

    const graphLookupSpec = {
        $graphLookup: {
            from: coll.getName(),
            as: "reportingHierarchy",
            connectToField: "name",
            connectFromField: "reportsTo",
            startWith: "$reportsTo"
        }
    };

    let command, cmdRes;

    // Test that self-graphlookup with unencrypted equality match fields, unencrypted 'startWith'
    // field and bringing unencrypted fields succeeds.
    command = {
        aggregate: coll.getName(),
        pipeline: [{
            $graphLookup: {
                from: coll.getName(),
                as: "reportingHierarchy",
                connectToField: "name",
                connectFromField: "reportsTo",
                startWith: "$reportsTo",
                depthField: "degree",
                maxDepth: NumberLong(5),
                restrictSearchWithMatch: {"hobbies": {"$eq": "golf"}}
            }
        }],
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

    // Test that self-graphlookup with equality match fields encrypted with a deterministic
    // algorithm and matching bsonTypes and bringing unencrypted fields succeeds.
    command = {
        aggregate: coll.getName(),
        pipeline: [graphLookupSpec],
        cursor: {},
        jsonSchema: {
            type: "object",
            properties: {name: encryptedStringSpec, reportsTo: encryptedStringSpec}
        },
        isRemoteSchema: false,
    };
    cmdRes = assert.commandWorked(testDB.runCommand(command));
    delete command.jsonSchema;
    delete command.isRemoteSchema;
    delete cmdRes.result.lsid;
    assert.eq(command, cmdRes.result, cmdRes);
    assert.eq(false, cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);

    // Test that self-graphLookup with 'restrictSearchWithMatch' on a top-level encrypted field is
    // correctly marked for encryption.
    command = {
        aggregate: coll.getName(),
        pipeline: [{
            $graphLookup: {
                from: coll.getName(),
                as: "reportingHierarchy",
                connectToField: "name",
                connectFromField: "reportsTo",
                startWith: "$reportsTo",
                restrictSearchWithMatch: {"hobbies": "golf"}
            }
        }],
        cursor: {},
        jsonSchema: {type: "object", properties: {hobbies: encryptedStringSpec}},
        isRemoteSchema: false,
    };
    cmdRes = assert.commandWorked(testDB.runCommand(command));
    assert.eq(true, cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);
    assert(cmdRes.hasOwnProperty("result"), cmdRes);
    assert.eq(coll.getName(), cmdRes.result.aggregate, cmdRes);
    assert(cmdRes.result.pipeline[0].$graphLookup.restrictSearchWithMatch.hobbies.$eq instanceof
               BinData,
           cmdRes);

    // Test that self-graphlookup with 'restrictSearchWithMatch' on a nested encrypted field is
    // correctly marked for encryption.
    command = {
        aggregate: coll.getName(),
        pipeline: [{
            $graphLookup: {
                from: coll.getName(),
                as: "reportingHierarchy",
                connectToField: "name",
                connectFromField: "reportsTo",
                startWith: "$reportsTo",
                restrictSearchWithMatch: {"personal.hobbies": "golf"}
            }
        }],
        cursor: {},
        jsonSchema: {
            type: "object",
            properties: {personal: {type: "object", properties: {hobbies: encryptedStringSpec}}}
        },
        isRemoteSchema: false,
    };
    cmdRes = assert.commandWorked(testDB.runCommand(command));
    assert.eq(true, cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);
    assert(cmdRes.hasOwnProperty("result"), cmdRes);
    assert.eq(coll.getName(), cmdRes.result.aggregate, cmdRes);
    assert(cmdRes.result.pipeline[0]
                   .$graphLookup.restrictSearchWithMatch["personal.hobbies"]
                   .$eq instanceof
               BinData,
           cmdRes);

    // Test that self-graphlookup with 'restrictSearchWithMatch' on a matching patternProperty is
    // correctly marked for encryption.
    command = {
        aggregate: coll.getName(),
        pipeline: [{
            $graphLookup: {
                from: coll.getName(),
                as: "reportingHierarchy",
                connectToField: "name",
                connectFromField: "reportsTo",
                startWith: "$reportsTo",
                restrictSearchWithMatch: {"hobbies": "golf"}
            }
        }],
        cursor: {},
        jsonSchema: {type: "object", patternProperties: {hob: encryptedStringSpec}},
        isRemoteSchema: false,
    };
    cmdRes = assert.commandWorked(testDB.runCommand(command));
    assert.eq(true, cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);
    assert(cmdRes.hasOwnProperty("result"), cmdRes);
    assert.eq(coll.getName(), cmdRes.result.aggregate, cmdRes);
    assert(cmdRes.result.pipeline[0].$graphLookup.restrictSearchWithMatch.hobbies.$eq instanceof
               BinData,
           cmdRes);

    // Test that self-graphlookup with 'restrictSearchWithMatch' on an additionalProperty is
    // correctly marked for encryption.
    command = {
        aggregate: coll.getName(),
        pipeline: [{
            $graphLookup: {
                from: coll.getName(),
                as: "reportingHierarchy",
                connectToField: "name",
                connectFromField: "reportsTo",
                startWith: "$reportsTo",
                restrictSearchWithMatch: {"hobbies": "golf"}
            }
        }],
        cursor: {},
        jsonSchema: {type: "object", properties: {}, additionalProperties: encryptedStringSpec},
        isRemoteSchema: false,
    };
    cmdRes = assert.commandWorked(testDB.runCommand(command));
    assert.eq(true, cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);
    assert(cmdRes.hasOwnProperty("result"), cmdRes);
    assert.eq(coll.getName(), cmdRes.result.aggregate, cmdRes);
    assert(cmdRes.result.pipeline[0].$graphLookup.restrictSearchWithMatch.hobbies.$eq instanceof
               BinData,
           cmdRes);

    // Test that 'startWith' expression in $graphLookup has constants correctly marked for
    // encryption.
    command = {
        aggregate: coll.getName(),
        pipeline: [{
            $graphLookup: {
                from: coll.getName(),
                as: "reportingHierarchy",
                connectToField: "name",
                connectFromField: "reportsTo",
                startWith: {
                    category: {
                        $cond: [
                            {$eq: ["$department", {$const: "IT"}]},
                            "$reportsTo1",
                            "$reportsTo2"
                        ]
                    }
                },
            }
        }],
        cursor: {},
        jsonSchema: {type: "object", properties: {department: encryptedStringSpec}},
        isRemoteSchema: false
    };
    cmdRes = assert.commandWorked(testDB.runCommand(command));
    assert.eq(true, cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);
    assert(cmdRes.hasOwnProperty("result"), cmdRes);
    assert.eq(coll.getName(), cmdRes.result.aggregate, cmdRes);
    assert(
        cmdRes.result.pipeline[0].$graphLookup.startWith.category.$cond[0].$eq[1].$const instanceof
            BinData,
        cmdRes);

    // Test that referencing 'as' field from $lookup in subsequent stages fails.
    assert.commandFailedWithCode(testDB.runCommand({
        aggregate: coll.getName(),
        pipeline: [graphLookupSpec, {$match: {"reportingHierarchy": {$gt: "winterfell"}}}],
        cursor: {},
        jsonSchema: {type: "object", properties: {}, additionalProperties: encryptedStringSpec},
        isRemoteSchema: false
    }),
                                 31133);

    // Test that not self-graphlookup fails.
    assert.commandFailedWithCode(testDB.runCommand({
        aggregate: coll.getName(),
        pipeline: [{
            $graphLookup: {
                from: "employee",
                as: "reportingHierarchy",
                connectToField: "name",
                connectFromField: "reportsTo",
                startWith: "$reportsTo"
            }
        }],
        cursor: {},
        jsonSchema: {},
        isRemoteSchema: false,
    }),
                                 51204);

    // Test that self-graphlookup when 'connectFromField' has an encrypted child fails.
    assert.commandFailedWithCode(testDB.runCommand({
        aggregate: coll.getName(),
        pipeline: [graphLookupSpec],
        cursor: {},
        jsonSchema: {
            type: "object",
            properties: {reportsTo: {type: "object", properties: {group: encryptedStringSpec}}}
        },
        isRemoteSchema: false,
    }),
                                 51230);

    // Test that self-graphlookup when 'connectToField' has an encrypted child fails.
    assert.commandFailedWithCode(testDB.runCommand({
        aggregate: coll.getName(),
        pipeline: [graphLookupSpec],
        cursor: {},
        jsonSchema: {
            type: "object",
            properties: {name: {type: "object", properties: {group: encryptedStringSpec}}}
        },
        isRemoteSchema: false,
    }),
                                 51231);

    // Test that self-graphlookup with unencrypted 'connectFromField' and encrypted 'connectToField'
    // fails.
    assert.commandFailedWithCode(testDB.runCommand({
        aggregate: coll.getName(),
        pipeline: [graphLookupSpec],
        cursor: {},
        jsonSchema: {type: "object", properties: {name: encryptedStringSpec}},
        isRemoteSchema: false,
    }),
                                 51232);

    // Test that self-graphlookup with encrypted 'connectFromField' and unencrypted 'connectToField'
    // fails.
    assert.commandFailedWithCode(testDB.runCommand({
        aggregate: coll.getName(),
        pipeline: [graphLookupSpec],
        cursor: {},
        jsonSchema: {type: "object", properties: {reportsTo: encryptedStringSpec}},
        isRemoteSchema: false,
    }),
                                 51232);

    // Test that self-graphlookup with encrypted 'connectFromField' and 'connectToField' with
    // different bsonTypes fails.
    assert.commandFailedWithCode(testDB.runCommand({
        aggregate: coll.getName(),
        pipeline: [graphLookupSpec],
        cursor: {},
        jsonSchema:
            {type: "object", properties: {name: encryptedStringSpec, reportTo: encryptedIntSpec}},
        isRemoteSchema: false,
    }),
                                 51232);

    // Test that self-graphlookup with encrypted 'connectFromField' and 'connectToField' with a
    // random algorithm fails.
    assert.commandFailedWithCode(testDB.runCommand({
        aggregate: coll.getName(),
        pipeline: [graphLookupSpec],
        cursor: {},
        jsonSchema: {
            type: "object",
            properties: {name: encryptedRandomSpec, reportsTo: encryptedRandomSpec}
        },
        isRemoteSchema: false,
    }),
                                 51233);

    mongocryptd.stop();
})();
