/**
 * Test that mongocryptd can correctly mark the $graphLookup agg stage with intent-to-encrypt
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
const coll = testDB.fle_agg_graph_lookup;

const encryptedStringSpec = {
    encrypt: {algorithm: kDeterministicAlgo, keyId: [UUID()], bsonType: "string"}
};

const encryptedIntSpec = {
    encrypt: {algorithm: kDeterministicAlgo, keyId: [UUID()], bsonType: "int"}
};

const encryptedRandomSpec = {
    encrypt: {algorithm: kRandomAlgo, keyId: [UUID()], bsonType: "int"}
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
command = Object.assign({
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
        cursor: {}}, generateSchema({}, coll.getFullName()));
cmdRes = assert.commandWorked(testDB.runCommand(command));
delete command.jsonSchema;
delete command.isRemoteSchema;
delete cmdRes.result.lsid;
assert.eq(command, cmdRes.result, cmdRes);
assert.eq(false, cmdRes.hasEncryptionPlaceholders, cmdRes);
assert.eq(false, cmdRes.schemaRequiresEncryption, cmdRes);

// Test that self-graphlookup with equality match fields encrypted with a deterministic
// algorithm and matching bsonTypes and bringing unencrypted fields succeeds. In FLE 2, comparing
// two encrypted fields is not supported.
command = Object.assign({aggregate: coll.getName(), pipeline: [graphLookupSpec], cursor: {}},
                        generateSchema({name: encryptedStringSpec, reportsTo: encryptedStringSpec},
                                       coll.getFullName()));
if (fle2Enabled()) {
    assert.commandFailedWithCode(testDB.runCommand(command), 6338401);
} else {
    cmdRes = assert.commandWorked(testDB.runCommand(command));
    delete command.jsonSchema;
    delete command.isRemoteSchema;
    delete cmdRes.result.lsid;
    assert.eq(command, cmdRes.result, cmdRes);
    assert.eq(false, cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);
}

// Test that self-graphLookup with 'restrictSearchWithMatch' on a top-level encrypted field is
// correctly marked for encryption.
command = Object.assign({
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
        cursor: {}}, generateSchema({hobbies: encryptedStringSpec}, coll.getFullName()));
cmdRes = assert.commandWorked(testDB.runCommand(command));
assert.eq(true, cmdRes.hasEncryptionPlaceholders, cmdRes);
assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);
assert(cmdRes.hasOwnProperty("result"), cmdRes);
assert.eq(coll.getName(), cmdRes.result.aggregate, cmdRes);
assert(
    cmdRes.result.pipeline[0].$graphLookup.restrictSearchWithMatch.hobbies.$eq instanceof BinData,
    cmdRes);

// Test that self-graphlookup with 'restrictSearchWithMatch' on a nested encrypted field is
// correctly marked for encryption.
command = Object.assign({
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
        cursor: {}}, generateSchema({'personal.hobbies': encryptedStringSpec}, coll.getFullName()));
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
// correctly marked for encryption. FLE 2 does not support patternProperties or
// additionalProperties.
if (!fle2Enabled()) {
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
}

// Test that 'startWith' expression in $graphLookup has constants correctly marked for
// encryption.
command = Object.assign({
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
        cursor: {}}, generateSchema({department: encryptedStringSpec}, coll.getFullName()));

cmdRes = assert.commandWorked(testDB.runCommand(command));
assert.eq(true, cmdRes.hasEncryptionPlaceholders, cmdRes);
assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);
assert(cmdRes.hasOwnProperty("result"), cmdRes);
assert.eq(coll.getName(), cmdRes.result.aggregate, cmdRes);
assert(cmdRes.result.pipeline[0].$graphLookup.startWith.category.$cond[0].$eq[1].$const instanceof
           BinData,
       cmdRes);

// Test that referencing 'as' field from $lookup in subsequent stages fails.
assert.commandFailedWithCode(
    testDB.runCommand(Object.assign(
        {
            aggregate: coll.getName(),
            pipeline: [graphLookupSpec, {$match: {"reportingHierarchy": {$gt: "winterfell"}}}],
            cursor: {}
        },
        generateSchema({reportingHierarchy: encryptedStringSpec}, coll.getFullName()))),
    31133);

// Test that not self-graphlookup fails.
// TODO SERVER-59284: Change expected error code to only be 9710000 after feature flag is enabled by
// default.
assert.commandFailedWithCode(testDB.runCommand(Object.assign({
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
        cursor: {}}, generateSchema({}, coll.getFullName()))),
                                 [51204, 9710000]);

// Test that self-graphlookup when 'connectFromField' has an encrypted child fails.
assert.commandFailedWithCode(
    testDB.runCommand(Object.assign(
        {aggregate: coll.getName(), pipeline: [graphLookupSpec], cursor: {}},
        generateSchema({'reportsTo.group': encryptedStringSpec}, coll.getFullName()))),
    51230);

// Test that self-graphlookup when 'connectToField' has an encrypted child fails.
assert.commandFailedWithCode(
    testDB.runCommand(
        Object.assign({aggregate: coll.getName(), pipeline: [graphLookupSpec], cursor: {}},
                      generateSchema({'name.group': encryptedStringSpec}, coll.getFullName()))),
    51231);

// Test that self-graphlookup with unencrypted 'connectFromField' and encrypted 'connectToField'
// fails.
assert.commandFailedWithCode(
    testDB.runCommand(
        Object.assign({aggregate: coll.getName(), pipeline: [graphLookupSpec], cursor: {}},
                      generateSchema({name: encryptedStringSpec}, coll.getFullName()))),
    [51232, 6331101]);

// Test that self-graphlookup with encrypted 'connectFromField' and unencrypted 'connectToField'
// fails.
assert.commandFailedWithCode(
    testDB.runCommand(
        Object.assign({aggregate: coll.getName(), pipeline: [graphLookupSpec], cursor: {}},
                      generateSchema({reportsTo: encryptedStringSpec}, coll.getFullName()))),
    [51232, 6331101]);

// Test that self-graphlookup with encrypted 'connectFromField' and 'connectToField' with
// different bsonTypes fails.
assert.commandFailedWithCode(
    testDB.runCommand(
        Object.assign({aggregate: coll.getName(), pipeline: [graphLookupSpec], cursor: {}},
                      generateSchema({name: encryptedStringSpec, reportTo: encryptedIntSpec},
                                     coll.getFullName()))),
    [51232, 6331101]);

// Test that self-graphlookup with encrypted 'connectFromField' and 'connectToField' with a
// random algorithm fails.
assert.commandFailedWithCode(
    testDB.runCommand(
        Object.assign({aggregate: coll.getName(), pipeline: [graphLookupSpec], cursor: {}},
                      generateSchema({name: encryptedRandomSpec, reportsTo: encryptedRandomSpec},
                                     coll.getFullName()))),
    [51233, 6338401]);

mongocryptd.stop();
