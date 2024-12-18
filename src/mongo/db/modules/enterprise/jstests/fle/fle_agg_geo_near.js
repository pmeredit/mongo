/**
 * Test that mongocryptd can correctly mark the $geoNear agg stage with intent-to-encrypt
 * placeholders.
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
const coll = testDB.fle_agg_geo_near;

const encryptedStringSpec = {
    encrypt: {algorithm: kDeterministicAlgo, keyId: [UUID()], bsonType: "string"}
};

let command, cmdRes;

// Test that $geoNear does not get affected when no encryption is required by the schema.
command = Object.assign({
    aggregate: coll.getName(),
    pipeline: [{
        $geoNear: {
            key: "location",
            near:
                {type: {$const: "Point"}, coordinates: [{$const: -73.99279}, {$const: 40.719296}]},
            distanceField: "dist.calculated",
            maxDistance: 10,
            minDistance: 2,
            query: {category: {"$eq": "public"}},
            spherical: true,
            includeLocs: "dist.location"
        }
    }],
    cursor: {}
},
                        generateSchema({}, coll.getFullName()));
cmdRes = assert.commandWorked(testDB.runCommand(command));
delete command.jsonSchema;
delete command.isRemoteSchema;
delete cmdRes.result.lsid;
assert.eq(command, cmdRes.result, cmdRes);
assert.eq(false, cmdRes.hasEncryptionPlaceholders, cmdRes);
assert.eq(false, cmdRes.schemaRequiresEncryption, cmdRes);

// Test that $geoNear which does not reference an encrypted field is correctly reflected
// back from mongocryptd.
command = Object.assign(command, generateSchema({foo: encryptedStringSpec}, coll.getFullName()));
cmdRes = assert.commandWorked(testDB.runCommand(command));
delete command.jsonSchema;
delete command.isRemoteSchema;
delete cmdRes.result.lsid;
assert.eq(command, cmdRes.result, cmdRes);
assert.eq(false, cmdRes.hasEncryptionPlaceholders, cmdRes);
assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);

// Test that $geoNear with a 'query' on a top-level encrypted field is correctly
// marked for encryption.
command =
    Object.assign(command, generateSchema({category: encryptedStringSpec}, coll.getFullName()));
cmdRes = assert.commandWorked(testDB.runCommand(command));
assert.eq(true, cmdRes.hasEncryptionPlaceholders, cmdRes);
assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);
assert(cmdRes.hasOwnProperty("result"), cmdRes);
assert.eq(coll.getName(), cmdRes.result.aggregate, cmdRes);
assert(cmdRes.result.pipeline[0].$geoNear.query.category.$eq instanceof BinData, cmdRes);

// Test that $geoNear with a 'query' on a nested encrypted field is correctly marked for
// encryption.
command.pipeline = [{
    $geoNear: {
        near: {type: "Point", coordinates: [-73.99279, 40.719296]},
        distanceField: "dist.calculated",
        maxDistance: 10,
        minDistance: 2,
        query: {"category.group": "public"},
        spherical: true,
        includeLocs: "dist.location"
    }
}];
command = Object.assign(
    command, generateSchema({'category.group': encryptedStringSpec}, coll.getFullName()));
cmdRes = assert.commandWorked(testDB.runCommand(command));
assert.eq(true, cmdRes.hasEncryptionPlaceholders, cmdRes);
assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);
assert(cmdRes.hasOwnProperty("result"), cmdRes);
assert.eq(coll.getName(), cmdRes.result.aggregate, cmdRes);
assert(cmdRes.result.pipeline[0].$geoNear.query["category.group"].$eq instanceof BinData, cmdRes);

// Test that $geoNear with a 'query' on a matching patternProperty is correctly marked for
// encryption. Both patternProperties and additionalProperties are FLE1-specific features.
if (fle2Enabled()) {
    command = {
        aggregate: coll.getName(),
        pipeline: [{
            $geoNear: {
                near: {type: "Point", coordinates: [-73.99279, 40.719296]},
                distanceField: "dist.calculated",
                maxDistance: 10,
                minDistance: 2,
                query: {category: "public"},
                spherical: true,
                includeLocs: "dist.location"
            }
        }],
        cursor: {},
        jsonSchema: {type: "object", patternProperties: {cat: encryptedStringSpec}},
        isRemoteSchema: false,
    };
    cmdRes = assert.commandWorked(testDB.runCommand(command));
    assert.eq(true, cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);
    assert(cmdRes.hasOwnProperty("result"), cmdRes);
    assert.eq(coll.getName(), cmdRes.result.aggregate, cmdRes);
    assert(cmdRes.result.pipeline[0].$geoNear.query.category.$eq instanceof BinData, cmdRes);

    // Test that $geoNear with a 'query' on an additionalProperty is correctly marked for
    // encryption.
    command = {
        aggregate: coll.getName(),
        pipeline: [{
            $geoNear: {
                near: {type: "Point", coordinates: [-73.99279, 40.719296]},
                distanceField: "dist.calculated",
                maxDistance: 10,
                minDistance: 2,
                query: {category: "public"},
                spherical: true,
                includeLocs: "dist.location"
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
    assert(cmdRes.result.pipeline[0].$geoNear.query.category.$eq instanceof BinData, cmdRes);
}

// Test that $geoNear with 'distanceField' that overrides an encrypted schema subtree, marks
// this field as not encrypted.
command = Object.assign({
    aggregate: coll.getName(),
    pipeline: [
        {
            $geoNear: {
                near: {type: "Point", coordinates: [-73.99279, 40.719296]},
                distanceField: "dist",
                minDistance: 2,
                spherical: true
            }
        },
        {$match: {"dist": "winterfell"}}
    ],
    cursor: {}
},
                        generateSchema({dist: encryptedStringSpec}, coll.getFullName()));
cmdRes = assert.commandWorked(testDB.runCommand(command));
assert.eq(false, cmdRes.hasEncryptionPlaceholders, cmdRes);
assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);
assert(cmdRes.hasOwnProperty("result"), cmdRes);
assert.eq(coll.getName(), cmdRes.result.aggregate, cmdRes);
assert.eq(cmdRes.result.pipeline[1].$match["dist"].$eq, "winterfell", cmdRes);

// Test that $geoNear with 'includeLocs' that overrides an encrypted schema subtree, marks this
// field as not encrypted.
command = Object.assign({
    aggregate: coll.getName(),
    pipeline: [
        {
            $geoNear: {
                near: {type: "Point", coordinates: [-73.99279, 40.719296]},
                minDistance: 2,
                spherical: true,
                includeLocs: "location"
            },
        },
        {$match: {"location": {$gt: "winterfell"}}}
    ],
    cursor: {}
},
                        generateSchema({location: encryptedStringSpec}, coll.getFullName()));
cmdRes = assert.commandWorked(testDB.runCommand(command));
assert.eq(false, cmdRes.hasEncryptionPlaceholders, cmdRes);
assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);
assert(cmdRes.hasOwnProperty("result"), cmdRes);
assert.eq(coll.getName(), cmdRes.result.aggregate, cmdRes);
assert.eq(cmdRes.result.pipeline[1].$match["location"].$gt, "winterfell", cmdRes);

// Test that $geoNear with an encrypted 'key' field fails.
assert.commandFailedWithCode(
    testDB.runCommand(
        Object.assign({
            aggregate: coll.getName(),
            pipeline: [{
                $geoNear: {
                    near: {type: "Point", coordinates: [-73.98142, 40.71782]},
                    key: "location",
                    distanceField: "dist.calculated",
                    query: {"category": "Parks"}
                }
            }],
            cursor: {}
        },
                      generateSchema({location: encryptedStringSpec}, coll.getFullName()))),
    51212);

// Test that $geoNear with a simple 'key' field that is a prefix of an encrypted field fails.
assert.commandFailedWithCode(
    testDB.runCommand(
        Object.assign({
            aggregate: coll.getName(),
            pipeline: [{
                $geoNear: {
                    near: {type: "Point", coordinates: [-73.98142, 40.71782]},
                    key: "location",
                    distanceField: "dist.calculated",
                    query: {"category": "Parks"}
                }
            }],
            cursor: {}
        },
                      generateSchema({'location.city': encryptedStringSpec}, coll.getFullName()))),
    51212);

// Test that $geoNear with a nested 'key' field that is a prefix of an encrypted field fails.
assert.commandFailedWithCode(
    testDB.runCommand(
        Object.assign({
            aggregate: coll.getName(),
            pipeline: [{
                $geoNear: {
                    near: {type: "Point", coordinates: [-73.98142, 40.71782]},
                    key: "location.city.district",
                    distanceField: "dist.calculated",
                    query: {"category": "Parks"}
                }
            }],
            cursor: {}
        },
                      generateSchema({'location.city': encryptedStringSpec}, coll.getFullName()))),
    51102);

mongocryptd.stop();
