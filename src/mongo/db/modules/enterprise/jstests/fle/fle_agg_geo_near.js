/**
 * Test that mongocrypt can correctly mark the $geoNear agg stage with intent-to-encrypt
 * placeholders.
 */
(function() {
    "use strict";

    load("src/mongo/db/modules/enterprise/jstests/fle/lib/mongocryptd.js");

    const mongocryptd = new MongoCryptD();
    mongocryptd.start();
    const conn = mongocryptd.getConnection();
    const testDB = conn.getDB("test");
    const coll = testDB.fle_agg_geo_near;

    const encryptedStringSpec = {
        encrypt: {
            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
            keyId: [UUID()],
            bsonType: "string"
        }
    };

    let command, cmdRes;

    // Test that $geoNear does not get affected when no encryption is required by the schema.
    command = {
        aggregate: coll.getName(),
        pipeline: [{
            $geoNear: {
                key: "location",
                near: {type: "Point", coordinates: [-73.99279, 40.719296]},
                distanceField: "dist.calculated",
                maxDistance: 10,
                minDistance: 2,
                query: {category: {"$eq": "public"}},
                spherical: true,
                includeLocs: "dist.location"
            }
        }],
        cursor: {},
        jsonSchema: {}
    };
    cmdRes = assert.commandWorked(testDB.runCommand(command));
    delete command.jsonSchema;
    delete cmdRes.result.lsid;
    assert.eq(command, cmdRes.result, cmdRes);
    assert.eq(false, cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert.eq(false, cmdRes.schemaRequiresEncryption, cmdRes);

    // Test that $geoNear which does not reference an encrypted field is correctly reflected
    // back from mongocryptd.
    command = {
        aggregate: coll.getName(),
        pipeline: [{
            $geoNear: {
                near: {type: "Point", coordinates: [-73.99279, 40.719296]},
                distanceField: "dist.calculated",
                maxDistance: 10,
                minDistance: 2,
                query: {category: {"$eq": "public"}},
                spherical: true,
                includeLocs: "dist.location"
            }
        }],
        cursor: {},
        jsonSchema: {type: "object", properties: {foo: encryptedStringSpec}},
    };
    cmdRes = assert.commandWorked(testDB.runCommand(command));
    delete command.jsonSchema;
    delete cmdRes.result.lsid;
    assert.eq(command, cmdRes.result, cmdRes);
    assert.eq(false, cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);

    // Test that $geoNear with a query on a top-level encrypted field is correctly
    // marked for encryption.
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
        jsonSchema: {type: "object", properties: {category: encryptedStringSpec}}
    };
    cmdRes = assert.commandWorked(testDB.runCommand(command));
    assert.eq(true, cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);
    assert(cmdRes.hasOwnProperty("result"), cmdRes);
    assert.eq(coll.getName(), cmdRes.result.aggregate, cmdRes);
    assert(cmdRes.result.pipeline[0].$geoNear.query.category.$eq instanceof BinData, cmdRes);

    // Test that $geoNear with a query on a nested encrypted field is correctly marked for
    // encryption.
    command = {
        aggregate: coll.getName(),
        pipeline: [{
            $geoNear: {
                near: {type: "Point", coordinates: [-73.99279, 40.719296]},
                distanceField: "dist.calculated",
                maxDistance: 10,
                minDistance: 2,
                query: {"category.group": "public"},
                spherical: true,
                includeLocs: "dist.location"
            }
        }],
        cursor: {},
        jsonSchema: {
            type: "object",
            properties: {category: {type: "object", properties: {group: encryptedStringSpec}}}
        }
    };
    cmdRes = assert.commandWorked(testDB.runCommand(command));
    assert.eq(true, cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);
    assert(cmdRes.hasOwnProperty("result"), cmdRes);
    assert.eq(coll.getName(), cmdRes.result.aggregate, cmdRes);
    assert(cmdRes.result.pipeline[0].$geoNear.query["category.group"].$eq instanceof BinData,
           cmdRes);

    // Test that $geoNear with a query on a matching patternProperty is correctly marked for
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
        jsonSchema: {type: "object", patternProperties: {cat: encryptedStringSpec}}
    };
    cmdRes = assert.commandWorked(testDB.runCommand(command));
    assert.eq(true, cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);
    assert(cmdRes.hasOwnProperty("result"), cmdRes);
    assert.eq(coll.getName(), cmdRes.result.aggregate, cmdRes);
    assert(cmdRes.result.pipeline[0].$geoNear.query.category.$eq instanceof BinData, cmdRes);

    // Test that $geoNear with a query on an additionalProperty is correctly marked for encryption.
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
        jsonSchema: {type: "object", properties: {}, additionalProperties: encryptedStringSpec}
    };
    cmdRes = assert.commandWorked(testDB.runCommand(command));
    assert.eq(true, cmdRes.hasEncryptionPlaceholders, cmdRes);
    assert.eq(true, cmdRes.schemaRequiresEncryption, cmdRes);
    assert(cmdRes.hasOwnProperty("result"), cmdRes);
    assert.eq(coll.getName(), cmdRes.result.aggregate, cmdRes);
    assert(cmdRes.result.pipeline[0].$geoNear.query.category.$eq instanceof BinData, cmdRes);

    // Test that $geoNear with an encrypted key field fails.
    assert.commandFailedWithCode(testDB.runCommand({
        aggregate: coll.getName(),
        pipeline: [{
            $geoNear: {
                near: {type: "Point", coordinates: [-73.98142, 40.71782]},
                key: "location",
                distanceField: "dist.calculated",
                query: {"category": "Parks"}
            }
        }],
        cursor: {},
        jsonSchema: {type: "object", properties: {location: encryptedStringSpec}}
    }),
                                 51200);

    // Test that $geoNear with a simple key field that is a prefix of an encrypted field fails.
    assert.commandFailedWithCode(testDB.runCommand({
        aggregate: coll.getName(),
        pipeline: [{
            $geoNear: {
                near: {type: "Point", coordinates: [-73.98142, 40.71782]},
                key: "location",
                distanceField: "dist.calculated",
                query: {"category": "Parks"}
            }
        }],
        cursor: {},
        jsonSchema: {
            type: "object",
            properties: {location: {type: "object", properties: {city: encryptedStringSpec}}}
        }
    }),
                                 51200);

    // Test that $geoNear with a nested key field that is a prefix of an encrypted field fails.
    assert.commandFailedWithCode(testDB.runCommand({
        aggregate: coll.getName(),
        pipeline: [{
            $geoNear: {
                near: {type: "Point", coordinates: [-73.98142, 40.71782]},
                key: "location.city.district",
                distanceField: "dist.calculated",
                query: {"category": "Parks"}
            }
        }],
        cursor: {},
        jsonSchema: {
            type: "object",
            properties: {location: {type: "object", properties: {city: encryptedStringSpec}}}
        }
    }),
                                 51102);

    mongocryptd.stop();
})();
