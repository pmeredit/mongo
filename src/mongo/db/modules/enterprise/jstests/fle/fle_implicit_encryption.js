/**
 * Check the functionality of query functions with encryption.
 *
 * TODO SERVER-64213 Enable end to end testing
 * @tags: [unsupported_fle_2]
 */

load('jstests/ssl/libs/ssl_helpers.js');
load("src/mongo/db/modules/enterprise/jstests/fle/lib/utils.js");

(function() {
"use strict";

const x509_options = {
    sslMode: "requireSSL",
    sslPEMKeyFile: SERVER_CERT,
    sslCAFile: CA_CERT,
    vvvvv: ""
};

const conn = MongoRunner.runMongod(x509_options);
const unencryptedDatabase = conn.getDB("test");

const localKMS = {
    key: BinData(
        0,
        "tu9jUCBqZdwCelwE/EAm/4WqdxrSMi04B8e9uAV+m30rI1J2nhKZZtQjdvsSCwuI4erR6IEcEK+5eGUAODv43NDNIR9QheT2edWFewUfHKsl9cnzTc86meIzOmYl6drp"),
};

const clientSideRemoteSchemaFLEOptions = {
    kmsProviders: {
        local: localKMS,
    },
    keyVaultNamespace: "test.keystore",
    schemaMap: {},
};

var encryptedShell = Mongo(conn.host, clientSideRemoteSchemaFLEOptions);
var keyVault = encryptedShell.getKeyVault();

keyVault.createKey("local", ['studentsKey']);
keyVault.createKey("local", ['teachersKey']);
const studentsKeyId = keyVault.getKeyByAltName("studentsKey").toArray()[0]._id;
const teachersKeyId = keyVault.getKeyByAltName("teachersKey").toArray()[0]._id;

var encryptedDatabase = encryptedShell.getDB("test");

let testRandomizedCollection = (keyId, encryptedShell, unencryptedShell, collectionName) => {
    const encryptedCollection = encryptedShell.getDB("test").getCollection(collectionName);
    const unencryptedCollection = unencryptedShell.getDB("test").getCollection(collectionName);
    // Performing CRUD on a collection encrypted with randomized algorithm.
    assert.writeOK(encryptedCollection.insert({name: "Shreyas", "ssn": NumberInt(123456789)}));
    assert.eq(0, unencryptedCollection.count({
        "ssn":
            encryptedShell.getClientEncryption().encrypt(keyId, NumberInt(123456789), kRandomAlgo)
    }));

    const ssn_bin = unencryptedCollection.find({name: "Shreyas"})[0].ssn;
    assert.eq(NumberInt(123456789), encryptedShell.getClientEncryption().decrypt(ssn_bin));
};

let testDeterministicCollection = (keyId, encryptedShell, unencryptedShell, collectionName) => {
    const encryptedCollection = encryptedShell.getDB("test").getCollection(collectionName);
    const unencryptedCollection = unencryptedShell.getDB("test").getCollection(collectionName);
    // Testing insert
    assert.writeOK(encryptedCollection.insert({name: "Shreyas", "ssn": NumberInt(123456789)}));
    assert.writeOK(encryptedCollection.insert({name: "Mark", "ssn": NumberInt(987654321)}));
    assert.writeOK(encryptedCollection.insert({name: "Spencer", "ssn": NumberInt(987654321)}));
    assert.writeOK(encryptedCollection.insert({"name": "Sara", "ssn": NumberInt(200000000)}));
    assert.writeOK(encryptedCollection.insert({"name": "Sara", "ssn": NumberInt(300000000)}));
    assert.writeOK(encryptedCollection.insert({"name": "Jonathan", "ssn": NumberInt(300000000)}));

    // Testing count
    assert.eq(6, encryptedCollection.count());
    assert.eq(2, encryptedCollection.count({"name": "Sara"}));
    assert.eq(0, encryptedCollection.explain().count({"name": "Sara"}).executionStats.nReturned);
    assert.eq(2, encryptedCollection.count({"ssn": NumberInt(300000000)}));
    assert.eq(1, encryptedCollection.count({"ssn": NumberInt(123456789)}));

    // Testing update
    assert.eq(
        1,
        encryptedCollection.explain()
            .update({"ssn": NumberInt(987654321)}, {name: "Spencer", "ssn": NumberInt(123456789)})
            .executionStats.executionStages.nWouldModify);
    assert.writeOK(encryptedCollection.update({"ssn": NumberInt(987654321)},
                                              {name: "Spencer", "ssn": NumberInt(123456789)}));
    assert.eq(2, encryptedCollection.count({"ssn": NumberInt(123456789)}));

    // Testing delete
    assert.eq("1",
              encryptedCollection.explain().remove({"ssn": NumberInt(300000000)}).explainVersion);
    encryptedCollection.deleteMany({"ssn": NumberInt(300000000)});
    assert.eq(0, encryptedCollection.count({"ssn": NumberInt(300000000)}));
    assert.eq(4, encryptedCollection.count());

    // Testing findAndModify
    assert.eq(1,
              encryptedCollection.explain()
                  .findAndModify({
                      query: {name: "Shreyas"},
                      update: {"name": "Shreyas", "ssn": NumberInt(987654321)}
                  })
                  .executionStats.executionStages.nWouldModify);
    let prevData = encryptedCollection.findAndModify(
        {query: {name: "Shreyas"}, update: {"name": "Shreyas", "ssn": NumberInt(987654321)}});
    assert.eq(prevData.ssn, NumberInt(123456789));
    assert.eq(2, encryptedCollection.count({"ssn": NumberInt(987654321)}));
    prevData = encryptedCollection.findAndModify({
        query: {ssn: NumberInt(123456789)},
        update: {"name": "Spencer", "ssn": NumberInt(987654321)}
    });
    assert.eq(prevData.ssn, NumberInt(123456789));
    assert.eq(3, encryptedCollection.count({"ssn": NumberInt(987654321)}));

    // Testing that deterministic encryption works
    const encryptedDeterministicSSN = encryptedShell.getClientEncryption().encrypt(
        keyId, NumberInt(987654321), kDeterministicAlgo);
    assert.eq(3, unencryptedCollection.count({"ssn": encryptedDeterministicSSN}));

    unencryptedCollection.deleteMany({"ssn": encryptedDeterministicSSN});
    assert.eq(0, encryptedCollection.count({"ssn": NumberInt(987654321)}));

    unencryptedCollection.insert({"name": "Shreyas", "ssn": encryptedDeterministicSSN});
    assert.eq(1, encryptedCollection.count({"ssn": NumberInt(987654321)}));

    // Test GetMore works
    for (let i = 0; i < 128; i++) {
        unencryptedCollection.insertOne({
            name: 'Davis' + i,
            'ssn': encryptedShell.getClientEncryption().encrypt(
                keyId, NumberInt(i), kDeterministicAlgo)
        });
    }

    let results = encryptedCollection.aggregate([]).toArray();
    for (let i = 0; i < results.length; i++) {
        assert.eq(false, results[i].ssn instanceof BinData, results[i]);
    }

    // Test distinct
    assert.sameMembers(encryptedCollection.distinct("name", {"ssn": NumberInt(987654321)}),
                       ["Shreyas"]);
    assert.eq(1,
              encryptedCollection.explain()
                  .distinct("name", {"ssn": NumberInt(987654321)})
                  .executionStats.nReturned);
    assert.sameMembers(unencryptedCollection.distinct("name", {"ssn": NumberInt(987654321)}), []);

    // Test explain
    const encryptedExplainObj = encryptedCollection.find({"ssn": NumberInt(987654321)}).explain();
    assert.eq(encryptedExplainObj.executionStats.nReturned, 1);
    const unencryptedExplainObj =
        unencryptedCollection.find({"ssn": NumberInt(987654321)}).explain();
    assert.eq(unencryptedExplainObj.executionStats, undefined);

    // Test find
    assert.eq(1, encryptedCollection.find({"ssn": NumberInt(987654321)}).itcount());

    // Test aggregation
    assert.eq(1, encryptedCollection.aggregate({$match: {ssn: NumberInt(987654321)}}).itcount());
};

encryptedDatabase.createCollection("students", {
    validator: {
        $jsonSchema: {
            bsonType: "object",
            properties: {
                name: {bsonType: "string", description: "must be a string"},
                ssn: {encrypt: {bsonType: "int", algorithm: kRandomAlgo, keyId: [studentsKeyId]}}
            }
        }
    }
});

testRandomizedCollection(studentsKeyId, encryptedShell, conn, "students");

encryptedDatabase.createCollection("teachers", {
    validator: {
        $jsonSchema: {
            bsonType: "object",
            properties: {
                name: {
                    bsonType: "string",
                    description: "must be a string and is required",
                },
                ssn: {
                    encrypt: {
                        bsonType: "int",
                        algorithm: kDeterministicAlgo,
                        keyId: [teachersKeyId],
                    }
                }
            }
        }
    }
});

testDeterministicCollection(teachersKeyId, encryptedShell, conn, "teachers");

keyVault.createKey("local", ['staffKey']);
keyVault.createKey("local", ['adminKey']);
keyVault.createKey("local", ['Shreyas']);
const staffKeyId = keyVault.getKeyByAltName("staffKey").toArray()[0]._id;
const adminKeyId = keyVault.getKeyByAltName("adminKey").toArray()[0]._id;
const bureaucracyKeyId = keyVault.getKeyByAltName("Shreyas").toArray()[0]._id;

const staffSchema = {
    bsonType: "object",
    properties: {
        name: {
            bsonType: "string",
            description: "must be a string and is required",
        },
        ssn: {
            encrypt: {
                bsonType: "int",
                algorithm: kRandomAlgo,
                keyId: [staffKeyId],
            }
        }
    }
};

const bureaucracySchema = {
    bsonType: "object",
    properties: {
        name: {
            bsonType: "string",
            description: "must be a string and is required",
        },
        ssn: {
            encrypt: {
                bsonType: "int",
                algorithm: kRandomAlgo,
                keyId: "/name",
            }
        }
    }
};

const adminSchema = {
    bsonType: "object",
    properties: {
        name: {
            bsonType: "string",
            description: "must be a string and is required",
        },
        ssn: {
            encrypt: {
                bsonType: "int",
                algorithm: kDeterministicAlgo,
                keyId: [adminKeyId],
            }
        }
    }
};

const clientSideLocalSchemaFLEOptions = {
    kmsProviders: {
        local: localKMS,
    },
    keyVaultNamespace: "test.keystore",
    schemaMap: {
        "test.staff": staffSchema,
        "test.admin": adminSchema,
        "test.bureaucracy": bureaucracySchema,
    }
};

encryptedShell = Mongo(conn.host, clientSideLocalSchemaFLEOptions);
keyVault = encryptedShell.getKeyVault();
encryptedDatabase = encryptedShell.getDB("test");

testRandomizedCollection(staffKeyId, encryptedShell, conn, "staff");
testDeterministicCollection(adminKeyId, encryptedShell, conn, "admin");
testRandomizedCollection(bureaucracyKeyId, encryptedShell, conn, "bureaucracy");

MongoRunner.stopMongod(conn);
}());
