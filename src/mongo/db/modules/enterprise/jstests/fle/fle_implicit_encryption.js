/**
* Check the functionality of query functions with encryption.
*/

load("src/mongo/db/modules/enterprise/jstests/fle/lib/mock_kms.js");
load('jstests/ssl/libs/ssl_helpers.js');

(function() {
    "use strict";

    const mock_kms = new MockKMSServer();
    const randomAlgorithm = "AEAD_AES_256_CBC_HMAC_SHA_512-Random";
    const deterministicAlgorithm = "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic";
    mock_kms.start();

    const x509_options =
        {sslMode: "requireSSL", sslPEMKeyFile: SERVER_CERT, sslCAFile: CA_CERT, vvvvv: ""};

    const conn = MongoRunner.runMongod(x509_options);
    const unencryptedDatabase = conn.getDB("test");
    const collection = unencryptedDatabase.keystore;

    const awsKMS = {
        accessKeyId: "access",
        secretAccessKey: "secret",
        url: mock_kms.getURL(),
    };

    const localKMS = {
        key: BinData(
            0,
            "/i8ytmWQuCe1zt3bIuVa4taPGKhqasVp0/0yI4Iy0ixQPNmeDF1J5qPUbBYoueVUJHMqj350eRTwztAWXuBdSQ=="),
    };

    const clientSideRemoteSchemaFLEOptions = {
        kmsProviders: {
            aws: awsKMS,
            local: localKMS,
        },
        keyVaultCollection: collection,
        useRemoteSchemas: true,
    };

    var encryptedShell = Mongo(conn.host, clientSideRemoteSchemaFLEOptions);
    var keyStore = encryptedShell.getKeyStore();

    assert.writeOK(keyStore.createKey(
        "aws", "arn:aws:mongo1:us-east-1:123456789:environment", ['studentsKey']));
    assert.writeOK(keyStore.createKey(
        "local", "arn:aws:mongo2:us-east-1:123456789:environment", ['teachersKey']));
    const studentsKeyId = keyStore.getKeyByAltName("studentsKey").toArray()[0]._id;
    const teachersKeyId = keyStore.getKeyByAltName("teachersKey").toArray()[0]._id;

    var encryptedDatabase = encryptedShell.getDB("test");

    let testRandomizedCollection = (keyId, encryptedShell, unencryptedShell, collectionName) => {
        const encryptedCollection = encryptedShell.getDB("test").getCollection(collectionName);
        const unencryptedCollection = unencryptedShell.getDB("test").getCollection(collectionName);
        // Performing CRUD on a collection encrypted with randomized algorithm.
        assert.writeOK(encryptedCollection.insert({name: "Shreyas", "ssn": NumberInt(123456789)}));
        assert.eq(0, unencryptedCollection.count({
            "ssn": encryptedShell.encrypt(keyId, NumberInt(123456789), randomAlgorithm)
        }));

        const ssn_bin = unencryptedCollection.find({name: "Shreyas"})[0].ssn;
        assert.eq(NumberInt(123456789), encryptedShell.decrypt(ssn_bin));
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
        assert.writeOK(
            encryptedCollection.insert({"name": "Jonathan", "ssn": NumberInt(300000000)}));

        // Testing count
        assert.eq(6, encryptedCollection.count());
        assert.eq(2, encryptedCollection.count({"name": "Sara"}));
        assert.eq(2, encryptedCollection.count({"ssn": NumberInt(300000000)}));
        assert.eq(1, encryptedCollection.count({"ssn": NumberInt(123456789)}));

        // Testing update
        assert.writeOK(encryptedCollection.update({"ssn": NumberInt(987654321)},
                                                  {name: "Spencer", "ssn": NumberInt(123456789)}));
        assert.eq(2, encryptedCollection.count({"ssn": NumberInt(123456789)}));

        // Testing delete
        encryptedCollection.deleteMany({"ssn": NumberInt(300000000)});
        assert.eq(0, encryptedCollection.count({"ssn": NumberInt(300000000)}));
        assert.eq(4, encryptedCollection.count());

        // Testing findAndModify
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
        const encryptedDeterministicSSN =
            encryptedShell.encrypt(keyId, NumberInt(987654321), deterministicAlgorithm);
        assert.eq(3, unencryptedCollection.count({"ssn": encryptedDeterministicSSN}));

        unencryptedCollection.deleteMany({"ssn": encryptedDeterministicSSN});
        assert.eq(0, encryptedCollection.count({"ssn": NumberInt(987654321)}));

        unencryptedCollection.insert({"name": "Shreyas", "ssn": encryptedDeterministicSSN});
        assert.eq(1, encryptedCollection.count({"ssn": NumberInt(987654321)}));

        // Will add tests for aggregate once query implements it.
        // TODO : File ticket if this goes in before query work is finished.
    };

    encryptedDatabase.createCollection("students", {
        validator: {
            $jsonSchema: {
                bsonType: "object",
                properties: {
                    name: {bsonType: "string", description: "must be a string"},
                    ssn: {
                        encrypt: {
                            bsonType: "int",
                            algorithm: randomAlgorithm,
                            keyId: [studentsKeyId]
                        }
                    }
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
                            algorithm: deterministicAlgorithm,
                            keyId: [teachersKeyId],
                        }
                    }
                }
            }
        }
    });

    testDeterministicCollection(teachersKeyId, encryptedShell, conn, "teachers");

    assert.writeOK(keyStore.createKey(
        "local", "arn:aws:mongo2:us-east-1:123456789:environment", ['staffKey']));
    assert.writeOK(
        keyStore.createKey("aws", "arn:aws:mongo1:us-east-1:123456789:environment", ['adminKey']));
    const staffKeyId = keyStore.getKeyByAltName("staffKey").toArray()[0]._id;
    const adminKeyId = keyStore.getKeyByAltName("adminKey").toArray()[0]._id;

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
                    algorithm: randomAlgorithm,
                    keyId: [staffKeyId],
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
                    algorithm: deterministicAlgorithm,
                    keyId: [adminKeyId],
                }
            }
        }
    };

    const clientSideLocalSchemaFLEOptions = {
        kmsProviders: {
            aws: awsKMS,
            local: localKMS,
        },
        keyVaultCollection: collection,
        schemas: {
            "test.staff": staffSchema,
            "test.admin": adminSchema,
        }
    };

    encryptedShell = Mongo(conn.host, clientSideLocalSchemaFLEOptions);
    keyStore = encryptedShell.getKeyStore();
    encryptedDatabase = encryptedShell.getDB("test");

    testRandomizedCollection(staffKeyId, encryptedShell, conn, "staff");
    testDeterministicCollection(adminKeyId, encryptedShell, conn, "admin");

    MongoRunner.stopMongod(conn);
    mock_kms.stop();
}());