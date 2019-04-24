/**
* Check the functionality of query functions with encryption.
*/

load("src/mongo/db/modules/enterprise/jstests/fle/lib/mock_kms.js");
load('jstests/ssl/libs/ssl_helpers.js');

(function() {
    "use strict";

    const mock_kms = new MockKMSServer();
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

    const clientSideFLEOptions = {
        kmsProviders: {
            aws: awsKMS,
            local: localKMS,
        },
        keyVaultCollection: collection,
        useRemoteSchemas: true,
    };

    const encryptedShell = Mongo(conn.host, clientSideFLEOptions);
    const keyStore = encryptedShell.getKeyStore();

    assert.writeOK(keyStore.createKey(
        "aws", "arn:aws:mongo1:us-east-1:123456789:environment", ['studentsKey']));
    assert.writeOK(keyStore.createKey(
        "local", "arn:aws:mongo2:us-east-1:123456789:environment", ['teachersKey']));
    const studentsKeyId = keyStore.getKeyByAltName("studentsKey").toArray()[0]._id;
    const teachersKeyId = keyStore.getKeyByAltName("teachersKey").toArray()[0]._id;

    const database = encryptedShell.getDB("test");

    database.createCollection("students", {
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
                            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Random",
                            keyId: [studentsKeyId],
                        }
                    }
                }
            }
        }
    });

    const teacherBinData = BinData(0, "YXNkZmFzZGZhc3RmYXNkZg==");

    database.createCollection("teachers", {
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
                            algorithm: "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
                            keyId: [teachersKeyId],
                        }
                    }
                }
            }
        }
    });

    print(database.getCollectionInfos());

    assert.writeOK(database.students.insert({name: "Shreyas", "ssn": NumberInt(123456789)}));
    assert.writeOK(database.teachers.insert({name: "Shreyas", "ssn": NumberInt(123456789)}));

    const ssn_bin = unencryptedDatabase.students.find({name: "Shreyas"})[0].ssn;
    assert.eq(NumberInt(123456789), encryptedShell.decrypt(ssn_bin));

    assert.writeOK(database.teachers.insert({name: "Mark", "ssn": NumberInt(987654321)}));
    assert.writeOK(database.teachers.insert({name: "Spencer", "ssn": NumberInt(987654321)}));
    assert.writeOK(database.teachers.insert({"name": "Sara", "ssn": NumberInt(200000000)}));
    assert.writeOK(database.teachers.insert({"name": "Sara", "ssn": NumberInt(300000000)}));
    assert.writeOK(database.teachers.insert({"name": "Jonathan", "ssn": NumberInt(300000000)}));

    assert.eq(6, database.teachers.count());
    assert.eq(2, database.teachers.count({"name": "Sara"}));

    // assert.eq(2, database.teachers.count({"ssn" : NumberInt(300000000)}));

    // assert.eq(1, database.teachers.count({"ssn" : NumberInt(123456789)}));
    // assert.writeOK(database.teachers.update({"ssn": NumberInt(987654321)},
    //                                         { name: "Spencer", "ssn": NumberInt(123456789) }));
    // assert.eq(2, database.teachers.count({"ssn" : NumberInt(123456789)}));

    // database.teachers.deleteMany({"ssn": NumberInt(300000000)});
    // assert.eq(0, database.teachers.count({"ssn" : NumberInt(300000000)}));
    // assert.eq(4, database.teachers.count());

    // let prevData = database.teachers.findAndModify({ query : { name: "Shreyas" }, update: {
    // "name" : "Shreyas", "ssn" : NumberInt(987654321) } });
    // assert.eq(prevData.ssn, NumberInt(123456789));
    // assert.eq(2, database.teachers.count({"ssn" : NumberInt(987654321)}));
    // prevData = database.teachers.findAndModify({ query : { ssn: NumberInt(123456789) }, update: {
    // "name" : "Spencer", "ssn" : NumberInt(987654321) } });
    // assert.eq(prevData.ssn, NumberInt(123456789));
    // assert.eq(3, database.teachers.count({"ssn" : NumberInt(987654321)}));

    // Will add tests for aggregate once query implements/fixes those.
    // TODO : File ticket if this goes in before query work is finished.

    MongoRunner.stopMongod(conn);
    mock_kms.stop();
}());