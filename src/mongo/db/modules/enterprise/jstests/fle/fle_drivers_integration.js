/**
 * These tests are the embedded tests from
 * https://github.com/mongodb/specifications/tree/master/source/client-side-encryption/tests
 */

load('jstests/ssl/libs/ssl_helpers.js');
load('src/mongo/db/modules/enterprise/jstests/fle/lib/drivers_data.js');

(function() {
    "use strict";

    const x509_options = {sslMode: "requireSSL", sslPEMKeyFile: SERVER_CERT, sslCAFile: CA_CERT};

    const conn = MongoRunner.runMongod(x509_options);

    var setUpEnvironment = function(keyvaultData, schema, collectionData) {
        const defaultDB = conn.getDB("default");

        const keyvaultCollection = defaultDB.getCollection("keyvault");
        keyvaultCollection.drop();
        keyvaultData.forEach(function(data) {
            assert.writeOK(keyvaultCollection.insert(data));
        });

        const dataCollection = defaultDB.getCollection("default");
        dataCollection.drop();
        defaultDB.createCollection("default", schema);

        collectionData.forEach(function(data) {
            assert.writeOK(dataCollection.insert(data));
        });
    };

    // Running the test for find
    let find = function(conn, providerObj, keyvaultData, jsonSchema, findData) {
        let clientSideFLEOptions = {
            kmsProviders: providerObj,
            keyVaultNamespace: "default.keyvault",
            schemaMap: {}
        };

        let unencryptedCollection = conn.getDB("default").getCollection("default");
        let encryptedShell = new Mongo(conn.host, clientSideFLEOptions);
        let encryptedCollection = encryptedShell.getDB("default").getCollection("default");

        setUpEnvironment(keyvaultData, jsonSchema, findData);

        assert.eq(2, encryptedCollection.count());

        assert.eq(1, encryptedCollection.count({"encrypted_string": "string0"}));
        assert.eq(1, encryptedCollection.count({"encrypted_string": "string1"}));
    };

    find(conn, providerObj, keyvaultDataAWS, jsonSchema, findDataAWS);

    // Running the test for insert
    let insert = function(conn, providerObj, keyvaultData, jsonSchema, findData) {
        let clientSideFLEOptions = {
            kmsProviders: providerObj,
            keyVaultNamespace: "default.keyvault",
            schemaMap: {}
        };

        let unencryptedCollection = conn.getDB("default").getCollection("default");
        let encryptedShell = new Mongo(conn.host, clientSideFLEOptions);
        let encryptedCollection = encryptedShell.getDB("default").getCollection("default");

        setUpEnvironment(keyvaultData, jsonSchema, findData);
        encryptedCollection.insertOne({
            "_id": 1,
            "encrypted_string": "string0",
        });

        encryptedCollection.insertOne({
            "_id": 2,
            "encrypted_string": "string1",
        });

        assert.eq(2, unencryptedCollection.count());
        // Searching for string0
        assert.eq(1, unencryptedCollection.count({
            "encrypted_string": BinData(
                6,
                "AQAAAAAAAAAAAAAAAAAAAAACwj+3zkv2VM+aTfk60RqhXq6a/77WlLwu/BxXFkL7EppGsju/m8f0x5kBDD3EZTtGALGXlym5jnpZAoSIkswHoA==")
        }));
        // Searching for string1
        assert.eq(1, unencryptedCollection.count({
            "encrypted_string": BinData(
                6,
                "AQAAAAAAAAAAAAAAAAAAAAACDdw4KFz3ZLquhsbt7RmDjD0N67n0uSXx7IGnQNCLeIKvot6s/ouI21Eo84IOtb6lhwUNPlSEBNY0/hbszWAKJg==")
        }));
    };

    insert(conn, providerObj, keyvaultDataAWS, jsonSchema, []);

    // Running the test for LocalKMS
    let local = function(conn, providerObj, keyvaultData, jsonSchema, findData) {
        let clientSideFLEOptions = {
            kmsProviders: providerObj,
            keyVaultNamespace: "default.keyvault",
            schemaMap: {}
        };

        let encryptedShell = new Mongo(conn.host, clientSideFLEOptions);
        let encryptedCollection = encryptedShell.getDB("default").getCollection("default");

        setUpEnvironment(keyvaultData, jsonSchema, findData);
        assert.eq(1, encryptedCollection.count({"encrypted_string": "string0"}));
    };

    local(conn, providerObj, keyvaultDataLocal, jsonSchema, findDataLocal);

    let decrypt = function(conn, providerObj, keyvaultData, binData) {
        let clientSideFLEOptions = {
            kmsProviders: providerObj,
            keyVaultNamespace: "default.keyvault",
            schemaMap: {}
        };

        let encryptedShell = new Mongo(conn.host, clientSideFLEOptions);
        setUpEnvironment(keyvaultData, {}, []);
        binData.forEach(function(data) {
            encryptedShell.decrypt(data);
        });
    };

    decrypt(conn, providerObj, keyvaultDataDecryption, binDataDecryption);
    MongoRunner.stopMongod(conn);
}());