/**
 * These tests are the embedded tests from
 * https://github.com/mongodb/specifications/tree/master/source/client-side-encryption/tests
 */

import {CA_CERT, SERVER_CERT} from "jstests/ssl/libs/ssl_helpers.js";
import {
    binDataDecryption,
    findDataLocal,
    jsonSchema,
    keyvaultDataDecryption,
    keyvaultDataLocal,
    providerObj,
} from "src/mongo/db/modules/enterprise/jstests/fle/lib/drivers_data.js";

const x509_options = {
    sslMode: "requireSSL",
    sslPEMKeyFile: SERVER_CERT,
    sslCAFile: CA_CERT,
    setParameter: {
        tlsOCSPVerifyTimeoutSecs: 5,
        tlsOCSPStaplingTimeoutSecs: 10,
    }
};

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
