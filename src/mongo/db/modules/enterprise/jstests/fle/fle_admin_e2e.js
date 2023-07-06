/**
 * End-to-end tests for the happy path for admin commands create and createIndexes with
 * FLE1.
 *
 * These end-to-end tests mostly exist to test the happy path and make sure that query analysis is
 * outputting commands that are still valid for mongod to process. Testing edge cases and failure
 * modes for query analysis is covered in fle_collection_validator.js and fle_createindexes.js which
 * explicitly communicate with query analysis in mongocryptd.
 *
 * @tags: [unsupported_fle_2]
 */
import {CA_CERT, SERVER_CERT} from "jstests/ssl/libs/ssl_helpers.js";
import {kDeterministicAlgo} from "src/mongo/db/modules/enterprise/jstests/fle/lib/utils.js";

// Set up key management and encrypted shell.
const x509_options = {
    sslMode: "requireSSL",
    sslPEMKeyFile: SERVER_CERT,
    sslCAFile: CA_CERT,
    vvvvv: ""
};

const conn = MongoRunner.runMongod(x509_options);

let localKMS = {
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

keyVault.createKey("local", ['key2']);

Random.setRandomSeed();

const defaultKeyId = keyVault.getKeyByAltName("key2").toArray()[0]._id;
const collName = jsTestName();
const schema = {
    encryptMetadata: {
        algorithm: kDeterministicAlgo,
        keyId: [defaultKeyId],
    },
    type: "object",
    properties: {ssn: {encrypt: {bsonType: "string"}}}
};

const encryptedDatabase = encryptedShell.getDB("crypt");

// In FLE 1, encrypted collections are defined by their jsonSchema validator.
assert.commandWorked(encryptedDatabase.runCommand({
    create: collName,
    validator: {$jsonSchema: schema},
}));

assert.commandWorked(encryptedDatabase.runCommand({
    createIndexes: collName,
    indexes: [{
        key: {name: 1},
        name: "name",
        partialFilterExpression: {indexed: true},
    }],
}));

try {
    encryptedDatabase.runCommand({
        createIndexes: collName,
        indexes: [{
            key: {name: 1, lastname: 1},
            name: "secondName",
            partialFilterExpression: {ssn: "abc"},
        }],
    });
    assert(false, "command succeeded when it should have failed.");
} catch (e) {
    assert(e.message.indexOf("Client Side Field Level Encryption Error") !== -1);
}
// Replacing the validator with a non-JSON Schema validator that doesn't reference an encrypted
// field should work.
assert.commandWorked(encryptedDatabase.runCommand({
    collMod: collName,
    validator: {unencrypted: "secretword"},
}));

MongoRunner.stopMongod(conn);
