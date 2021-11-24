// This test tests that the KMIP 'State' attribute is 'active' upon creation.
// It assumes that PyKMIP is installed
// @tags: [uses_pykmip]

(function() {
"use strict";

const testDir = "src/mongo/db/modules/enterprise/jstests/encryptdb/";
const kmipServerPort = 6566;

load("jstests/libs/log.js");
load(testDir + "libs/helpers.js");

if (!TestData.setParameters.featureFlagKmipActivate) {
    // Don't accept option when FF not enabled.
    return;
}

// Run mongod and create a new key by not passing in encryptionKeyId.
function createKeyMongod(extraOpts) {
    clearRawMongoProgramOutput();
    let defaultOpts = {
        enableEncryption: "",
        kmipServerName: "127.0.0.1",
        kmipPort: kmipServerPort,
        kmipServerCAFile: "jstests/libs/trusted-ca.pem",
        encryptionCipherMode: "AES256-CBC",
        // We need the trusted-client certificate file in order to avoid permission issues when
        // getting the state attribute from the newly created key.
        kmipClientCertificateFile: "jstests/libs/trusted-client.pem",
    };
    let opts = Object.merge(defaultOpts, extraOpts);

    const md = MongoRunner.runMongod(opts);
    const adminDB = md.getDB('admin');

    // Get the keyId of the newly created key from the logs.
    // The log reads: {..."msg":"Created KMIP key","attr":{"keyId":"1"}}
    let log = assert.commandWorked(adminDB.adminCommand({getLog: "global"})).log;
    let line = findMatchingLogLine(log, {id: 24199, msg: "Created KMIP key"});
    assert(line, "Failed to find a log line matching the message");

    let keyId = JSON.parse(line).attr.keyId;
    jsTest.log('Key ID found: ' + keyId);

    MongoRunner.stopMongod(md);
    return keyId;
}

// Run mongod, create a new key (by not passing in encryptionKeyId) and ensure it is active.
function mongodKeyActivationTest() {
    let kmipServerPid = startPyKMIPServer(kmipServerPort);
    let keyId = createKeyMongod({});
    let isActive = isPyKMIPKeyActive(kmipServerPort, keyId);

    assert(isActive);
    killPyKMIPServer(kmipServerPid);
}

// Run mongod, create a new key with kmipActivateKeys set to false and ensure it is not active.
function turnOffKmipActivateKeys() {
    let kmipServerPid = startPyKMIPServer(kmipServerPort);
    let keyId = createKeyMongod({kmipActivateKeys: false});
    let isActive = isPyKMIPKeyActive(kmipServerPort, keyId);

    assert(!isActive);
    killPyKMIPServer(kmipServerPid);
}

mongodKeyActivationTest();
turnOffKmipActivateKeys();
})();
