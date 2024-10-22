// This test ensures that the KMIP Activate functionality works
// correctly with a ESE key rollover. However if the key being rolled
// over to is deactivated, the Server should fail to start.
// @tags: [uses_pykmip, requires_gcm, incompatible_with_s390x]

import {
    activatePyKMIPKey,
    createPyKMIPKey,
    deactivatePyKMIPKey,
    killPyKMIPServer,
    startPyKMIPServer,
} from "src/mongo/db/modules/enterprise/jstests/encryptdb/libs/helpers.js";

const testDir = "src/mongo/db/modules/enterprise/jstests/encryptdb/";
const kmipServerPort = 6571;

if (_isWindows()) {
    quit();
}

// Set up test by creating two keys
// Create PyKMIPKey activates them as well
// de-activate the second key
const kmipServerPid = startPyKMIPServer(kmipServerPort);
const goodKey = createPyKMIPKey(kmipServerPort);
activatePyKMIPKey(kmipServerPort, goodKey);
const newGoodKey = createPyKMIPKey(kmipServerPort);
activatePyKMIPKey(kmipServerPort, newGoodKey);
const badKey = createPyKMIPKey(kmipServerPort);
activatePyKMIPKey(kmipServerPort, badKey);
deactivatePyKMIPKey(kmipServerPort, badKey);

const opts = {
    enableEncryption: "",
    kmipServerName: "127.0.0.1",
    kmipPort: kmipServerPort,
    kmipServerCAFile: "jstests/libs/trusted-ca.pem",
    encryptionCipherMode: "AES256-GCM",
    // We need the trusted-client certificate file in order to avoid permission issues when
    // getting the state attribute from the newly created key.
    kmipKeyIdentifier: goodKey,
    kmipClientCertificateFile: "jstests/libs/trusted-client.pem",
};

let mongod;

jsTest.log("Start up the server normally and add some values to databases.");
{
    mongod = MongoRunner.runMongod(opts);
    const testdb = mongod.getDB("test");

    assert.commandWorked(testdb.test.insert({foo: "bar"}));
    MongoRunner.stopMongod(mongod);
}

jsTest.log("Test that rollover works with active key.");
{
    const newOpts = Object.merge(opts, {eseDatabaseKeyRollover: "", restart: mongod});
    mongod = MongoRunner.runMongod(newOpts);
    const testdb = mongod.getDB("test");

    assert.commandWorked(testdb.test.insert({foo2: "bar2"}));
    const findResults = testdb.test.find({}, {_id: 0}).toArray();

    print(tojson(findResults));
    assert.eq(findResults.length, 2);
    MongoRunner.stopMongod(mongod);
}

jsTest.log(
    "Test that when the key being rotated from is de-activated we can still rotate to the new key.");
{
    deactivatePyKMIPKey(kmipServerPort, goodKey);

    const newOpts = Object.merge(
        opts, {kmipKeyIdentifier: newGoodKey, kmipRotateMasterKey: "", restart: mongod});
    clearRawMongoProgramOutput();
    mongod = MongoRunner.runMongod(newOpts);
    assert(rawMongoProgramOutput(".*").search("Rotated master encryption key") !== -1);
}

jsTest.log("Test that ESE key rotate does not work with de-activated key.");
{
    mongod = MongoRunner.runMongod(Object.merge(opts, {kmipKeyIdentifier: newGoodKey}));
    MongoRunner.stopMongod(mongod);

    const newOpts =
        Object.merge(opts, {kmipKeyIdentifier: badKey, kmipRotateMasterKey: "", restart: mongod});
    mongod = MongoRunner.runMongod(newOpts);

    assert(!mongod);
    assert(rawMongoProgramOutput(".*").search(
               "State of KMIP Key for ESE is not active on startup") !== -1);
}

killPyKMIPServer(kmipServerPid);
