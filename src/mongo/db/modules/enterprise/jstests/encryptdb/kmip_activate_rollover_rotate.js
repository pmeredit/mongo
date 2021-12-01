// This test ensures that the KMIP Activate functionality works
// correctly with a ESE key rollover. However if the key being rolled
// over to is deactivated, the Server should fail to start.

(function() {
"use strict";

const testDir = "src/mongo/db/modules/enterprise/jstests/encryptdb/";
const kmipServerPort = 6571;

load("jstests/libs/log.js");
load(testDir + "libs/helpers.js");

if (!TestData.setParameters.featureFlagKmipActivate || _isWindows()) {
    // Don't accept option when FF not enabled.
    return;
}

// Set up test by creating two keys
// Create PyKMIPKey activates them as well
// de-activate the second key
const kmipServerPid = startPyKMIPServer(kmipServerPort);
const goodKey = createPyKMIPKey(kmipServerPort);
const badKey = createPyKMIPKey(kmipServerPort);
deactivatePyKMIPKey(kmipServerPort, badKey);

jsTest.log("Start up the server normally and add some values to databases.");
let opts = {
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

let mongod = MongoRunner.runMongod(opts);

let testdb = mongod.getDB("test");
assert.commandWorked(testdb.test.insert({foo: "bar"}));

MongoRunner.stopMongod(mongod);

jsTest.log("Test that rollover works with active key.");
let newOpts = Object.merge(opts, {eseDatabaseKeyRollover: "", restart: mongod});
mongod = MongoRunner.runMongod(newOpts);

testdb = mongod.getDB("test");
assert.commandWorked(testdb.test.insert({foo2: "bar2"}));
const findResults = testdb.test.find({}, {_id: 0}).toArray();
print(tojson(findResults));
assert.eq(findResults.length, 2);

MongoRunner.stopMongod(mongod);

jsTest.log("Test that ESE key rotate does not work with de-activated key.");
clearRawMongoProgramOutput();

let badOpts =
    Object.merge(opts, {kmipKeyIdentifier: badKey, kmipRotateMasterKey: "", restart: mongod});
mongod = MongoRunner.runMongod(badOpts);
assert(!mongod);
assert(rawMongoProgramOutput().search("State of KMIP Key for ESE is not active on startup") !== -1);
killPyKMIPServer(kmipServerPid);
}());