// Tests that rotating KMIP master key after rolling over the database key works.
// @tags: [uses_pykmip, incompatible_with_s390x]
(function() {
'use strict';

load("src/mongo/db/modules/enterprise/jstests/encryptdb/libs/helpers.js");

if (!platformSupportsGCM) {
    jsTest.log("Skipping test: Platform does not support GCM");
    return;
}

const kmipServerPort = 63070;
const kmipServerPid = startPyKMIPServer(kmipServerPort);

const defaultOpts = {
    enableEncryption: "",
    kmipServerName: "127.0.0.1",
    kmipPort: kmipServerPort,
    kmipServerCAFile: "jstests/libs/trusted-ca.pem",
    kmipKeyStatePollingSeconds: 5,
    encryptionCipherMode: "AES256-GCM",
    kmipClientCertificateFile: "jstests/libs/trusted-client.pem",
    // Make sure we aren't compressing so that our inserts correctly exceed 16kB (more information
    // below).
    wiredTigerCollectionBlockCompressor: "none",
    wiredTigerJournalCompressor: "none"
};

// Assert that the given keyId matches the global KMIP key ID of the given stopped mongod instance.
function assertKeyId(md, keyId) {
    // Start a new mongod pointing at the dbpath of md, setting the key ID. This will fail to run if
    // the key ID is wrong.
    const keyIdTest = MongoRunner.runMongod(Object.assign(
        {dbpath: md.dbpath, noCleanData: true, kmipKeyIdentifier: keyId}, defaultOpts));
    assert.neq(null, keyIdTest, "Wasn't able to start mongod with the keyID of " + keyId);
    MongoRunner.stopMongod(keyIdTest);
}
// WiredTiger pages are 16kB, and we want to ensure that our inserts have their own pages.
// Otherwise, a new insert could cause a new page to fully replace the old page that the previous
// insert was on, which could lead to the rollover ID associated with old pages to be cleaned up. By
// ensuring that our inserts are bigger than 32kB = 2^15, we assure that each insert is the sole
// occupier of at least one page.
const fooData = 'a'.repeat(Math.pow(2, 15) + 1);
const barData = 'b'.repeat(Math.pow(2, 15) + 1);

// First invoke mongod normally, add some data, and shut down
let mongod = MongoRunner.runMongod(defaultOpts);

assert(mongod, "Could not start mongod with GCM encryption enabled");

let testdb = mongod.getDB("test");
assert.commandWorked(testdb.test.insert({foo: fooData}));

MongoRunner.stopMongod(mongod);
assertKeyId(mongod, 1);

// Now restart with eseDatabaseKeyRollover to rollover the database key, and add some more data
mongod = MongoRunner.runMongod(
    Object.assign({restart: mongod, remember: true, eseDatabaseKeyRollover: ""}, defaultOpts));

testdb = mongod.getDB("test");
assert.commandWorked(testdb.test.insert({bar: barData}));

MongoRunner.stopMongod(mongod);
assertKeyId(mongod, 1);

// Finally, rotate the master key and ensure that it runs successfully. Note that this mongod
// invocation will exit by itself after rotating.
assert.eq(null,
          MongoRunner.runMongod(Object.assign(
              {restart: mongod, remember: true, kmipRotateMasterKey: ""}, defaultOpts)));

// Ensure the master key was rotated by checking that the key ID has been bumped.
assertKeyId(mongod, 2);

// Ensure that after restarting normally, we can still read the data.
mongod = MongoRunner.runMongod(Object.assign({restart: mongod, remember: true}, defaultOpts));

testdb = mongod.getDB("test");
assert.eq(1, testdb.test.find({foo: fooData}).toArray().length);
assert.eq(1, testdb.test.find({bar: barData}).toArray().length);

MongoRunner.stopMongod(mongod);
assertKeyId(mongod, 2);

// Try rolling over the database key again and ensure we can still read the data.
mongod = MongoRunner.runMongod(
    Object.assign({restart: mongod, remember: true, eseDatabaseKeyRollover: ""}, defaultOpts));

testdb = mongod.getDB("test");
assert.eq(1, testdb.test.find({foo: fooData}).toArray().length);
assert.eq(1, testdb.test.find({bar: barData}).toArray().length);

MongoRunner.stopMongod(mongod);
assertKeyId(mongod, 2);

killPyKMIPServer(kmipServerPid);
})();
