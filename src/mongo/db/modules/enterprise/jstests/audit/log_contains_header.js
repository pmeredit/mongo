/**
 * Tests that the audit log contains a valid header at the top
 * when compression is enabled
 * @tags: [uses_pykmip]
 */

load('src/mongo/db/modules/enterprise/jstests/audit/lib/audit_encryption.js');
load('jstests/ssl/libs/ssl_helpers.js');

(function() {

"use strict";

// note: this port number must be unique among the audit tests that use
// the PyKMIP server, so that these tests can run in parallel without
// EADDRINUSE issues.
const kmipServerPort = 6567;

if (determineSSLProvider() !== "windows") {
    run("chmod", "600", AUDIT_LOCAL_KEY_ENCRYPT_KEYFILE);
}

print("Testing audit log contains header when compression is enabled");
function testAuditLogHeader(serverFixture, isMongos, enableCompression, keyManagerFixture) {
    keyManagerFixture.startKeyServer();
    let opts = keyManagerFixture.generateOptsWithDefaults(enableCompression);

    if (isMongos) {
        opts = {other: {mongosOptions: opts}};
    }

    jsTest.log("Testing: " + tojson(opts));
    const {conn, audit, admin} = serverFixture.startProcess(opts);

    assert.soon(() => {
        audit.resetAuditLine();
        const fileHeader = audit.getNextEntryNoParsing();
        return isValidEncryptedAuditLogHeader(fileHeader, keyManagerFixture.getKeyStoreType());
    }, "Audit log did not contain a valid header line on the top");

    serverFixture.stopProcess();

    keyManagerFixture.stopKeyServer();

    // Add a one-second delay to ensure that the next 'mongod' or
    // 'mongos' run will not rotate to the same filename as the
    // file created during this run
    sleep(1000);
}

let keyManagerFixtures = [
    new LocalFixture(),
    new KMIPGetFixture(kmipServerPort),
    new KMIPEncryptFixture(kmipServerPort)
];

for (const keyManagerFixture of keyManagerFixtures) {
    jsTest.log("Testing with key store type " + keyManagerFixture.getKeyStoreType());

    {
        const standaloneFixture = new StandaloneFixture();

        jsTest.log("Testing audit file from standalone contains a valid header log");
        testAuditLogHeader(standaloneFixture, false, true, keyManagerFixture);
        testAuditLogHeader(standaloneFixture, false, false, keyManagerFixture);
    }

    {
        const shardingFixture = new ShardingFixture();

        jsTest.log("Testing audit file from sharded cluster contains a valid header log");
        testAuditLogHeader(shardingFixture, true, true, keyManagerFixture);
        testAuditLogHeader(shardingFixture, true, false, keyManagerFixture);
    }
}
})();
