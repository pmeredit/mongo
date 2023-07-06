/**
 * Tests that the encrypted audit log contains a valid header at the top
 * @tags: [uses_pykmip, requires_gcm, incompatible_with_s390x]
 */

import {determineSSLProvider} from "jstests/ssl/libs/ssl_helpers.js";
import {
    ShardingFixture,
    StandaloneFixture
} from "src/mongo/db/modules/enterprise/jstests/audit/lib/audit.js";
import {
    AUDIT_LOCAL_KEY_ENCRYPT_KEYFILE,
    isValidEncryptedAuditLogHeader,
    KMIPEncryptFixture,
    KMIPGetFixture,
    LocalFixture,
} from "src/mongo/db/modules/enterprise/jstests/audit/lib/audit_encryption.js";

// note: this port number must be unique among the audit tests that use
// the PyKMIP server, so that these tests can run in parallel without
// EADDRINUSE issues.
const kmipServerPort = 6567;

if (determineSSLProvider() !== "windows") {
    run("chmod", "600", AUDIT_LOCAL_KEY_ENCRYPT_KEYFILE);
}

print("Testing audit log contains header if encryption is enabled");
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
    new KMIPGetFixture(kmipServerPort, false /* useLegacyProtocol */),
    new KMIPGetFixture(kmipServerPort, true /* useLegacyProtocol */),
    new KMIPEncryptFixture(kmipServerPort, false /* useLegacyProtocol */)
];

for (const keyManagerFixture of keyManagerFixtures) {
    jsTest.log("Testing with key store type " + keyManagerFixture.getName());

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
