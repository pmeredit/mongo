/**
 * Tests that modifications to the encrypted audit log file
 * can be detected on decrypt.
 * @tags: [uses_pykmip, requires_gcm]
 */

load('src/mongo/db/modules/enterprise/jstests/audit/lib/audit_encryption.js');
load('jstests/ssl/libs/ssl_helpers.js');

(function() {

"use strict";

// note: this port number must be unique among the audit tests that use
// the PyKMIP server, so that these tests can run in parallel without
// EADDRINUSE issues.
const kmipServerPort = 6568;

if (determineSSLProvider() !== "windows") {
    run("chmod", "600", AUDIT_LOCAL_KEY_ENCRYPT_KEYFILE);
}

print("Testing audit log integrity checks are performed by the decryptor");
function testAuditLogIntegrity(serverFixture, isMongos, keyManagerFixture) {
    const enableCompression = true;
    keyManagerFixture.startKeyServer();
    let opts = keyManagerFixture.generateOptsWithDefaults(enableCompression);

    if (isMongos) {
        opts = {other: {mongosOptions: opts}};
    }

    jsTest.log("Testing: " + tojson(opts));
    const {conn, audit, admin} = serverFixture.startProcess(opts);

    // need at least 4 lines to perform this test
    assert.soon(() => {
        audit.resetAuditLine();
        audit.getNextEntryNoParsing();
        audit.fastForward();
        return audit.getCurrentAuditLine() >= 4;
    }, "Audit log must have at least 4 entries including header");

    serverFixture.stopProcess();

    // TEST 1: normal output decrypts successfully
    jsTest.log("Testing decrypt of normal audit log succeeds");

    let auditPath = conn.fullOptions.auditPath;
    const runId = (isMongos ? "sharded" : "standalone") + "_" + keyManagerFixture.getKeyStoreType();
    const outputFile = MongoRunner.dataPath + "decompressedLog_" + runId + ".json";
    let decryptPid = keyManagerFixture.runDecryptor(auditPath, outputFile);
    assert.eq(waitProgram(decryptPid), 0);

    // TEST 2: modify audit file by swapping log lines, verify decrypt fails
    jsTest.log("Testing decrypt fails if log lines are swapped");

    audit.resetAuditLine();
    let lines = audit.getAllLines();
    let tmpline = lines[2];
    lines[2] = lines[1];
    lines[1] = tmpline;

    auditPath = MongoRunner.dataPath + "auditLog_" + runId + ".swapped";
    writeFile(auditPath, lines.join("\n"));

    decryptPid = keyManagerFixture.runDecryptor(auditPath, outputFile + ".swapped");
    assert.neq(waitProgram(decryptPid), 0);

    // TEST 3: modify audit file by removing lines, verify decrypt fails
    jsTest.log("Testing decrypt fails if log lines are removed");
    audit.resetAuditLine();
    lines = audit.getAllLines();

    // erase lines between the first 2 lines and the last line
    lines.splice(2, lines.length - 3);

    auditPath = MongoRunner.dataPath + "auditLog_" + runId + ".removed";
    writeFile(auditPath, lines.join("\n"));

    decryptPid = keyManagerFixture.runDecryptor(auditPath, outputFile + ".removed");
    assert.neq(waitProgram(decryptPid), 0);

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
        jsTest.log("Testing integrity checks when decrypting audit file from standalone");
        testAuditLogIntegrity(standaloneFixture, false, keyManagerFixture);
    }
    {
        const shardingFixture = new ShardingFixture();
        jsTest.log("Testing integrity checks when decrypting audit file from sharded cluster");
        testAuditLogIntegrity(shardingFixture, true, keyManagerFixture);
    }
}
})();
