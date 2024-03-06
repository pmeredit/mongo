/**
 * Tests that mongoauditdecrypt can successfully decrypt an
 * encrypted audit log file, which is generated for each of
 * the key store types and wrapping methods.
 * @tags: [uses_pykmip]
 */

load('src/mongo/db/modules/enterprise/jstests/audit/lib/audit_encryption.js');
load('jstests/ssl/libs/ssl_helpers.js');

(function() {

"use strict";

// note: this port number must be unique among the audit tests that use
// the PyKMIP server, so that these tests can run in parallel without
// EADDRINUSE issues.
const kmipServerPort = 6566;

if (determineSSLProvider() !== "windows") {
    run("chmod", "600", AUDIT_LOCAL_KEY_ENCRYPT_KEYFILE);
}

/**
 * Tries parsing a JSON line, will return true if successful
 */
function isValidJSON(json) {
    try {
        JSON.parse(json);
        return true;
    } catch (e) {
        return false;
    }
}

/**
 * Returns the compressed message, will return {error: true} if fail
 */
function getCompressedMessage(json) {
    try {
        const auditLineParsed = JSON.parse(json);
        return auditLineParsed.log;
    } catch (e) {
        return {error: true};
    }
}

print("Testing audit log decryptor program");
function testAuditLogDecryptor(serverFixture, isMongos, keyManagerFixture) {
    const enableCompression = true;
    keyManagerFixture.startKeyServer();
    let opts = keyManagerFixture.generateOptsWithDefaults(enableCompression);

    if (isMongos) {
        opts = {other: {mongosOptions: opts}};
    }

    jsTest.log("Testing: " + tojson(opts));
    const {conn, audit, admin} = serverFixture.startProcess(opts);

    // Skips first line since it's the header
    audit.setCurrentAuditLine(audit.getCurrentAuditLine() + 1);

    assert.soon(() => {
        const auditLine = audit.getNextEntryNoParsing();

        const base64Line = getCompressedMessage(auditLine);

        // Audit log was compressed and encoded as base64 so it can't be parsed
        return !isValidJSON(base64Line);
    }, "Audit log was not compressed and encoded as base64");
    serverFixture.stopProcess();

    // Prevent race condition trying to read the audit file before it is written and saved
    sleep(2000);

    const auditPath = conn.fullOptions.auditPath;
    const runId = (isMongos ? "sharded" : "standalone") + "_" + keyManagerFixture.getKeyStoreType();
    const outputFile = MongoRunner.dataPath + "decompressedLog_" + runId + ".json";
    const decryptPid = keyManagerFixture.runDecryptor(auditPath, outputFile);
    assert.eq(waitProgram(decryptPid), 0);

    const outputAudit = new AuditSpooler(outputFile, false);
    const outputLine = outputAudit.getNextEntryNoParsing();

    // Audit log was decoded and decompressed so it was able to be parsed
    assert.eq(isValidJSON(outputLine), true);

    keyManagerFixture.stopKeyServer();
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
        jsTest.log("Testing decrypt of audit file from standalone");
        testAuditLogDecryptor(standaloneFixture, false, keyManagerFixture);
    }
    {
        const shardingFixture = new ShardingFixture();
        jsTest.log("Testing decrypt of audit file fromm sharded cluster");
        testAuditLogDecryptor(shardingFixture, true, keyManagerFixture);
    }
}
})();
