/**
 * Tests that mongoauditdecrypt can successfully decompress a file
 * with logs compressed with zstd and encoded with base64
 */

load('src/mongo/db/modules/enterprise/jstests/audit/lib/audit.js');

(function() {

"use strict";

if (!TestData.setParameters.featureFlagAtRestEncryption) {
    // Don't accept option when FF not enabled.
    assert.throws(() => MongoRunner.runMongod({auditCompressionEnabled: true}));
    return;
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

print("Testing audit log decryptor program");
function testAuditLogDecryptor(fixture, isMongos) {
    let opts = {
        auditCompressionMode: "zstd",
        kmipKeyStoreIdentifier: "testKey",
        kmipEncryptionKeyIdentifier: "testKeyIdentifier",
    };
    if (isMongos) {
        opts = {other: {mongosOptions: opts}};
    }

    jsTest.log("Testing: " + tojson(opts));
    const {conn, audit, admin} = fixture.startProcess(opts);

    // Skips first line since it's the header
    audit.setCurrentAuditLine(audit.getCurrentAuditLine() + 1);

    assert.soon(() => {
        const base64Line = audit.getNextEntryNoParsing();
        // Audit log was compressed and encoded as base64 so it can't be parsed
        return !isValidJSON(base64Line);
    }, "Audit log was not compressed and encoded as base64");
    fixture.stopProcess();

    // Prevent race condition trying to read the audit file before it is written and saved
    sleep(2000);

    const auditPath = conn.fullOptions.auditPath;
    const outputFile = MongoRunner.dataPath += "decompressedLog.json";

    jsTest.log("Running mongoauditdecrypt");
    jsTest.log("Decrypting " + auditPath);
    jsTest.log("Saving json file to " + outputFile);
    const decryptPid = _startMongoProgram(
        "mongoauditdecrypt", "--inputPath", auditPath, "--outputPath", outputFile, "--noConfirm");

    assert.eq(waitProgram(decryptPid), 0);

    const outputAudit = new AuditSpooler(outputFile, false);
    const outputLine = outputAudit.getNextEntryNoParsing();

    // Audit log was decoded and decompressed so it was able to be parsed
    assert.eq(isValidJSON(outputLine), true);
}

{
    const standaloneFixture = new StandaloneFixture();

    jsTest.log("Testing audit file from standalone can be decompressed with mongoauditdecrypt");
    testAuditLogDecryptor(standaloneFixture, false);
}

sleep(2000);

{
    const shardingFixture = new ShardingFixture();

    jsTest.log(
        "Testing audit file from sharded cluster can be decompressed with mongoauditdecrypt");
    testAuditLogDecryptor(shardingFixture, true);
}
})();
