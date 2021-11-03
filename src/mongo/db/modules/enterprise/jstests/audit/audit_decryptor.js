/**
 * Tests that mongoauditdecrypt can successfully decompress a file
 * with logs compressed with zstd and encoded with base64
 */

load('src/mongo/db/modules/enterprise/jstests/audit/lib/audit.js');
load('src/mongo/db/modules/enterprise/jstests/encryptdb/libs/helpers.js');
load('jstests/ssl/libs/ssl_helpers.js');

(function() {

"use strict";

const kmipServerPort = 6566;

if (determineSSLProvider() === "windows") {
    // windows doesn't currently support GCM, so
    // the tests below will fail.
    return;
}

run("chmod", "600", AUDIT_LOCAL_KEY_ENCRYPT_KEYFILE);

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
function testAuditLogDecryptor(fixture, isMongos, useKmip) {
    let opts;
    let kmipPID;
    if (useKmip) {
        kmipPID = startPyKMIPServer(kmipServerPort);
        const kmipUID = createPyKMIPKey(kmipServerPort);
        opts = {
            kmipServerName: "127.0.0.1",
            kmipPort: kmipServerPort,
            kmipServerCAFile: "jstests/libs/trusted-ca.pem",
            kmipClientCertificateFile: "jstests/libs/trusted-client.pem",
            auditEncryptionKeyUID: kmipUID,
            auditCompressionMode: "zstd"
        };
    } else {
        opts = {auditLocalKeyFile: AUDIT_LOCAL_KEY_ENCRYPT_KEYFILE, auditCompressionMode: "zstd"};
    }
    if (isMongos) {
        opts = {other: {mongosOptions: opts}};
    }

    jsTest.log("Testing: " + tojson(opts));
    const {conn, audit, admin} = fixture.startProcess(opts);

    // Skips first line since it's the header
    audit.setCurrentAuditLine(audit.getCurrentAuditLine() + 1);

    assert.soon(() => {
        const auditLine = audit.getNextEntryNoParsing();

        const base64Line = getCompressedMessage(auditLine);

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
    let decryptPid;
    if (useKmip) {
        decryptPid = _startMongoProgram("mongoauditdecrypt",
                                        "--inputPath",
                                        auditPath,
                                        "--outputPath",
                                        outputFile,
                                        "--noConfirm",
                                        "--kmipServerName",
                                        "127.0.0.1",
                                        "--kmipPort",
                                        kmipServerPort,
                                        "--kmipServerCAFile",
                                        "jstests/libs/trusted-ca.pem",
                                        "--kmipClientCertificateFile",
                                        "jstests/libs/trusted-client.pem");
    } else {
        decryptPid = _startMongoProgram("mongoauditdecrypt",
                                        "--inputPath",
                                        auditPath,
                                        "--outputPath",
                                        outputFile,
                                        "--noConfirm");
    }
    assert.eq(waitProgram(decryptPid), 0);

    const outputAudit = new AuditSpooler(outputFile, false);
    const outputLine = outputAudit.getNextEntryNoParsing();

    // Audit log was decoded and decompressed so it was able to be parsed
    assert.eq(isValidJSON(outputLine), true);
    if (useKmip) {
        killPyKMIPServer(kmipPID);
    }
}

jsTest.log("Testing with local key file");

{
    const standaloneFixture = new StandaloneFixture();

    jsTest.log("Testing audit file from standalone can be decompressed with mongoauditdecrypt" +
               " using local key");
    testAuditLogDecryptor(standaloneFixture, false, false);
}

{
    const shardingFixture = new ShardingFixture();

    jsTest.log(
        "Testing audit file from sharded cluster can be decompressed with mongoauditdecrypt" +
        " using local key");
    testAuditLogDecryptor(shardingFixture, true, false);
}

jsTest.log("Testing with KMIP-based key");

{
    const standaloneFixture = new StandaloneFixture();

    jsTest.log("Testing audit file from standalone can be decompressed with mongoauditdecrypt" +
               " using KMIP");
    testAuditLogDecryptor(standaloneFixture, false, true);
}

{
    const shardingFixture = new ShardingFixture();

    jsTest.log(
        "Testing audit file from sharded cluster can be decompressed with mongoauditdecrypt" +
        " using KMIP");
    testAuditLogDecryptor(shardingFixture, true, true);
}
})();
