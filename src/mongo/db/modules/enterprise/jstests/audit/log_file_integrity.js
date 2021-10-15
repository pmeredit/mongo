/**
 * Tests that modifications to the encrypted audit log file
 * can be detected on decrypt.
 */

load('src/mongo/db/modules/enterprise/jstests/audit/lib/audit.js');
load('jstests/ssl/libs/ssl_helpers.js');

(function() {

"use strict";

if (!TestData.setParameters.featureFlagAtRestEncryption) {
    // Don't accept option when FF not enabled.
    assert.throws(
        () => MongoRunner.runMongodAuditLogger({auditCompressionMode: "zstd"}, false /* isBSON */));
    return;
}

if (determineSSLProvider() !== "windows") {
    run("chmod", "600", AUDIT_LOCAL_KEY_ENCRYPT_KEYFILE);
}

print("Testing audit log integrity checks are performed by the decryptor");
function testAuditLogHeader(fixture, isMongos) {
    let opts = {
        auditLocalKeyFile: AUDIT_LOCAL_KEY_ENCRYPT_KEYFILE,
        auditCompressionMode: "zstd",
    };
    if (isMongos) {
        opts = {other: {mongosOptions: opts}};
    }

    const runId = ISODate().getTime();

    jsTest.log("Testing: " + tojson(opts));
    const {conn, audit, admin} = fixture.startProcess(opts);

    // need at least 4 lines to perform this test
    assert.soon(() => {
        audit.resetAuditLine();
        audit.getNextEntryNoParsing();
        audit.fastForward();
        return audit.getCurrentAuditLine() >= 4;
    }, "Audit log must have at least 4 entries including header");

    fixture.stopProcess();

    // TEST 1: normal output decrypts successfully
    jsTest.log("Testing decrypt of normal audit log succeeds");

    let auditPath = conn.fullOptions.auditPath;
    const outputFile = MongoRunner.dataPath + "decompressedLog_" + runId + ".json";

    let decryptPid = _startMongoProgram(
        "mongoauditdecrypt", "--inputPath", auditPath, "--outputPath", outputFile, "--noConfirm");

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

    decryptPid = _startMongoProgram("mongoauditdecrypt",
                                    "--inputPath",
                                    auditPath,
                                    "--outputPath",
                                    outputFile + ".swapped",
                                    "--noConfirm");
    assert.neq(waitProgram(decryptPid), 0);

    // TEST 3: modify audit file by removing lines, verify decrypt fails
    jsTest.log("Testing decrypt fails if log lines are removed");
    audit.resetAuditLine();
    lines = audit.getAllLines();

    // erase lines between the first 2 lines and the last line
    lines.splice(2, lines.length - 3);

    auditPath = MongoRunner.dataPath + "auditLog_" + runId + ".removed";
    writeFile(auditPath, lines.join("\n"));

    decryptPid = _startMongoProgram("mongoauditdecrypt",
                                    "--inputPath",
                                    auditPath,
                                    "--outputPath",
                                    outputFile + ".removed",
                                    "--noConfirm");
    assert.neq(waitProgram(decryptPid), 0);
}

{
    const standaloneFixture = new StandaloneFixture();

    jsTest.log("Testing integrity checks when decrypting audit file from standalone");
    testAuditLogHeader(standaloneFixture, false);
}

sleep(2000);

{
    const shardingFixture = new ShardingFixture();

    jsTest.log("Testing integrity checks when decrypting audit file from sharded cluster");
    testAuditLogHeader(shardingFixture, true);
}
})();
