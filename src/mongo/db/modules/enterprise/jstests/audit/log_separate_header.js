/**
 * Tests that the header metadata log option works
 */

load('src/mongo/db/modules/enterprise/jstests/audit/lib/audit_encryption.js');
load('jstests/ssl/libs/ssl_helpers.js');

(function() {

"use strict";

if (determineSSLProvider() !== "windows") {
    run("chmod", "600", AUDIT_LOCAL_KEY_ENCRYPT_KEYFILE);
}

print("Testing header metadata log duplicates audit log header when compression is enabled");
function testHeaderMetadataLog(fixture, isMongos) {
    const sepHeaderPath = MongoRunner.dataPath + "audit_header.log";
    const enableCompression = true;
    const keyManagerFixture = new LocalFixture();
    let opts = keyManagerFixture.generateOptsWithDefaults(enableCompression);

    const extraOpts = {
        setParameter: {auditEncryptionHeaderMetadataFile: sepHeaderPath},
    };
    opts = mergeDeepObjects(opts, extraOpts);

    if (isMongos) {
        opts = {other: {mongosOptions: opts}};
    }

    jsTest.log("Testing: " + tojson(opts));
    let auditSepHeader = new AuditSpooler(sepHeaderPath, false);
    // Will fail if the file is not created
    try {
        auditSepHeader.fastForward();
    } catch (e) {
        // reset on failure
        auditSepHeader = new AuditSpooler(sepHeaderPath, false);
    }
    const {conn, audit, admin} = fixture.startProcess(opts);

    assert.soon(() => {
        audit.resetAuditLine();
        const fileHeader = audit.getNextEntryNoParsing();
        assert.eq(isValidEncryptedAuditLogHeader(fileHeader, keyManagerFixture.getKeyStoreType()),
                  true);

        const sepFileHeader = auditSepHeader.getNextEntryNoParsing();
        assert.eq(
            isValidEncryptedAuditLogHeader(sepFileHeader, keyManagerFixture.getKeyStoreType()),
            true);

        let jsonFileHeader = JSON.parse(fileHeader);
        let jsonSepFileHeader = JSON.parse(sepFileHeader);
        assert.eq(jsonFileHeader, jsonSepFileHeader);
        return true;
    }, "Header was invalid, or headers did not match across files");

    fixture.stopProcess();
}

{
    const standaloneFixture = new StandaloneFixture();

    jsTest.log("Testing audit file from standalone has matching header logs");
    testHeaderMetadataLog(standaloneFixture, false);
}

sleep(2000);

{
    const shardingFixture = new ShardingFixture();

    jsTest.log("Testing audit file from sharded cluster has matching header logs");
    testHeaderMetadataLog(shardingFixture, true);
}
})();
