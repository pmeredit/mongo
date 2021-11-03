/**
 * Tests that the audit log contains a valid header at the top
 * when compression is enabled
 */

load('src/mongo/db/modules/enterprise/jstests/audit/lib/audit_encryption.js');
load('jstests/ssl/libs/ssl_helpers.js');

(function() {

"use strict";

if (determineSSLProvider() === "windows") {
    // windows doesn't currently support GCM, so
    // the tests below will fail.
    return;
}

run("chmod", "600", AUDIT_LOCAL_KEY_ENCRYPT_KEYFILE);

print("Testing audit log contains header when compression is enabled");
function testAuditLogHeader(fixture, isMongos, enableCompression) {
    let opts = {
        auditLocalKeyFile: AUDIT_LOCAL_KEY_ENCRYPT_KEYFILE,
    };
    if (enableCompression) {
        opts.auditCompressionMode = "zstd";
    }
    if (isMongos) {
        opts = {other: {mongosOptions: opts}};
    }

    jsTest.log("Testing: " + tojson(opts));
    const {conn, audit, admin} = fixture.startProcess(opts);

    assert.soon(() => {
        audit.resetAuditLine();
        const fileHeader = audit.getNextEntryNoParsing();
        return isValidEncryptedAuditLogHeader(fileHeader);
    }, "Audit log did not contain a valid header line on the top");

    fixture.stopProcess();

    // Add a one-second delay to ensure that the next 'mongod' or
    // 'mongos' run will not rotate to the same filename as the
    // file created during this run
    sleep(1000);
}

{
    const standaloneFixture = new StandaloneFixture();

    jsTest.log("Testing audit file from standalone contains a valid header log");
    testAuditLogHeader(standaloneFixture, false, true);
    testAuditLogHeader(standaloneFixture, false, false);
}

sleep(2000);

{
    const shardingFixture = new ShardingFixture();

    jsTest.log("Testing audit file from sharded cluster contains a valid header log");
    testAuditLogHeader(shardingFixture, true, true);
    testAuditLogHeader(shardingFixture, true, false);
}
})();
