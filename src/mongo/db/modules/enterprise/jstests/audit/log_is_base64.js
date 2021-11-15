/**
 * Tests that encrypted audit logs are being written as base64
 * @tags: [requires_gcm]
 */

load('src/mongo/db/modules/enterprise/jstests/audit/lib/audit_encryption.js');
load('jstests/ssl/libs/ssl_helpers.js');

(function() {

'use strict';

if (determineSSLProvider() !== "windows") {
    run("chmod", "600", AUDIT_LOCAL_KEY_ENCRYPT_KEYFILE);
}

function messageIsBase64(auditLine) {
    const base64regex = /^([0-9a-zA-Z+/]{4})*(([0-9a-zA-Z+/]{2}==)|([0-9a-zA-Z+/]{3}=))?$/;
    try {
        const auditLineParsed = JSON.parse(auditLine);
        let base64Line = auditLineParsed.log;
        base64Line = base64Line.replace(/\n$/, "");

        return base64regex.test(base64Line);
    } catch (e) {
        return false;
    }
}

print("Testing logs being base64.");
function testAuditLineBase64(fixture, isMongos, enableCompression) {
    const keyManagerFixture = new LocalFixture();
    let opts = keyManagerFixture.generateOptsWithDefaults(enableCompression);

    if (isMongos) {
        opts = {other: {mongosOptions: opts}};
    }

    jsTest.log("Testing: " + tojson(opts));
    const {conn, audit, admin} = fixture.startProcess(opts);

    // Skips first line since it's the header
    audit.setCurrentAuditLine(audit.getCurrentAuditLine() + 1);

    assert.soon(() => {
        const auditLine = audit.getNextEntryNoParsing();

        return messageIsBase64(auditLine);
    }, "Got not base64 when it was expected");

    fixture.stopProcess();

    // Add a one-second delay to ensure that the next 'mongod' or
    // 'mongos' run will not rotate to the same filename as the
    // file created during this run
    sleep(1000);
}

{
    const standaloneFixture = new StandaloneFixture();

    jsTest.log("Testing audit line is base64 on standalone");
    testAuditLineBase64(standaloneFixture, false, true);
    testAuditLineBase64(standaloneFixture, false, false);
}

sleep(2000);

{
    const shardingFixture = new ShardingFixture();

    jsTest.log("Testing audit line is base64 on sharded cluster");
    testAuditLineBase64(shardingFixture, true, true);
    testAuditLineBase64(shardingFixture, true, false);
}
})();
