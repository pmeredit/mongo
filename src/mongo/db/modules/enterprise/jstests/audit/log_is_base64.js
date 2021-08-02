// Tests the logs are being written as base64

load('src/mongo/db/modules/enterprise/jstests/audit/lib/audit.js');

(function() {

'use strict';

if (!TestData.setParameters.featureFlagAtRestEncryption) {
    // Don't accept option when FF not enabled.
    assert.throws(() => MongoRunner.runMongod({auditCompressionEnabled: true}));
    return;
}

print("Testing logs being base64.");
function testAuditLineBase64(fixture, isMongos, enableCompression) {
    let opts = {auditCompressionEnabled: enableCompression};
    if (isMongos) {
        opts = {other: {mongosOptions: opts}};
    }

    jsTest.log("Testing: " + tojson(opts));
    const {conn, audit, admin} = fixture.startProcess(opts);

    // We need to sleep to prevent race conditions
    sleep(2000);

    let base64Line = audit.getNextEntryNoParsing();
    base64Line = base64Line.replace(/\n$/, "");
    const base64regex = /^([0-9a-zA-Z+/]{4})*(([0-9a-zA-Z+/]{2}==)|([0-9a-zA-Z+/]{3}=))?$/;
    const isBase64 = base64regex.test(base64Line);
    assert.eq(isBase64, enableCompression, "Got (or not) base64 when it was(n't) expected");

    fixture.stopProcess();
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
