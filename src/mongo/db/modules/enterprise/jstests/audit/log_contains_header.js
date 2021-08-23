/**
 * Tests that the audit log contains a valid header at the top
 * when compression is enabled
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
 * Tries parsing a header line and checking it contains all properties,
 * will return true if successful
 */
function isValidHeader(json) {
    try {
        const fileheader = JSON.parse(json);
        const properties = [
            "ts",
            "version",
            "compressionMode",
            "keyStoreIdentifier",
            "encryptedKey",
            "auditRecordType"
        ];

        for (let prop of properties) {
            if (!fileheader.hasOwnProperty(prop)) {
                return false;
            }
        }

        // Verify that the audit file header contains no unknown properties.
        if (Object.keys(fileheader).filter((k) => !properties.includes(k)).length) {
            return false;
        }
        return true;
    } catch (e) {
        return false;
    }
}

print("Testing audit log contains header when compression is enabled");
function testAuditLogHeader(fixture, isMongos, enableCompression) {
    let opts = {
        kmipKeyStoreIdentifier: "testKey",
        kmipEncryptionKeyIdentifier: "testKeyIdentifier",
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
        return isValidHeader(fileHeader) == enableCompression;
    }, "Audit log did not contain a valid header line on the top");

    fixture.stopProcess();
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
