/**
 * Tests that the encrypted audit log has correct framing
 * @tags: [requires_gcm]
 */

import {determineSSLProvider} from "jstests/ssl/libs/ssl_helpers.js";
import {
    ShardingFixture,
    StandaloneFixture
} from "src/mongo/db/modules/enterprise/jstests/audit/lib/audit.js";
import {
    AUDIT_LOCAL_KEY_ENCRYPT_KEYFILE,
    LocalFixture
} from "src/mongo/db/modules/enterprise/jstests/audit/lib/audit_encryption.js";

if (determineSSLProvider() !== "windows") {
    run("chmod", "600", AUDIT_LOCAL_KEY_ENCRYPT_KEYFILE);
}

/**
 * Tries parsing an encrypted audit line and checks that it contains all properties.
 * will return true if successful
 */
function isValidFrame(json) {
    try {
        const auditLine = JSON.parse(json);
        const properties = [
            "ts",
            "log",
        ];

        for (let prop of properties) {
            if (!auditLine.hasOwnProperty(prop)) {
                return false;
            }
        }

        // Verify that the audit file header contains no unknown properties.
        if (Object.keys(auditLine).filter((k) => !properties.includes(k)).length) {
            return false;
        }
        return true;
    } catch (e) {
        return false;
    }
}

print("Testing encrypted audit log has valid framing protocol");
function testAuditLogFrame(fixture, isMongos, enableCompression) {
    const keyManagerFixture = new LocalFixture();
    let opts = keyManagerFixture.generateOptsWithDefaults(enableCompression);

    if (isMongos) {
        opts = {other: {mongosOptions: opts}};
    }

    jsTest.log("Testing: " + tojson(opts));
    const {conn, audit, admin} = fixture.startProcess(opts);

    assert.soon(() => {
        const auditLine = audit.getNextEntryNoParsing();
        return isValidFrame(auditLine);
    }, "Audit line does not have valid framing protocol");

    fixture.stopProcess();

    // Add a one-second delay to ensure that the next 'mongod' or
    // 'mongos' run will not rotate to the same filename as the
    // file created during this run
    sleep(1000);
}

{
    const standaloneFixture = new StandaloneFixture();

    jsTest.log("Testing audit log from standalone has a valid framing protocol");
    testAuditLogFrame(standaloneFixture, false, true);
    testAuditLogFrame(standaloneFixture, false, false);
}

sleep(2000);

{
    const shardingFixture = new ShardingFixture();

    jsTest.log("Testing audit log from sharded cluster has a valid framing protocol");
    testAuditLogFrame(shardingFixture, true, true);
    testAuditLogFrame(shardingFixture, true, false);
}
