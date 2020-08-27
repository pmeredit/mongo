// Verify logApplicationMessage is sent to audit log

(function() {
'use strict';

load('src/mongo/db/modules/enterprise/jstests/audit/lib/audit.js');

function runTest(asBSON) {
    jsTest.log("START audit-log-application-message.js " + tojson(asBSON));

    const m = MongoRunner.runMongodAuditLogger({}, asBSON);
    const audit = m.auditSpooler();
    const db = m.getDB("test");

    // Test null byte separately.
    // We expect this to fail during command parsing when the message
    // is treated like a namespace (due to BasicCommand handling).
    assert.commandFailedWithCode(db.runCommand({logApplicationMessage: "Hello\u0000World"}),
                                 [ErrorCodes.InvalidNamespace]);

    // Write and check for an audit message containing any character except null byte.
    for (let i = 1; i < 256; ++i) {
        try {
            const msg = "Hello" + String.fromCharCode(i) + "World";
            assert.commandWorked(db.runCommand({logApplicationMessage: msg}));
            audit.assertEntry("applicationMessage", {msg: msg});
        } catch (e) {
            jsTest.log("Failed at i=" + i + ", asBSON=" + tojson(asBSON));
            throw e;
        }
    }

    MongoRunner.stopMongod(m);
    jsTest.log("SUCCESS audit-log-application-message.js " + tojson(asBSON));
}

// Test with both JSON and BSON files to ensure some coverage for each.
runTest(true);
runTest(false);
})();
