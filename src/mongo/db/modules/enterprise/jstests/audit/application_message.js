// Verify logApplicationMessage is sent to audit log
import "src/mongo/db/modules/enterprise/jstests/audit/lib/audit.js";

function test(audit, db, asBSON) {
    jsTest.log("START audit-log-application-message.js " + tojson(asBSON));

    // Write and check for an audit message containing any character except null byte.
    for (let i = 0; i < 256; ++i) {
        try {
            const msg = "Hello" + String.fromCharCode(i) + "World";
            assert.commandWorked(db.runCommand({logApplicationMessage: msg}));
            audit.assertEntry("applicationMessage", {msg: msg});
        } catch (e) {
            jsTest.log("Failed at i=" + i + ", asBSON=" + tojson(asBSON));
            throw e;
        }
    }

    jsTest.log("SUCCESS audit-log-application-message.js " + tojson(asBSON));
}

function runMongodTest(format) {
    const m = MongoRunner.runMongodAuditLogger({}, format);
    const audit = m.auditSpooler();
    const db = m.getDB("test");

    test(audit, db, format);
    MongoRunner.stopMongod(m);
}

function runShardedTest(format) {
    const st = MongoRunner.runShardedClusterAuditLogger({}, {}, format);
    const auditMongos = st.s0.auditSpooler();
    const db = st.s0.getDB("test");

    test(auditMongos, db, format);
    st.stop();
}

// Test with both JSON and BSON files to ensure some coverage for each.
runMongodTest("JSON");
runMongodTest("BSON");

runShardedTest("JSON");
runShardedTest("BSON");
