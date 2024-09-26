// Tests the functionality of rotate log.
import {
    ContainsLogWithId,
    ShardingFixture,
    StandaloneFixture
} from "src/mongo/db/modules/enterprise/jstests/audit/lib/audit.js";

const kListeningOnID = 23015;
const kLogRotationInitiatedID = 23166;

print("Testing functionality of rotating both logs.");
function testRotateLogs(fixture) {
    {
        print("Testing functionality of rotating both the server and audit logs.");
        const {conn, audit, admin} = fixture.startProcess();
        fixture.createUserAndAuth();

        admin.auth({user: "user2", pwd: "wrong"});

        assert(ContainsLogWithId(kListeningOnID, fixture));
        audit.assertEntry("authenticate",
                          {"user": "user2", "db": "admin", "mechanism": "SCRAM-SHA-256"});

        // We need this sleep in case the server startup happens so quickly that the log tries to
        // be rotated to the same name as an existing log archive file from the previous shutdown.
        sleep(2000);

        assert.commandWorked(admin.adminCommand({logRotate: 1}));

        // Log message for "Log rotation initiated".
        assert(checkLog.checkContainsOnceJson(conn, kLogRotationInitiatedID));

        admin.auth({user: "user1", pwd: "wrong"});

        // One of the server startup logs is kMongoDStartupID. We expect the log to have rotated, so
        // the old logs should not have that ID.
        assert(!ContainsLogWithId(kListeningOnID, fixture));

        // The auth audit event should not be in the logs either, since that action
        // was performed before we rotated the logs. The new auth event should be there.
        audit.resetAuditLine();
        audit.assertNoEntry("authenticate",
                            {"user": "user2", "db": "admin", "mechanism": "SCRAM-SHA-256"});
        audit.assertEntry("authenticate",
                          {"user": "user1", "db": "admin", "mechanism": "SCRAM-SHA-256"});

        fixture.stopProcess();
    }

    // We need this sleep in case the server shutdown happens so quickly that the log tries to
    // be moved to the same name as an existing log archive file from the previous rotate.
    sleep(2000);

    {
        print("Testing functionality of rotating just the server log.");
        const {conn, audit, admin} = fixture.startProcess();
        fixture.createUserAndAuth();

        admin.auth({user: "user1", pwd: "wrong"});

        assert(ContainsLogWithId(kListeningOnID, fixture));
        audit.assertEntry("authenticate",
                          {"user": "user1", "db": "admin", "mechanism": "SCRAM-SHA-256"});

        // See the comment below "Testing functionality of rotating both logs" for details.
        sleep(2000);

        assert.commandWorked(admin.adminCommand({logRotate: "server"}));
        assert(
            checkLog.checkContainsOnceJson(conn, kLogRotationInitiatedID, {"logType": "server"}));

        // We should see the auth audit event in the audit log, but should
        // not see the startup message in the global log.
        assert(!ContainsLogWithId(kListeningOnID, fixture));
        audit.resetAuditLine();
        audit.assertEntry("authenticate",
                          {"user": "user1", "db": "admin", "mechanism": "SCRAM-SHA-256"});

        fixture.stopProcess();
    }

    // This sleep serves the same function as the one above rotating just the server log.
    sleep(2000);

    {
        print("Testing functionality of rotating just the audit log.");
        const {conn, audit, admin} = fixture.startProcess();
        fixture.createUserAndAuth();

        admin.auth({user: "user1", pwd: "wrong"});

        assert(ContainsLogWithId(kListeningOnID, fixture));
        audit.assertEntry("authenticate",
                          {"user": "user1", "db": "admin", "mechanism": "SCRAM-SHA-256"});

        // Audit logs are rotated on startup, so this will be the second audit rotate after the
        // startup of the process.
        sleep(2000);

        assert.commandWorked(admin.adminCommand({logRotate: "audit"}));
        sleep(1000);

        audit.resetAuditLine();
        const auditLine = audit.assertEntry("rotateLog");

        // Check that a log rotation status was logged
        assert.neq(auditLine.param.logRotationStatus, undefined);
        assert.neq(auditLine.param.pid, undefined);
        assert.neq(auditLine.param.osInfo, undefined);
        assert.neq(auditLine.param.osInfo.name, undefined);
        assert.neq(auditLine.param.osInfo.version, undefined);
        // Ensure we have a client connection
        assert.neq(auditLine.local, undefined);
        assert.neq(auditLine.remote, undefined);

        assert(checkLog.checkContainsOnceJson(conn, kLogRotationInitiatedID, {"logType": "audit"}));

        admin.auth({user: "user2", pwd: "wrong"});

        // We should not see the startup audit event in the audit log, but
        // should see the startup message in the global log.
        assert(ContainsLogWithId(kListeningOnID, fixture));

        audit.resetAuditLine();
        audit.assertNoEntry("authenticate",
                            {"user": "user1", "db": "admin", "mechanism": "SCRAM-SHA-256"});
        audit.assertEntry("authenticate",
                          {"user": "user2", "db": "admin", "mechanism": "SCRAM-SHA-256"});

        fixture.stopProcess();
    }
}

{
    const standaloneFixture = new StandaloneFixture();

    jsTest.log("Testing rotate functionality on standalone");
    testRotateLogs(standaloneFixture);
}

sleep(2000);

{
    const shardingFixture = new ShardingFixture();

    jsTest.log("Testing rotate functionality on sharded cluster");
    testRotateLogs(shardingFixture);
}
