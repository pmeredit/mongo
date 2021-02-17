// Tests the functionality of rotate log.

load('src/mongo/db/modules/enterprise/jstests/audit/lib/audit.js');

(function() {

'use strict';

const kListeningOnID = 23015;
const kLogRotationInitiatedID = 23166;

let logPathMongod = MongoRunner.dataPath + "mongod.log";
let logPathMongos = MongoRunner.dataPath + "mongos.log";
let auditPath = MongoRunner.dataPath + "audit.log";

// Checks the logPath defined above for the specific ID. Does not use any system logs or joint
// logs.
function ContainsLogWithId(id, fixture) {
    const logPath = fixture.logPath;
    return cat(logPath).trim().split("\n").some((line) => JSON.parse(line).id === id);
}

print("Testing functionality of rotating both logs.");
let testRotateLogs = function(fixture) {
    {
        print("Testing functionality of rotating both the server and audit logs.");
        var {conn, audit, admin} = fixture.startProcess();

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
        var {conn, audit, admin} = fixture.startProcess();
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
        var {conn, audit, admin} = fixture.startProcess();
        admin.auth({user: "user1", pwd: "wrong"});

        assert(ContainsLogWithId(kListeningOnID, fixture));
        audit.assertEntry("authenticate",
                          {"user": "user1", "db": "admin", "mechanism": "SCRAM-SHA-256"});

        assert.commandWorked(admin.adminCommand({logRotate: "audit"}));
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
};

{
    class StandaloneFixture {
        constructor() {
        }

        startProcess() {
            const conn = MongoRunner.runMongodAuditLogger({
                logpath: logPathMongod,
                auth: "",
                setParameter: "auditAuthorizationSuccess=true",
                auditPath: auditPath,
            },
                                                          false);

            this.audit = conn.auditSpooler();
            this.admin = conn.getDB("admin");

            assert.commandWorked(this.admin.runCommand(
                {createUser: "user1", pwd: "pwd", roles: [{role: "root", db: "admin"}]}));

            assert(this.admin.auth({user: "user1", pwd: "pwd"}));

            assert.commandWorked(this.admin.runCommand(
                {createUser: "user2", pwd: "pwd", roles: [{role: "root", db: "admin"}]}));

            this.conn = conn;
            this.logPath = logPathMongod;
            return {"conn": this.conn, "audit": this.audit, "admin": this.admin};
        }

        stopProcess() {
            MongoRunner.stopMongod(this.conn);
        }
    }

    let standaloneFixture = new StandaloneFixture();

    jsTest.log("Testing rotate functionality on standalone");
    testRotateLogs(standaloneFixture);
}

sleep(2000);

{
    class ShardingFixture {
        constructor() {
        }

        startProcess() {
            const st = MongoRunner.runShardedClusterAuditLogger({
                mongos: 1,
                config: 1,
                shards: 1,
                other: {
                    mongosOptions: {
                        logpath: logPathMongos,
                        auth: null,
                        setParameter: "auditAuthorizationSuccess=true",
                        auditPath: auditPath,
                        auditDestination: "file",
                        auditFormat: "JSON",
                    },
                }
            });

            this.audit = st.s0.auditSpooler();
            this.admin = st.s0.getDB("admin");

            assert.commandWorked(this.admin.runCommand(
                {createUser: "user1", pwd: "pwd", roles: [{role: "root", db: "admin"}]}));

            assert(this.admin.auth({user: "user1", pwd: "pwd"}));

            assert.commandWorked(this.admin.runCommand(
                {createUser: "user2", pwd: "pwd", roles: [{role: "root", db: "admin"}]}));

            this.st = st;
            this.logPath = logPathMongos;
            return {"conn": this.st.s0, "audit": this.audit, "admin": this.admin};
        }

        stopProcess() {
            this.st.stop();
        }
    }

    let shardingFixture = new ShardingFixture();

    jsTest.log("Testing rotate functionality on sharded cluster");
    testRotateLogs(shardingFixture);
}
})();