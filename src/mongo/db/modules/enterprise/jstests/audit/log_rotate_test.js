// Tests the functionality of rotate log.

load('src/mongo/db/modules/enterprise/jstests/audit/lib/audit.js');

(function() {

'use strict';

const kMongoDStartupID = 4615611;
const kLogRotationInitiatedID = 23166;

let logPath = MongoRunner.dataPath + "mongod.log";
let auditPath = MongoRunner.dataPath + "audit.log";

// Checks the logPath defined above for the specific ID. Does not use any system logs or joint
// logs.
function ContainsLogWithId(id) {
    return cat(logPath).trim().split("\n").some((line) => JSON.parse(line).id === id);
}

// Starts a mongod with auditing and creates an admin database with a user "user1" on the db.
// Returns the mongod handle, the handle to the auditSpooler, and the handle to the admin db.
function startProcess() {
    const conn = MongoRunner.runMongodAuditLogger({
        logpath: logPath,
        auth: "",
        setParameter: "auditAuthorizationSuccess=true",
        auditPath: auditPath,
    },
                                                  false);

    const audit = conn.auditSpooler();
    const admin = conn.getDB("admin");

    assert.commandWorked(
        admin.runCommand({createUser: "user1", pwd: "pwd", roles: [{role: "root", db: "admin"}]}));

    assert(admin.auth({user: "user1", pwd: "pwd"}));
    return {"conn": conn, "audit": audit, "admin": admin};
}

print("Testing functionality of rotating both logs.");
{
    const {conn, audit, admin} = startProcess();

    assert(ContainsLogWithId(kMongoDStartupID));
    audit.assertEntry("createDatabase", {"ns": "admin"});

    // We need this sleep in case the server startup happens so quickly that the log tries to
    // be rotated to the same name as an existing log archive file from the previous shutdown.
    sleep(2000);

    assert.commandWorked(admin.adminCommand({logRotate: 1}));

    // Log message for "Log rotation initiated".
    assert(checkLog.checkContainsOnceJson(conn, kLogRotationInitiatedID));

    admin.auth({user: "user1", pwd: "wrong"});

    // One of the server startup logs is kMongoDStartupID. We expect the log to have rotated, so
    // the old logs should not have that ID.
    assert(!ContainsLogWithId(kMongoDStartupID));

    // The create database audit event should not be in the logs either, since that action
    // was performed before we rotated the logs.
    audit.resetAuditLine();
    audit.assertNoEntry("createDatabase", {"ns": "admin"});
    audit.assertEntry("authenticate",
                      {"user": "user1", "db": "admin", "mechanism": "SCRAM-SHA-256"});

    MongoRunner.stopMongod(conn);
}

// We need this sleep in case the server shutdown happens so quickly that the log tries to
// be moved to the same name as an existing log archive file from the previous rotate.
sleep(2000);

print("Testing functionality of rotating just the server log.");
{
    const {conn, audit, admin} = startProcess();

    assert(ContainsLogWithId(kMongoDStartupID));

    // See the comment below "Testing functionality of rotating both logs" for details.
    sleep(2000);

    assert.commandWorked(admin.adminCommand({logRotate: "server"}));
    assert(checkLog.checkContainsOnceJson(conn, kLogRotationInitiatedID, {"logType": "server"}));

    admin.auth({user: "user1", pwd: "wrong"});

    // We should see the startup audit event in the audit log, but should
    // not see the startup message in the global log.
    assert(!ContainsLogWithId(kMongoDStartupID));

    audit.assertEntry("createDatabase", {"ns": "admin"});

    MongoRunner.stopMongod(conn);
}

// This sleep serves the same function as the one above rotating just the server log.
sleep(2000);

print("Testing functionality of rotating just the audit log.");
{
    const {conn, audit, admin} = startProcess();

    audit.assertEntry("createDatabase", {"ns": "admin"});

    assert.commandWorked(admin.adminCommand({logRotate: "audit"}));
    assert(checkLog.checkContainsOnceJson(conn, kLogRotationInitiatedID, {"logType": "audit"}));

    admin.auth({user: "user1", pwd: "wrong"});

    // We should not see the startup audit event in the audit log, but
    // should see the startup message in the global log.
    assert(ContainsLogWithId(kMongoDStartupID));

    audit.resetAuditLine();
    audit.assertNoEntry("createDatabase", {"ns": "admin"});
    audit.assertEntry("authenticate",
                      {"user": "user1", "db": "admin", "mechanism": "SCRAM-SHA-256"});

    MongoRunner.stopMongod(conn);
}
})();