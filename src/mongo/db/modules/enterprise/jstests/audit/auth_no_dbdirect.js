// Check to make sure that there are no entries from DB Direct Client in the audit log

(function() {
'use strict';

load('src/mongo/db/modules/enterprise/jstests/audit/lib/audit.js');

const m =
    MongoRunner.runMongodAuditLogger({auth: "", setParameter: "auditAuthorizationSuccess=true"});

const audit = m.auditSpooler();
const admin = m.getDB("admin");

assert.commandWorked(
    admin.runCommand({createUser: "user1", pwd: "pwd", roles: [{role: "root", db: "admin"}]}));

admin.auth({user: "user1", pwd: "pwd"});

audit.fastForward();

assert.commandWorked(
    admin.runCommand({createUser: "user2", pwd: "pwd2", roles: [{role: "root", db: "admin"}]}));

// The DB direct client runs two commands, calling update on admin.system.version and calling
// insert on admin.system.users. We want to make sure these commands are not logged.
audit.assertNoNewEntries("authCheck", {"command": "update"});
audit.assertNoNewEntries("authCheck", {"command": "insert"});

MongoRunner.stopMongod(m);
})();