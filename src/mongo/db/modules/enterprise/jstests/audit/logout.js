// Verify logout events are sent to audit log.

(function() {
'use strict';

load('src/mongo/db/modules/enterprise/jstests/audit/lib/audit.js');
print('START audit-logout.js');
print('START explicit logout');
// Start mongod and use db admin and test.
const port = allocatePort();
const m = MongoRunner.runMongodAuditLogger({auth: '', port: port});
const audit = m.auditSpooler();
const admin = m.getDB("admin");
const test1 = m.getDB("test1");
const test2 = m.getDB("test2");

// Create users on db test1 and test2 as admin.
assert.commandWorked(admin.runCommand({createUser: "admin", pwd: "pwd", roles: ['root']}));
assert(admin.auth("admin", "pwd"));

assert.commandWorked(test1.runCommand({createUser: "user1", pwd: "pwd1", roles: []}));
assert.commandWorked(test2.runCommand({createUser: "user2", pwd: "pwd2", roles: []}));
assert.commandWorked(admin.logout());

// Check that explicit admin logout was recorded in audit log with no implicit logouts.
audit.assertNoNewEntries("logout", {reason: "Implicit logout due to client connection closure"});
audit.assertEntry("logout", {
    reason: "Explicit logout from db 'admin'",
    initialUsers: [{"user": "admin", "db": "admin"}],
    updatedUsers: []
});

// Login to the databases.
assert(test1.auth({user: "user1", pwd: "pwd1"}));
assert(test2.auth({user: "user2", pwd: "pwd2"}));

// Explicitly log out of test1 and check for audit event.
audit.fastForward();
assert(test1.logout());
let startLine = audit.getCurrentAuditLine();
audit.assertEntry("logout", {
    reason: "Explicit logout from db 'test1'",
    initialUsers: [{"user": "user1", "db": "test1"}, {"user": "user2", "db": "test2"}],
    updatedUsers: [{"user": "user2", "db": "test2"}]
});
audit.setCurrentAuditLine(startLine);
audit.assertNoNewEntries("logout", {reason: "Implicit logout due to client connection closure"});

print('SUCCESS explicit logout');

print('START implicit logout');
// Spawn a separate mongo client to login as user1 to test1 and then quit the shell
audit.fastForward();
const uri = 'mongodb://localhost:' + port;
const cmd = function() {
    db.getSiblingDB("test1").auth("user1", "pwd1");
    db.getSiblingDB("test2").auth("user2", "pwd2");
    quit();
};

runMongoProgram('mongo', uri, '--shell', '--eval', `(${cmd})();`);
startLine = audit.getCurrentAuditLine();
audit.assertEntry("logout", {
    reason: "Implicit logout due to client connection closure",
    initialUsers: [{"user": "user1", "db": "test1"}, {"user": "user2", "db": "test2"}],
    updatedUsers: []
});
audit.setCurrentAuditLine(startLine);
audit.assertNoNewEntries("logout", {reason: /Explicit logout from db '.+'/});

print('SUCCESS implicit logout');

MongoRunner.stopMongod(m);
print("SUCCESS audit-logout.js");
})();