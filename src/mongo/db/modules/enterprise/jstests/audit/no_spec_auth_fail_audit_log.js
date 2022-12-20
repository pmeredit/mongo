// Verify that a failed speculative authentication attempt does not cause an audit log.
(function() {
'use strict';

load('src/mongo/db/modules/enterprise/jstests/audit/lib/audit.js');

// Set the authenticationMechanisms server parameter to not contain SCRAM-SHA-256.
const authmech = "SCRAM-SHA-1";
const options = {
    setParameter: {
        authenticationMechanisms: authmech,
    }
};
const mongod = MongoRunner.runMongodAuditLogger(Object.merge(options, {auth: ''}));
const audit = mongod.auditSpooler();

const admin = mongod.getDB("admin");
const db = mongod.getDB("test");
let port = mongod.port;
if (mongod.fullOptions) {
    port = mongod.fullOptions.port;
}

assert.commandWorked(admin.runCommand({createUser: "admin", pwd: "pwd", roles: ['root']}));
assert(admin.auth("admin", "pwd"));

assert.commandWorked(db.runCommand({createUser: "user1", pwd: "pwd", roles: []}));
admin.logout();

// Authenticate via the shell.
audit.fastForward();
const uri = 'mongodb://user1:pwd@localhost:' + port + '/test';
const cmd = 'db.coll1.find({});';
runMongoProgram('mongo', uri, '--eval', cmd);
const shellSuccess =
    audit.assertEntry("authenticate", {user: "user1", db: "test", mechanism: authmech});
assert.eq(shellSuccess.result, 0);

// Assert that no log entry is found for a failed SCRAM-SHA-256 authentication attempt (which would
// have been a speculative one, since SCRAM-SHA-1 was specified as the authentication mechanism
// below). The appearance of this log message could be interepreted as someone trying to log in with
// an incorrect passowrd, which is not the case.
audit.assertNoEntry("authenticate", {user: "user1", db: "test", mechanism: "SCRAM-SHA-256"});

MongoRunner.stopMongod(mongod);
})();
