// Verify auth is sent to audit log

(function() {
    'use strict';

    load('src/mongo/db/modules/enterprise/jstests/audit/audit.js');
    print("START audit-authenticate.js");

    // Be specific about the mechanism in case the default changes
    const authmech = "SCRAM-SHA-1";
    const port = allocatePort();
    const m = MongoRunner.runMongodAuditLogger(
        {setParameter: "authenticationMechanisms=" + authmech, port: port, auth: ''});
    const audit = m.auditSpooler();
    const admin = m.getDB("admin");
    const db = m.getDB("test");

    assert.commandWorked(admin.runCommand({createUser: "admin", pwd: "pwd", roles: ['root']}));
    assert(admin.auth("admin", "pwd"));

    assert.commandWorked(db.runCommand({createUser: "user1", pwd: "pwd", roles: []}));
    admin.logout();

    // Check for positive auditing of authentications.
    audit.fastForward();
    assert(db.auth({mechanism: authmech, user: "user1", pwd: "pwd"}));
    const success =
        audit.assertEntry("authenticate", {user: "user1", db: "test", mechanism: authmech});
    assert.eq(success.result, 0);
    audit.assertNoNewEntries("authenticate");

    // Check that connecting via shell only audits once.
    audit.fastForward();
    const uri = 'mongodb://user1:pwd@localhost:' + port + '/test';
    const cmd = 'db.coll1.find({});';
    const shell = runMongoProgram('mongo', uri, '--eval', cmd);
    const shellSuccess =
        audit.assertEntry("authenticate", {user: "user1", db: "test", mechanism: authmech});
    assert.eq(shellSuccess.result, 0);
    audit.assertNoNewEntries("authenticate");

    // Negative auditing (incorrect password).
    audit.fastForward();
    assert(!db.auth({mechanism: authmech, user: "user1", pwd: "wrong_pwd"}));
    const pwdFailure =
        audit.assertEntry("authenticate", {user: "user1", db: "test", mechanism: authmech});
    assert.eq(pwdFailure.result, ErrorCodes.AuthenticationFailed);
    audit.assertNoNewEntries("authenticate");

    // Negative auditing (unknown user).
    audit.fastForward();
    assert(!db.auth({mechanism: authmech, user: "unknown_user", pwd: "pwd"}));
    const userFailure =
        audit.assertEntry("authenticate", {user: "unknown_user", db: "test", mechanism: authmech});
    assert.eq(userFailure.result, ErrorCodes.AuthenticationFailed);
    audit.assertNoNewEntries("authenticate");

    // Negative auditing (unknown mechanism).
    // Explicitly call saslStart to avoid hitting client failure at unknown mechanism.
    audit.fastForward();
    assert.commandFailed(db.runCommand({saslStart: 1, mechanism: "HAXX", payload: ""}));
    const mechFailure =
        audit.assertEntry("authenticate", {user: "", db: "test", mechanism: "HAXX"});
    assert.eq(mechFailure.result, ErrorCodes.BadValue);
    audit.assertNoNewEntries("authenticate");

    MongoRunner.stopMongod(m);
    print("SUCCESS audit-authenticate.js");
})();
