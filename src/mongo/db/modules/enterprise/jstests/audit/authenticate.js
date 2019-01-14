// Verify auth is sent to audit log

(function() {
    'use strict';

    load('src/mongo/db/modules/enterprise/jstests/audit/audit.js');
    print("START audit-authenticate.js");

    // Be specific about the mechanism in case the default changes
    var authmech = "SCRAM-SHA-1";
    var m =
        MongoRunner.runMongodAuditLogger({setParameter: "authenticationMechanisms=" + authmech});
    var audit = m.auditSpooler();
    var db = m.getDB("test");

    assert.commandWorked(db.runCommand({createUser: "user1", pwd: "pwd", roles: []}));

    // Check for positive auditing of authentications.
    audit.fastForward();
    assert(db.auth({mechanism: authmech, user: "user1", pwd: "pwd"}));
    const success =
        audit.assertEntry("authenticate", {user: "user1", db: "test", mechanism: authmech});
    assert.eq(success.result, 0);

    // Negative auditing (incorrect password).
    audit.fastForward();
    assert(!db.auth({mechanism: authmech, user: "user1", pwd: "wrong_pwd"}));
    const pwdFailure =
        audit.assertEntry("authenticate", {user: "user1", db: "test", mechanism: authmech});
    assert.eq(pwdFailure.result, ErrorCodes.AuthenticationFailed);

    // Negative auditing (unknown user).
    audit.fastForward();
    assert(!db.auth({mechanism: authmech, user: "unknown_user", pwd: "pwd"}));
    const userFailure =
        audit.assertEntry("authenticate", {user: "unknown_user", db: "test", mechanism: authmech});
    assert.eq(userFailure.result, ErrorCodes.AuthenticationFailed);

    // Negative auditing (unknown mechanism).
    // Explicitly call saslStart to avoid hitting client failure at unknown mechanism.
    audit.fastForward();
    assert.commandFailed(db.runCommand({saslStart: 1, mechanism: "HAXX", payload: ""}));
    const mechFailure =
        audit.assertEntry("authenticate", {user: "", db: "test", mechanism: "HAXX"});
    assert.eq(mechFailure.result, ErrorCodes.BadValue);

    MongoRunner.stopMongod(m);
    print("SUCCESS audit-authenticate.js");
})();
