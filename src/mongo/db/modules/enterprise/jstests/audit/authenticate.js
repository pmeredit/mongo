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

    // Check for negative auditing of authentications.
    audit.fastForward();
    assert(!db.auth({mechanism: authmech, user: "user1", pwd: "wrong_pwd"}));
    const failure =
        audit.assertEntry("authenticate", {user: "user1", db: "test", mechanism: authmech});
    assert.eq(failure.result, ErrorCodes.AuthenticationFailed);

    MongoRunner.stopMongod(m);
    print("SUCCESS audit-authenticate.js");
})();
