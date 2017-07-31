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
    audit.fastForward();

    assert(db.auth({mechanism: authmech, user: "user1", pwd: "pwd"}));
    audit.assertEntry("authenticate", {user: "user1", db: "test", mechanism: authmech});

    print("SUCCESS audit-authenticate.js");
})();
