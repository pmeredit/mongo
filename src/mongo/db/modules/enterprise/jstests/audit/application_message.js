// Verify logApplicationMessage is sent to audit log

(function() {
    'use strict';

    load('src/mongo/db/modules/enterprise/jstests/audit/audit.js');
    print("START audit-log-application-message.js");

    var m = MongoRunner.runMongodAuditLogger({});
    var audit = m.auditSpooler();
    var db = m.getDB("test");

    assert.commandWorked(db.runCommand({logApplicationMessage: "Hello World"}));
    audit.assertEntry("applicationMessage", {msg: "Hello World"});

    print("SUCCESS audit-log-application-message.js");
})();
