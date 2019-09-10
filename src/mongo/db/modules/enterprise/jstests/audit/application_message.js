// Verify logApplicationMessage is sent to audit log

(function() {
'use strict';

load('src/mongo/db/modules/enterprise/jstests/audit/lib/audit.js');
print("START audit-log-application-message.js");

// Test with BSON files to ensure some coverage for BSON
var m = MongoRunner.runMongodAuditLogger({}, true);
var audit = m.auditSpooler();
var db = m.getDB("test");

assert.commandWorked(db.runCommand({logApplicationMessage: "Hello World"}));
audit.assertEntry("applicationMessage", {msg: "Hello World"});

MongoRunner.stopMongod(m);
print("SUCCESS audit-log-application-message.js");
})();
