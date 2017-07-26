// Verify shutdown is sent to audit log

(function() {
    'use strict';

    load('src/mongo/db/modules/enterprise/jstests/audit/audit.js');
    print("START audit-shutdown.js");

    var m = MongoRunner.runMongodAuditLogger({});
    var audit = m.auditSpooler();
    var admin = m.getDB("admin");

    // We actually expect shutdownServer() to return undefined
    // since the connection closes immediately.
    // So interpret an undefined result as OK.
    // The audit entry ensures it was processed correctly.
    assert(admin.shutdownServer({timeout: 3}) === undefined);
    audit.assertEntry("shutdown", {});

    print("SUCCESS audit-shutdown.js");
})();
