// Verify logout events are sent to audit log.

(function() {
'use strict';

load('src/mongo/db/modules/enterprise/jstests/audit/lib/audit.js');

const kExplicitLogoutMessage = "Logging out on user request";
const kImplicitLogoutMessage = "Client has disconnected";

let runTest = function(conn) {
    const port = conn.port;
    const audit = conn.auditSpooler();
    const admin = conn.getDB("admin");
    const test = conn.getDB("test");

    // Create users on db test1 and test2 as admin.
    assert.commandWorked(admin.runCommand({createUser: "admin", pwd: "pwd", roles: ['root']}));

    assert(admin.auth("admin", "pwd"));
    assert.commandWorked(test.runCommand({createUser: "user", pwd: "pwd", roles: []}));
    assert.commandWorked(admin.logout());

    // Check that explicit admin logout was recorded in audit log with no implicit logouts.
    audit.assertNoNewEntries("logout", {reason: kImplicitLogoutMessage});
    audit.assertEntry("logout", {
        reason: kExplicitLogoutMessage,
        initialUsers: [{"user": "admin", "db": "admin"}],
        updatedUsers: []
    });

    // Explicitly log out check for audit event.
    print('START explicit logout');
    assert(test.auth({user: "user", pwd: "pwd"}));
    audit.fastForward();
    assert(test.logout());

    let startLine = audit.getCurrentAuditLine();
    audit.assertEntry("logout", {
        reason: kExplicitLogoutMessage,
        initialUsers: [{"user": "user", "db": "test"}],
        updatedUsers: []
    });
    audit.setCurrentAuditLine(startLine);
    audit.assertNoNewEntries("logout", {reason: kImplicitLogoutMessage});
    print('SUCCESS explicit logout');

    // Auto logout and check for audit event.
    print('START implicit logout');
    // Spawn a separate mongo client to login as user and then quit the shell
    audit.fastForward();
    const uri = 'mongodb://localhost:' + port;
    const cmd = function() {
        assert(db.getSiblingDB("test").auth("user", "pwd"));
        quit();
    };

    runMongoProgram('mongo', uri, '--shell', '--eval', `(${cmd})();`);
    startLine = audit.getCurrentAuditLine();
    audit.assertEntry("logout", {
        reason: kImplicitLogoutMessage,
        initialUsers: [{"user": "user", "db": "test"}],
        updatedUsers: []
    });
    audit.setCurrentAuditLine(startLine);
    audit.assertNoNewEntries("logout", {reason: /Explicit logout from db '.+'/});
    print('SUCCESS implicit logout');
};

{
    print('START audit-logout.js for standalone');

    const m = MongoRunner.runMongodAuditLogger({auth: ''});
    runTest(m);

    MongoRunner.stopMongod(m);

    print('SUCCESS implicit logout for standalone');
}

{
    print('START audit-logout.js for Sharded Cluster');

    const st = MongoRunner.runShardedClusterAuditLogger({}, {auth: null});
    const mongos = st.s0;
    runTest(mongos);

    st.stop();
    print('SUCCESS implicit logout for standalone');
}

print("SUCCESS audit-logout.js");
})();
