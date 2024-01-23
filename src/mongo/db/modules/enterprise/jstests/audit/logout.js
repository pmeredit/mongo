// Verify logout events are sent to audit log.
import "src/mongo/db/modules/enterprise/jstests/audit/lib/audit.js";

const kExplicitLogoutMessage = "Logging out on user request";
const kImplicitLogoutMessage = "Client has disconnected";

let runTest = function(conn) {
    const port = conn.port;
    const audit = conn.auditSpooler();
    const admin = conn.getDB("admin");
    const test = conn.getDB("test");

    audit.fastForward();
    // Create users on db test1 and test2 as admin.
    assert.commandWorked(admin.runCommand({createUser: "admin", pwd: "pwd", roles: ['root']}));

    assert(admin.auth("admin", "pwd"));
    assert.commandWorked(test.runCommand({createUser: "user", pwd: "pwd", roles: []}));
    assert.commandWorked(admin.logout());

    // Check that explicit admin logout was recorded in audit log with no implicit logouts.
    audit.assertNoNewEntries("logout", {reason: kImplicitLogoutMessage});
    const opts = {runHangAnalyzer: false};
    const kTimeoutForAssertEntryRelaxedCallMS = 5 * 1000;
    let params = {
        reason: kExplicitLogoutMessage,
        initialUsers: [{"user": "admin", "db": "admin"}],
        updatedUsers: []
    };
    const kTimeDiffBetweenAuthAndLogoutMS = 15 * 1000;
    const kAcceptableRangeMS = 5 * 1000;
    let entry = audit.assertEntryRelaxed('logout', params);
    assert(entry.result === 0, "Audit entry is not OK: " + tojson(entry));

    // Explicitly log out check for audit event.
    print('START explicit logout');
    assert(test.auth({user: "user", pwd: "pwd"}));
    const authLine = audit.noAdvance(() => audit.getNextEntry());
    audit.fastForward();
    // This test introduces a sleep to check the difference between the login and logout time.
    sleep(kTimeDiffBetweenAuthAndLogoutMS);
    assert(test.logout());
    const logoutLine = audit.noAdvance(() => audit.getNextEntry());

    assert(authLine.hasOwnProperty("ts") && logoutLine.hasOwnProperty("param") &&
           logoutLine["param"].hasOwnProperty("loginTime"));
    const authDate = Date.parse(authLine["ts"]["$date"]);
    const logoutDate = Date.parse(logoutLine["ts"]["$date"]);
    const loginDate = Date.parse(logoutLine["param"]["loginTime"]["$date"]);
    assert(Math.abs(loginDate - authDate) < kAcceptableRangeMS);
    assert(Math.abs(logoutDate - authDate - kTimeDiffBetweenAuthAndLogoutMS) < kAcceptableRangeMS);

    let startLine = audit.getCurrentAuditLine();
    audit.assertEntry("logout", {
        reason: kExplicitLogoutMessage,
        initialUsers: [{"user": "user", "db": "test"}],
        updatedUsers: [],
        loginTime: authLine["ts"]
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
    entry = audit.assertEntryRelaxed("logout",
                                     {
                                         reason: kImplicitLogoutMessage,
                                         initialUsers: [{"user": "user", "db": "test"}],
                                         updatedUsers: []
                                     },
                                     kTimeoutForAssertEntryRelaxedCallMS);
    assert(entry.result === 0, "Audit entry is not OK: " + tojson(entry));
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
