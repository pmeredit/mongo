// Verify an auth failure is NOT audited when an unauthed session is terminated.
import "src/mongo/db/modules/enterprise/jstests/audit/lib/audit.js";

let runTest = function(conn, audit, admin) {
    // Create a "dummy" user to turn off the localhost auth bypass.
    assert.commandWorked(admin.runCommand({createUser: "admin", pwd: "pwd", roles: ['root']}));

    // Sanity check that localhost auth bypass is off.
    assert.commandFailed(admin.runCommand({createUser: "nobody", pwd: "fail", roles: []}));

    // Explicitly close a session without auth.
    // This will fail as unauthorized, but we don't need to see a audit for it.
    audit.fastForward();
    const session = conn.startSession();
    assert.commandFailedWithCode(admin.runCommand({endSessions: [session.getSessionId()]}),
                                 ErrorCodes.Unauthorized);
    audit.assertNoNewEntries("authCheck");
    admin.auth("admin", "pwd");
};

{
    print("START orphan-session.js for standalone");

    const m = MongoRunner.runMongodAuditLogger({auth: ''});
    const audit = m.auditSpooler();
    const admin = m.getDB("admin");

    runTest(m, audit, admin);

    MongoRunner.stopMongod(m);
    print("SUCCESS orphan-session.js for standalone");
}

{
    print("START orphan-session.js for sharded cluster");

    const st = MongoRunner.runShardedClusterAuditLogger({auth: null, keyFile: "jstests/libs/key1"});
    const mongos = st.s0;
    const audit = mongos.auditSpooler();
    const admin = mongos.getDB("admin");

    runTest(mongos, audit, admin);

    st.stop();
    print("SUCCESS orphan-session.js for sharded cluster");
}
