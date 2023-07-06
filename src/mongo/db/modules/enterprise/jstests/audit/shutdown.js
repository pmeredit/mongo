// Verify shutdown is sent to audit log
// @tags: [live_record_incompatible]

import "src/mongo/db/modules/enterprise/jstests/audit/lib/audit.js";

{
    print("START audit-shutdown.js standalone");

    sleep(2000);

    const m = MongoRunner.runMongodAuditLogger({});
    const audit = m.auditSpooler();
    const admin = m.getDB("admin");

    // We actually expect shutdownServer() to return undefined
    // since the connection closes immediately.
    // So interpret an undefined result as OK.
    // The audit entry ensures it was processed correctly.
    assert(admin.shutdownServer({timeoutSecs: 3}) === undefined);
    audit.assertEntry("shutdown", {});
    waitProgram(m.pid);

    print("SUCCESS audit-shutdown.js standalone");
}

{
    print("START audit-shutdown.js sharded");

    sleep(2000);
    const st = MongoRunner.runShardedClusterAuditLogger({});
    const auditMongos = st.s0.auditSpooler();

    st.stopMongos(0, st.s0.opts);
    auditMongos.assertEntry("shutdown", {});

    sleep(2000);
    st.restartMongos(0, st.s0.opts);

    st.stop();

    print("SUCCESS audit-shutdown.js sharded");
}
