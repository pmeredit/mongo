// Verify auth is sent to audit log

import "src/mongo/db/modules/enterprise/jstests/audit/lib/audit.js";

const runTest = function(audit, db, admin) {
    assert.commandWorked(
        admin.runCommand({createUser: "user1", pwd: "pwd", roles: [{role: "root", db: "admin"}]}));
    audit.fastForward();

    // Authentication does not match the audit filter
    assert(admin.auth({user: "user1", pwd: "pwd"}));
    audit.fastForward();

    const coll = db.coll;

    // "insert" command matches the audit filter
    assert.writeOK(coll.insert({_id: 0, data: "foobar"}));
    const uuid = audit
                     .assertCmd("authCheck", {
                         "command": "insert",
                         "ns": coll.getFullName(),
                         "args": {
                             "insert": coll.getName(),
                             "ordered": true,
                             "lsid": db.getSession().getSessionId(),
                             "$db": db.getName(),
                             "documents": [{"_id": 0, "data": "foobar"}]
                         }
                     })
                     .uuid;
    jsTest.log('Connection UUID: ' + tojson(uuid));

    // "find" command matches the audit filter
    assert.eq("foobar", coll.findOne({_id: 0}).data);
    audit.assertCmd("authCheck", {
        "command": "find",
        "ns": coll.getFullName(),
        "args": {
            "find": coll.getName(),
            "filter": {"_id": 0},
            "limit": 1,
            "singleBatch": true,
            "lsid": db.getSession().getSessionId(),
            "$db": db.getName()
        }
    });

    // Collection drop does not match the audit filter, so no entry is produced
    coll.drop();
    audit.assertNoNewEntriesMatching({"uuid": uuid});
    admin.logout();
};

{
    const m = MongoRunner.runMongodAuditLogger({
        auth: "",
        auditFilter: '{atype: "authCheck", "param.command": {$in: ["find", "insert"]}}',
        setParameter: {auditAuthorizationSuccess: true}
    });
    const audit = m.auditSpooler();
    const db = m.getDB("test");
    const admin = m.getDB("admin");

    audit.assertCmd = audit.assertEntry;
    runTest(audit, db, admin);
    MongoRunner.stopMongod(m);
}

{
    const st = MongoRunner.runShardedClusterAuditLogger({}, {
        auditFilter: '{atype: "authCheck", "param.command": {$in: ["find", "insert"]}}',
        setParameter: {auditAuthorizationSuccess: true},
        auth: null,
    });
    const auditMongos = st.s0.auditSpooler();
    const db = st.s0.getDB("test");
    const admin = st.s0.getDB("admin");

    // On clusters, clusterTime shows up in the param field. We don't want to worry about that
    // so we run assertEntryRelaxed instead.
    auditMongos.assertCmd = auditMongos.assertEntryRelaxed;
    runTest(auditMongos, db, admin);
    st.stop();
}
