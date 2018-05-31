// Verify auth is sent to audit log

(function() {
    'use strict';

    load('src/mongo/db/modules/enterprise/jstests/audit/audit.js');

    const m = MongoRunner.runMongodAuditLogger({
        auth: "",
        auditFilter: '{atype: "authCheck", "param.command": {$in: ["find", "insert"]}}',
        setParameter: "auditAuthorizationSuccess=true"
    });
    const audit = m.auditSpooler();
    const db = m.getDB("test");
    const coll = db.coll;
    const admin = m.getDB("admin");

    assert.commandWorked(
        admin.runCommand({createUser: "user1", pwd: "pwd", roles: [{role: "root", db: "admin"}]}));
    audit.fastForward();

    // Authentication does not match the audit filter
    assert(admin.auth({user: "user1", pwd: "pwd"}));
    audit.assertNoNewEntries();

    // "insert" command matches the audit filter
    assert.writeOK(coll.insert({_id: 0, data: "foobar"}));
    audit.assertEntry("authCheck", {
        "command": "insert",
        "ns": coll.getFullName(),
        "args": {
            "insert": coll.getName(),
            "ordered": true,
            "lsid": db.getSession().getSessionId(),
            "$db": db.getName(),
            "documents": [{"_id": 0, "data": "foobar"}]
        }
    });

    // "find" command matches the audit filter
    assert.eq("foobar", coll.findOne({_id: 0}).data);
    audit.assertEntry("authCheck", {
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
    audit.assertNoNewEntries();
    MongoRunner.stopMongod(m);
})();
