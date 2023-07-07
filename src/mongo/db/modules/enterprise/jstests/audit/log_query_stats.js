// Verify usage of $queryStats agg stage can be sent to audit log

(function() {
'use strict';

load('src/mongo/db/modules/enterprise/jstests/audit/lib/audit.js');

const runTest = function(audit, db, admin) {
    assert.commandWorked(
        admin.runCommand({createUser: "user1", pwd: "pwd", roles: [{role: "root", db: "admin"}]}));
    audit.fastForward();

    // Authentication does not match the audit filter
    assert(admin.auth({user: "user1", pwd: "pwd"}));
    audit.assertNoNewEntries();

    // "queryStats" command with no transform identifiers matches the audit filter
    assert.commandWorked(
        db.adminCommand({aggregate: 1, pipeline: [{$queryStats: {}}], cursor: {}}));
    audit.assertCmd("authCheck", {
        "command": "aggregate",
        "ns": "admin.$cmd.aggregate",
        "args": {
            "aggregate": 1,
            "pipeline": [{"$queryStats": {}}],
            "cursor": {},
            "lsid": db.getSession().getSessionId(),
            "$db": "admin",
        }
    });

    // "queryStats" with both transform identifiers matches the audit filter
    assert.commandWorked(db.adminCommand({
        aggregate: 1,
        pipeline: [{
            $queryStats: {
                transformIdentifiers: {
                    algorithm: "hmac-sha-256",
                    hmacKey: BinData(0, "MjM0NTY3ODkxMDExMTIxMzE0MTUxNjE3MTgxOTIwMjE=")
                }
            }
        }],
        cursor: {}
    }));
    audit.assertCmd("authCheck", {
        "command": "aggregate",
        "ns": "admin.$cmd.aggregate",
        "args": {
            "aggregate": 1,
            "pipeline": [{
                "$queryStats": {
                    "transformIdentifiers": {
                        "algorithm": "hmac-sha-256",
                        // TODO SERVER-78082 ensure this value does not escape unredacted into the
                        // logs.
                        "hmacKey": BinData(0, "MjM0NTY3ODkxMDExMTIxMzE0MTUxNjE3MTgxOTIwMjE=")
                    }
                }
            }],
            "cursor": {},
            "lsid": db.getSession().getSessionId(),
            "$db": "admin",
        }
    });
    // Collection drop does not match the audit filter, so no entry is produced
    db.coll.drop();
    audit.assertNoNewEntries();
    admin.logout();
};

const auditFilterStr = "{'param.args.pipeline.0.$queryStats':{$exists:true}}";
const parameters = {
    auditAuthorizationSuccess: true,
    featureFlagQueryStats: true,
    internalQueryStatsRateLimit: -1
};

{
    const m = MongoRunner.runMongodAuditLogger(
        {auth: "", auditFilter: auditFilterStr, setParameter: parameters});
    const audit = m.auditSpooler();
    const db = m.getDB("test");
    const admin = m.getDB("admin");

    audit.assertCmd = audit.assertEntry;
    runTest(audit, db, admin);
    MongoRunner.stopMongod(m);
}

{
    const st = MongoRunner.runShardedClusterAuditLogger({}, {
        auth: null,
        auditFilter: auditFilterStr,
        setParameter: parameters,
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
})();
