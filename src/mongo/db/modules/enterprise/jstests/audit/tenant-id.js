// Verify Tenant ID is included in audit messages.
// @tags: [requires_replication, requires_sharding]

(function() {
'use strict';

const isMongoStoreEnabled = TestData.setParameters.featureFlagMongoStore;
if (!isMongoStoreEnabled) {
    assert.throws(() => MongoRunner.runMongod({
        setParameter: "supportMultitenancy=true",
    }));
    return;
}

load('src/mongo/db/modules/enterprise/jstests/audit/lib/audit.js');

function test(audit, conn, asBSON) {
    jsTest.log('asBSON: ' + tojson(asBSON));

    // Verify that no lines even have a tenant ID yet.
    const preamble = audit.getAllLines().map((l) => JSON.parse);
    preamble.forEach(function(a) {
        assert(a.tenant === undefined);
    });
    audit.setCurrentAuditLine(preamble.length);

    // Set security token on connection.
    const tenantID = ObjectId();
    conn._setSecurityToken({tenant: tenantID});

    const admin = conn.getDB('admin');
    assert.commandWorked(admin.runCommand({logApplicationMessage: 'Hello World'}));
    const entry = audit.assertEntry('applicationMessage', {msg: 'Hello World'});
    assert(entry.tenant !== undefined, 'No tenant entry found: ' + tojson(entry));
    assert(entry.tenant['$oid'] !== undefined,
           'Tenant does not seem to be an OID: ' + tojson(entry));
    assert.eq(entry.tenant['$oid'], tenantID.str, 'Mismatched tenant ID');
}

const opts = {
    setParameter: 'supportMultitenancy=true',
};

function runMongodTest(asBSON) {
    const m = MongoRunner.runMongodAuditLogger(opts, asBSON);
    const audit = m.auditSpooler();

    test(audit, m, asBSON);
    MongoRunner.stopMongod(m);
}

function runShardedTest(asBSON) {
    const st = MongoRunner.runShardedClusterAuditLogger({}, opts);
    const auditMongos = st.s0.auditSpooler();

    test(auditMongos, st.s0, asBSON);
    st.stop();
}

jsTest.log('START audit tenant-id.js ');
runMongodTest(true);
runMongodTest(false);

jsTest.log('Sharding');
runShardedTest(true);
runShardedTest(false);
jsTest.log('SUCCESS audit tenant-id.js');
})();
