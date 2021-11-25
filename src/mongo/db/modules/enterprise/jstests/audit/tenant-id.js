// Verify Tenant ID is included in audit messages.
// @tags: [requires_replication, requires_sharding]

(function() {
'use strict';

const isMongoStoreEnabled = TestData.setParameters.featureFlagMongoStore;
if (!isMongoStoreEnabled) {
    assert.throws(() => MongoRunner.runMongod({
        setParameter: "multitenancySupport=true",
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

    // Make a tenant user.
    const tenantID = ObjectId();
    assert.commandWorked(
        conn.adminCommand({createUser: 'user1', '$tenant': tenantID, pwd: 'pwd', roles: ['root']}));

    // Set security token on connection.
    const tennantConn = new Mongo(conn.host);
    tennantConn._setSecurityToken(
        _createSecurityToken({user: 'user1', db: 'admin', tenant: tenantID}));

    // Generate audit event.
    assert.commandWorked(tennantConn.adminCommand({logApplicationMessage: 'Hello World'}));

    // Look for audited entry with tenant ID.
    const entry = audit.assertEntry('applicationMessage', {msg: 'Hello World'});
    assert(entry.tenant !== undefined, 'No tenant entry found: ' + tojson(entry));
    assert(entry.tenant['$oid'] !== undefined,
           'Tenant does not seem to be an OID: ' + tojson(entry));
    assert.eq(entry.tenant['$oid'], tenantID.str, 'Mismatched tenant ID');
}

const opts = {
    setParameter: 'multitenancySupport=true',
};

function runMongodTest(asBSON) {
    const m = MongoRunner.runMongodAuditLogger(opts, asBSON);
    const audit = m.auditSpooler();

    test(audit, m, asBSON);
    MongoRunner.stopMongod(m);
}

jsTest.log('START audit tenant-id.js ');
runMongodTest(true);
runMongodTest(false);
})();
