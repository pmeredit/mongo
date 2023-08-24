// Verify Tenant ID is included in audit messages.
// @tags: [requires_replication, requires_sharding, featureFlagSecurityToken]

import "src/mongo/db/modules/enterprise/jstests/audit/lib/audit.js";
const kVTSKey = 'secret';

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
    conn._setSecurityToken(_createTenantToken(tenantID));
    assert.commandWorked(conn.adminCommand({createUser: 'user1', pwd: 'pwd', roles: ['root']}));
    conn._setSecurityToken(undefined);

    // Set security token on connection.
    const tennantConn = new Mongo(conn.host);
    tennantConn._setSecurityToken(
        _createSecurityToken({user: 'user1', db: 'admin', tenant: tenantID}, kVTSKey));

    // Generate audit event.
    const result = tennantConn.adminCommand({logApplicationMessage: 'Hello World'});
    if (!result.ok) {
        // Disabled test pending PM-2608 and ability to create custom roles
        // containg applicationMessage action type.
        return;
    }

    // Look for audited entry with tenant ID.
    const entry = audit.assertEntry('applicationMessage', {msg: 'Hello World'});
    assert(entry.tenant !== undefined, 'No tenant entry found: ' + tojson(entry));
    assert(entry.tenant['$oid'] !== undefined,
           'Tenant does not seem to be an OID: ' + tojson(entry));
    assert.eq(entry.tenant['$oid'], tenantID.str, 'Mismatched tenant ID');
}

const opts = {
    setParameter: {
        multitenancySupport: true,
        testOnlyValidatedTenancyScopeKey: kVTSKey,
    },
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
