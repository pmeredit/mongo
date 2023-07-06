// Test that the correct AuditInterface is set

import {StandaloneFixture} from "src/mongo/db/modules/enterprise/jstests/audit/lib/audit.js";

function testOCSFAuditing(fixture) {
    const {conn, audit, admin} =
        fixture.startProcess({"auditSchema": "OCSF", "setParameter": "featureFlagOCSF=true"});

    // Currently OCSF auditing does not log anything in log file,
    // only in system logs for testing
    let auditLogs = audit.getAllLines();
    assert.eq(auditLogs, 0);

    let kLogStartupOptionsId = 7881521;
    admin.createUser({user: 'admin', pwd: 'pass', roles: jsTest.adminUserRoles});
    assert(admin.auth('admin', 'pass'));
    assert(checkLog.checkContainsOnceJson(admin, kLogStartupOptionsId, {}));

    fixture.stopProcess();
}

function testMongoAuditing(fixture) {
    const {conn, audit, admin} =
        fixture.startProcess({"auditSchema": "mongo", "setParameter": "featureFlagOCSF=true"});

    audit.assertEntry("startup");
    let auditLogs = audit.getAllLines();
    assert.neq(auditLogs, 0);

    fixture.stopProcess();
}

function testNoSchemaSpecified(fixture) {
    const {conn, audit, admin} = fixture.startProcess({"setParameter": "featureFlagOCSF=true"});

    audit.assertEntry("startup");
    let auditLogs = audit.getAllLines();
    assert.neq(auditLogs, 0);

    fixture.stopProcess();
}

{
    jsTest.log("Testing auditing when auditSchema is not specified. Should default to AuditMongo");
    const fixture = new StandaloneFixture();
    testNoSchemaSpecified(fixture);
}

{
    jsTest.log("Testing auditSchema set to 'mongo'");
    const fixture = new StandaloneFixture();
    testMongoAuditing(fixture);
}

{
    jsTest.log("Testing auditSchema set to 'OCSF'");
    const fixture = new StandaloneFixture();
    testOCSFAuditing(fixture);
}
