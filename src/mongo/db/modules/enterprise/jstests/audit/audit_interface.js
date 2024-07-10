// Test that the correct AuditInterface is set

import {StandaloneFixture} from "src/mongo/db/modules/enterprise/jstests/audit/lib/audit.js";

function testOCSFAuditing(fixture) {
    const {conn, audit, admin} = fixture.startProcess({"auditSchema": "OCSF"});

    // Currently OCSF auditing does not log anything in log file,
    // only in system logs for testing
    let auditLogs = audit.getAllLines();
    assert.neq(auditLogs, 0);

    fixture.stopProcess();
}

function testMongoAuditing(fixture) {
    const {conn, audit, admin} = fixture.startProcess({"auditSchema": "mongo"});

    audit.assertEntry("startup");
    let auditLogs = audit.getAllLines();
    assert.neq(auditLogs, 0);

    fixture.stopProcess();
}

function testNoSchemaSpecified(fixture) {
    const {conn, audit, admin} = fixture.startProcess({});

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

sleep(2000);

{
    jsTest.log("Testing auditSchema set to 'mongo'");
    const fixture = new StandaloneFixture();
    testMongoAuditing(fixture);
}

sleep(2000);

{
    jsTest.log("Testing auditSchema set to 'OCSF'");
    const fixture = new StandaloneFixture();
    testOCSFAuditing(fixture);
}
