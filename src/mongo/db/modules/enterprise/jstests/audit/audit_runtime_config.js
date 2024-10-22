// Basic tests for enabling runtime audit configuration.
// @tags: [requires_fcv_49]
import "src/mongo/db/modules/enterprise/jstests/audit/lib/audit.js";

function assertStartupFails(opts, expect) {
    clearRawMongoProgramOutput();
    // Default in an auditDestination just in case one isn't specified.
    const options = Object.assign({auditDestination: 'console'}, opts);
    assert.throws(() => MongoRunner.runMongod(options),
                  [],
                  "Mongod started when it should not have with opts: " + tojson(opts));
    const output = rawMongoProgramOutput(".*");
    assert(output.includes(expect),
           "Could not find expected reason for mongod failing to start: " + expect);
}

function assertStartupSucceeds(opts) {
    const options = Object.assign({auditDestination: 'console'}, opts);
    const m = MongoRunner.runMongod(options);
    assert(m, "Mongod failed to start when it should have with opts: " + tojson(opts));
    MongoRunner.stopMongod(m);
}

// Default condition expressed explicitly.
assertStartupSucceeds({auditRuntimeConfiguration: false});

// Basic test enabling runtime audit config.
assertStartupSucceeds({auditRuntimeConfiguration: true});

// Fail if we also specify filter/auditAuthorizationSuccess (even if they're empty/false).
const mustNot = ' must not be configured when runtime audit configuration is enabled';
assertStartupFails({auditRuntimeConfiguration: true, auditFilter: '{}'},
                   'auditLog.filter' + mustNot);
assertStartupFails(
    {auditRuntimeConfiguration: true, setParameter: 'auditAuthorizationSuccess=true'},
    'auditAuthorizationSuccess' + mustNot);

// Startup and attempt to modify auditAuthorizationSuccess at runtime.
{
    const m = MongoRunner.runMongod({auditRuntimeConfiguration: true, auditDestination: 'console'});
    assert(m, "Mongod failed to start");
    const admin = m.getDB('admin');
    const getSP =
        assert.commandWorked(admin.runCommand({getParameter: 1, auditAuthorizationSuccess: 1}));
    assert(getSP.auditAuthorizationSuccess === false, tojson(getSP));
    const reply =
        assert.commandFailed(admin.runCommand({setParameter: 1, auditAuthorizationSuccess: true}));
    const expect =
        'auditAuthorizationSuccess may not be changed via set parameter when runtime audit configuration is enabled';
    assert(reply.errmsg.includes(expect),
           'setParameter failed for the wrong reason: ' + tojson(reply));
    MongoRunner.stopMongod(m);
}
