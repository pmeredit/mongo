/**
 * Integration test for LDAP health checker in Mongos.
 * Responsible for checking that mongos remains operational even
 * when the LDAP health checker detects LDAP downtime.
 * Enforces some loose bounds around the number of successful and failed
 * LDAP health checks when the LDAP server is flaky.
 */

import {ShardingTest} from "jstests/libs/shardingtest.js";
import {
    MockLDAPServer
} from "src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldap_mock_server_utils.js";

// Increment this for the real stress test, value is low for the Evergreen.
const kIterations = 2;
const kWaitForCompletedChecksCount = 50;
const kWaitForPassedChecksCount = 10;

const kMonitoringIntervalMs = 200;

// Setting LDAP interval to be pretty short allowing a low probability failure.
const kLdapTimeout = 5000;

const kProgressMonitorInterval = 100;

// The timeout should be enough to repeat the LDAP check once more after timeout.
const kProgressMonitorDeadlineSec = (kLdapTimeout / 1000) * 2 + 10;

// Let all blocked threads to timeout before terminating.
const kAfterAllTestsSleep = kLdapTimeout + 100;

const ldapTestServers = function() {
    // Start 3 mock LDAP servers.
    const mockServerHandles = [
        new MockLDAPServer(
            'src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldap_mock_server_dit.ldif'),
        new MockLDAPServer(
            'src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldap_mock_server_dit.ldif'),
        new MockLDAPServer(
            'src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldap_mock_server_dit.ldif')
    ];
    const mockServers =
        mockServerHandles.map((mockServerHandle) => { return mockServerHandle.getHostAndPort(); })
            .join();
    mockServerHandles.forEach((mockServerHandle) => { mockServerHandle.start(); });
    return {
        "OpenLDAP": {
            "ldapServers": mockServers,
            "ldapServerHandles": mockServerHandles,
            "ldapTransportSecurity": "none",
            "ldapBindMethod": "simple",
            "ldapQueryUser": "cn=ldapz_admin,ou=Users,dc=10gen,dc=cc",
            "ldapQueryPassword": "Secret123",
            "ldapAuthzQueryTemplate": "{USER}?memberOf",
            "ldapTimeoutMS": kLdapTimeout,
        },
        // TODO SERVER-73227: The linked ticket tracks what to do for a long term solution for
        // active directory integration testing. The original server was removed from aws causing
        // this test to fail.
    };
}();

const runTestSuite = function(ldapTestServer) {
    const ldapServerHandles = ldapTestServer["ldapServerHandles"];

    const st = new ShardingTest({
        shards: 1,
        config: 1,
        mongos: [{
            setParameter: {
                healthMonitoringIntensities: tojson({
                    values: [
                        {type: "ldap", intensity: "critical"},
                    ]
                }),
                progressMonitor: tojson(
                    {interval: kProgressMonitorInterval, deadline: kProgressMonitorDeadlineSec}),
                healthMonitoringIntervals:
                    tojson({values: [{type: "ldap", interval: kMonitoringIntervalMs}]}),
            },
            ldapServers: ldapTestServer["ldapServers"],
            ldapTransportSecurity: "none",
            ldapBindMethod: "simple",
            ldapQueryUser: ldapTestServer["ldapQueryUser"],
            ldapQueryPassword: ldapTestServer["ldapQueryPassword"],
            ldapTimeoutMS: 8000,
        }],
    });

    assert.commandWorked(st.s0.adminCommand(
        {"setParameter": 1, logComponentVerbosity: {processHealth: {verbosity: 3}}}));

    const mongosProcessId = (() => {
        clearRawMongoProgramOutput();
        const shellArgs = ['ps', '-e'];
        const rc = _runMongoProgram.apply(null, shellArgs);
        assert.eq(rc, 0);
        const lines = rawMongoProgramOutput(".*");
        let found;
        lines.split('\n').forEach((line) => {
            const match = line.match(/[' ']+([0-9]+).*mongos.*/i);
            if (match) {
                found = match[1];
            }
        });
        return found;
    })();

    // If there is more than one LDAP server, stop all but one LDAP server.
    // If there is only one server skip this test.
    const testWithOneResponsiveLDAPServer = function() {
        if (ldapServerHandles.length <= 1) {
            return;
        }
        const indexToNotBlock = Math.floor(Math.random() * ldapServerHandles.length);
        const serversToBlock = ldapServerHandles.slice(0, indexToNotBlock)
                                   .concat(ldapServerHandles.slice(indexToNotBlock + 1));

        serversToBlock.forEach((blockedServerHandle) => { blockedServerHandle.stop(); });
        sleep(10000);  // Let mongos to run with blocked LDAP server.
        // The timeout for each request is 8 sec. In each health check, one thread
        // will succeed and one timeout before the LDAP server is stopped.

        // Mongos should be functional.
        assert.commandWorked(st.s0.adminCommand({"ping": 1}));

        serversToBlock.forEach((blockedServerHandle) => { blockedServerHandle.start(); });
    };

    const testWithNoResponsiveLDAPServers = function() {
        ldapServerHandles.forEach((serverHandle) => { serverHandle.stop(); });
        sleep(10000);  // Let mongos run with unresponsive LDAP servers.

        ldapServerHandles.forEach((serverHandle) => { serverHandle.start(); });

        // Mongos should be functional.
        assert.commandWorked(st.s0.adminCommand({"ping": 1}));
    };

    const printMemoryForProcess = function(processId) {
        clearRawMongoProgramOutput();
        // /smaps could be unavailable, first print the /status
        const statusShellArgs = ['ls', '/proc/' + processId + '/status'];
        let rc = _runMongoProgram.apply(null, statusShellArgs);
        if (rc != 0) {
            jsTestLog(`Process status unavailable for pid ${processId}`);
            // List /proc for debug.
            const shellArgs = ['ls', '/proc'];
            _runMongoProgram.apply(null, shellArgs);
            return;
        }
        clearRawMongoProgramOutput();
        const shellArgs = ['cat', '/proc/' + processId + '/smaps'];
        rc = _runMongoProgram.apply(null, shellArgs);
        if (rc != 0) {
            jsTestLog(`/proc/${processId}/smaps not available`);
            return;
        }
        const lines = rawMongoProgramOutput(".*");
        let totalKb = 0;
        lines.split('\n').forEach((line) => {
            const match = line.match(/[' ']+Pss:[' ']+([0-9]+).*kB.*/i);
            if (match) {
                totalKb += parseInt(match[1]);
            }
        });
        jsTestLog(`Total memory usage ${totalKb} kB`);
    };

    const printFdCount = function(processId) {
        clearRawMongoProgramOutput();
        const shellArgs = ['ls', '/proc/' + processId + '/fd/'];
        const rc = _runMongoProgram.apply(null, shellArgs);
        if (rc != 0) {
            jsTestLog(`/proc/${processId}/fd/ not available`);
            return;
        }
        const lines = rawMongoProgramOutput(".*");
        const totalFds = lines.split('\n').length;
        jsTestLog(`Total files open ${totalFds}`);
    };

    // Expects some minimal check count to pass. This expects the test to be enabled
    // on Enterprise builds only.
    const checkServerStats = function() {
        while (true) {
            const result =
                assert.commandWorked(st.s0.adminCommand({serverStatus: 1, health: {details: true}}))
                    .health;
            print(`Server status: ${tojson(result)}`);
            // Wait for: at least 100 checks completed.
            // At least some checks passed (more than 1).
            if (result.LDAP.totalChecks >= kWaitForCompletedChecksCount &&
                result.LDAP.totalChecks - result.LDAP.totalChecksWithFailure >=
                    kWaitForPassedChecksCount) {
                break;
            }
            sleep(1000);
        }
    };
    try {
        for (let i = 0; i < kIterations; ++i) {
            testWithOneResponsiveLDAPServer();
            if (i % 10 == 0) {
                printMemoryForProcess(mongosProcessId);
                printFdCount(mongosProcessId);
            }
        }
        for (let i = 0; i < kIterations; ++i) {
            testWithNoResponsiveLDAPServers();
            if (i % 10 == 0) {
                printMemoryForProcess(mongosProcessId);
                printFdCount(mongosProcessId);
            }
        }
    } finally {
        // If we hit an assertion above, make sure the LDAP server is restarted.
        ldapServerHandles.forEach((serverHandle) => {
            // If LDAP server is not running, start it back up.
            if (!serverHandle.isRunning()) {
                serverHandle.start();
            }
        });
    }

    sleep(kAfterAllTestsSleep);  // Let all health checker threads stuck because of blocked LDAP
                                 // server to terminate.
    assert.commandWorked(st.s0.adminCommand({"ping": 1}));
    printMemoryForProcess(mongosProcessId);
    printFdCount(mongosProcessId);
    checkServerStats();

    try {
        jsTestLog('Shutting down the sharded cluster and all LDAP servers used by it.');
        st.stop();
        ldapServerHandles.forEach((serverHandle) => { serverHandle.stop(); });
    } catch (err) {
        jsTestLog(`Error during shutdown ${err}`);
    }
};

Object.keys(ldapTestServers).forEach((serverName) => {
    jsTestLog(`Running tests for server ${serverName}`);
    runTestSuite(ldapTestServers[serverName]);
});
