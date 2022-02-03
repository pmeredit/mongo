/**
 * Integration test for LDAP health checker in Mongos.
 */

(function() {
'use strict';

load("src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldap_authz_lib.js");
load("src/mongo/db/modules/enterprise/jstests/external_auth/lib/ufw_firewall_lib.js");

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
    return {
        "OpenLDAP": {
            "ldapServers": baseLDAPUrls[0],
            "ldapTransportSecurity": "none",
            "ldapBindMethod": "simple",
            "ldapQueryUser": "cn=ldapz_admin,ou=Users,dc=10gen,dc=cc",
            "ldapQueryPassword": "Secret123",
            "ldapAuthzQueryTemplate": "{USER}?memberOf",
            "ldapTimeoutMS": kLdapTimeout,
        },
        "ActiveDirectory": {
            // Elastic IP for the ec2-18-216-194-49.us-east-2.compute.amazonaws.com host.
            "ldapServers": "3.129.54.246",
            "ldapTransportSecurity": "none",
            "ldapBindMethod": "simple",
            "ldapQueryUser": "cn=ldapz_admin,cn=Users,dc=mongotest,dc=com",
            "ldapQueryPassword": "Secret123",
            "ldapAuthzQueryTemplate": "{USER}?memberOf",
            "ldapTimeoutMS": kLdapTimeout,
        }
    };
}();

const runTestSuite = function(ldapTestServer) {
    const ldapServersArray = ldapTestServer["ldapServers"].split(',');

    var st = new ShardingTest({
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
        var lines = rawMongoProgramOutput();
        var found;
        lines.split('\n').forEach((line) => {
            const match = line.match(/[' ']+([0-9]+).*mongos.*/i);
            if (match) {
                found = match[1];
            }
        });
        return found;
    })();

    // If there is more than one server, disable all but one server with firewall.
    // If there is only one server skip this test.
    const testWithPartiallyDisabledFirewall = function() {
        if (ldapServersArray.length <= 1 || !isAnyUbuntu) {
            return;
        }
        const indexToNotBlock = Math.floor(Math.random() * ldapServersArray.length);
        const serversToBlock = ldapServersArray.slice(0, indexToNotBlock)
                                   .concat(ldapServersArray.slice(indexToNotBlock + 1));

        serversToBlock.forEach((serverName) => {
            changeFirewallForServer(serverName);
        });
        sleep(10000);  // Let mongos to run with firewall.
        // The timeout for each request is 8 sec. In each health check, one thread
        // will succeed and one timeout before the firewall is enabled.

        // Mongos should be functional.
        assert.commandWorked(st.s0.adminCommand({"ping": 1}));

        serversToBlock.forEach((serverName) => {
            changeFirewallForServer(serverName, true /* remove rule */);
        });
    };

    const testWithFullyDisabledFirewall = function() {
        ldapServersArray.forEach((serverName) => {
            changeFirewallForServer(serverName);
        });
        sleep(Math.random() * 10000);  // Let mongos to run with firewall.

        ldapServersArray.forEach((serverName) => {
            changeFirewallForServer(serverName, true /* remove rule */);
        });

        // Mongos should be functional.
        assert.commandWorked(st.s0.adminCommand({"ping": 1}));
    };

    const printMemoryForProcess = function(processId) {
        clearRawMongoProgramOutput();
        // /smaps could be unavailable, first print the /status
        const statusShellArgs = ['ls', '/proc/' + processId + '/status'];
        var rc = _runMongoProgram.apply(null, statusShellArgs);
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
        var lines = rawMongoProgramOutput();
        var totalKb = 0;
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
        var lines = rawMongoProgramOutput();
        var totalFds = lines.split('\n').length;
        jsTestLog(`Total files open ${totalFds}`);
    };

    // Expects some minimal check count to pass. This expects the test to be enabled
    // on Enterprise builds only.
    const checkServerStats = function() {
        while (true) {
            let result =
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

    var firewallIsEnabledAtStart = false;
    try {
        if (isAnyUbuntu) {
            firewallIsEnabledAtStart = checkFirewallIsEnabled();
            if (!firewallIsEnabledAtStart) {
                enableFirewall();
            }
            for (var i = 0; i < kIterations; ++i) {
                testWithPartiallyDisabledFirewall();
                if (i % 10 == 0) {
                    printMemoryForProcess(mongosProcessId);
                    printFdCount(mongosProcessId);
                }
            }
            for (var i = 0; i < kIterations; ++i) {
                testWithFullyDisabledFirewall();
                if (i % 10 == 0) {
                    printMemoryForProcess(mongosProcessId);
                    printFdCount(mongosProcessId);
                }
            }
        }

    } finally {
        if (isAnyUbuntu && !firewallIsEnabledAtStart) {
            disableFirewall();
        }
    }

    sleep(kAfterAllTestsSleep);  // Let all health checker threads stuck because of firewall to
                                 // terminate.
    assert.commandWorked(st.s0.adminCommand({"ping": 1}));
    if (isAnyUbuntu) {
        printMemoryForProcess(mongosProcessId);
        printFdCount(mongosProcessId);
    }
    checkServerStats();

    try {
        jsTestLog('Shutting down the sharded cluster');
        st.stop();
    } catch (err) {
        jsTestLog(`Error during shutdown ${err}`);
    }
};

Object.keys(ldapTestServers).forEach((serverName) => {
    jsTestLog(`Running tests for server ${serverName}`);
    runTestSuite(ldapTestServers[serverName]);
});
})();
