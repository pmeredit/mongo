/*
 * Tests that LDAP health checker is crashing mongos if
 * enabled and set to critical and the LDAP server is inaccessible.
 */

import {ShardingTest} from "jstests/libs/shardingtest.js";
import {
    MockLDAPServer
} from "src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldap_mock_server_utils.js";

const mockServerHandle = new MockLDAPServer(
    'src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldap_mock_server_dit.ldif');
const mockServer = mockServerHandle.getHostAndPort();
mockServerHandle.start();
const ACTIVE_FAULT_DURATION_SECS = 1;

// Crashed mongos will remain holding its socket as a zombie for some time.
TestData.ignoreUnterminatedProcesses = true;
TestData.ignoreChildProcessErrorCode = true;
// Because this test intentionally crashes the server, we instruct the
// the shell to clean up after us and remove the core dump.
TestData.cleanUpCoreDumpsFromExpectedCrash = true;

let st = new ShardingTest({
    shards: 1,
    config: 1,
    mongos: [
        {
            setParameter: {
                healthMonitoringIntensities: tojson({
                    values: [
                        {type: "ldap", intensity: "critical"},
                    ]
                }),
                progressMonitor: tojson({interval: 100, deadline: 60}),
                healthMonitoringIntervals: tojson({values: [{type: "ldap", interval: 2000}]}),
            },
            ldapServers: mockServer,
            ldapTransportSecurity: "none",
            ldapBindMethod: "simple",
            ldapQueryUser: "cn=ldapz_admin,ou=Users,dc=10gen,dc=cc",
            ldapQueryPassword: "Secret123",
            ldapTimeoutMS: 5000,
        },
        {}
    ],
});

assert.commandWorked(st.s0.adminCommand(
    {"setParameter": 1, logComponentVerbosity: {processHealth: {verbosity: 3}}}));

assert.commandWorked(
    st.s0.adminCommand({"setParameter": 1, activeFaultDurationSecs: ACTIVE_FAULT_DURATION_SECS}));

const faultState = function() {
    let result =
        assert.commandWorked(st.s0.adminCommand({serverStatus: 1, health: {details: true}})).health;
    print(`Server status: ${tojson(result)}`);
    return result.state;
};

jsTestLog('Ensure mongos is up');
assert.commandWorked(st.s0.adminCommand({"ping": 1}));

assert(faultState() == 'Ok');

// Turn off the LDAP server to simulate an outage.
mockServerHandle.stop();

// Wait for non-ok LDAP status or network error.
assert.soon(() => {
    try {
        return faultState() == 'TransientFault' || faultState() == 'ActiveFault';
    } catch (e) {
        jsTestLog(`Can't fetch server status: ${e}`);
        return true;  // Server must be down already.
    }
}, 'Cannot reach mongos', 20000, 1000);

// Waits until the mongos crashes.
assert.soon(() => {
    try {
        let res = st.s0.adminCommand({"ping": 1});
        jsTestLog(`Ping result: ${tojson(res)}`);
        return res.ok != 1;
    } catch (e) {
        jsTestLog(`Ping failed: ${tojson(e)}`);
        return true;
    }
}, 'Mongos is not shutting down as expected', 30000, 400);

try {
    st.stop({skipValidatingExitCode: true, skipValidation: true});
} catch (e) {
    jsTestLog(`Exception during shutdown: ${e}`);
}
