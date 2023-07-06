/*
 * Tests that LDAP health checker is crashing Mongos.
 *
 * Test requires Ubuntu firewall:
 *  @tags: [
 *    incompatible_with_amazon_linux,
 *    incompatible_with_windows_tls,
 *    incompatible_with_macos
 *  ]
 */

import {
    disableFirewallFromServer,
    enableFirewallFromServer,
    isAnyUbuntu,
    isFirewallEnabledFromServer,
} from "src/mongo/db/modules/enterprise/jstests/external_auth/lib/iptables_lib.js";

const ldapServer = "ldaptest.10gen.cc";
const ACTIVE_FAULT_DURATION_SECS = 1;

// Crashed mongos will remain holding its socket as a zombie for some time.
TestData.failIfUnterminatedProcesses = false;

if (!isAnyUbuntu) {
    jsTestLog('Test requires Ubuntu for firewall actions');
    quit();
}

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
            ldapServers: ldapServer,
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

try {
    // TODO: SERVER-64479.  Fix firewall concurrency issues.
    // Turn the firewall on (drop packets from ldapServer).
    enableFirewallFromServer(ldapServer);

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
} finally {
    // In case assertion is hit, make sure firewall is down
    while (isFirewallEnabledFromServer(ldapServer)) {
        disableFirewallFromServer(ldapServer);
    }
}

try {
    st.stop({skipValidatingExitCode: true, skipValidation: true});
} catch (e) {
    jsTestLog(`Exception during shutdown: ${e}`);
}
