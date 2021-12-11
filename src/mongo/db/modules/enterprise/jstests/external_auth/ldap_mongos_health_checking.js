/**
 * Integration test for LDAP health checker in Mongos.
 */

(function() {
'use strict';

load("src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldap_authz_lib.js");

// Firewall actions require Linux.
const isLinux = getBuildInfo().buildEnvironment.target_os == "linux";
const isAnyUbuntu = (() => {
    if (!isLinux) {
        return false;
    }

    const result = runProgram('cat', '/etc/issue');
    if (result == 0) {
        return false;
    }

    return result.match(/Ubuntu/i);
})();

const ldapServers =
    "ec2-3-142-199-96.us-east-2.compute.amazonaws.com,ec2-3-141-40-15.us-east-2.compute.amazonaws.com";
// const ldapServers = "ldaptest.10gen.cc";
const ldapServersArray = ldapServers.split(',');

var st = new ShardingTest({
    shards: 1,
    mongos: 1,
    config: 1,
    other: {
        mongosOptions: {
            "ldapServers": ldapServers,
            "ldapTransportSecurity": "none",
            "ldapBindMethod": "simple",
            //"ldapQueryUser": "cn=ldapz_admin,ou=Users,dc=10gen,dc=cc",
            "ldapQueryUser": "cn=admin,dc=10gen,dc=cc",
            "ldapQueryPassword": "Secret123",
            "ldapTimeoutMS": 8000,
            "setParameter": "featureFlagHealthMonitoring=true",
        },
    },
});

assert.commandWorked(st.s0.adminCommand(
    {"setParameter": 1, logComponentVerbosity: {processHealth: {verbosity: 3}}}));

// Invokes the 'ufw' firewall utility with 'args'.
const firewallAction = function(args, allowedToFail = false) {
    clearRawMongoProgramOutput();
    const shellArgs = ['sudo', 'ufw'].concat(args);
    jsTestLog(`${shellArgs}`);
    const rc = _runMongoProgram.apply(null, shellArgs);
    if (!allowedToFail) {
        assert.eq(rc, 0);
    }
    return rawMongoProgramOutput();
};

// If 'removeRule' is true remove the blocking rule back.
const changeFirewallForServer = function(server, removeRule = false) {
    const removeRuleArg = removeRule ? ['delete'] : [];
    const ip = resolve(server);
    jsTestLog(`Change firewall rule for ${server} resolved to ${ip} with ${removeRule}`);
    firewallAction(removeRuleArg.concat(['deny', 'out', 'to', ip]));
};

const resolve = function(host) {
    clearRawMongoProgramOutput();
    runMongoProgram('dig', '+short', host);
    const out = rawMongoProgramOutput();
    jsTestLog(out);
    const matchIp = out.match(
        /\b(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)/i);
    jsTestLog(matchIp);
    return matchIp[0];
};

// If there is more than one server, disable all but one server with firewall.
// If there is only one server skip this test.
const testWithPartiallyDisabledFirewall = function() {
    if (ldapServersArray.length <= 1 || !isAnyUbuntu) {
        return;
    }
    const indexToNotBlock = Math.floor(Math.random() * ldapServersArray.length);
    const serversToBlock = ldapServersArray.slice(0, indexToNotBlock)
                               .concat(ldapServersArray.slice(indexToNotBlock + 1));
    jsTestLog(serversToBlock);

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

const checkFirewallIsEnabled = function() {
    const out = firewallAction(['status']);
    const active = !/inactive/.test(out);
    jsTestLog(`Firewall is active: ${active}`);
    return active;
};

const enableFirewall = function() {
    firewallAction(['enable']);
};

const disableFirewall = function() {
    jsTestLog('Disable firewall');
    firewallAction(['disable'], true /* this can fail */);
};

var firewallIsEnabledAtStart = false;
try {
    if (isAnyUbuntu) {
        firewallIsEnabledAtStart = checkFirewallIsEnabled();
        if (!firewallIsEnabledAtStart) {
            enableFirewall();
        }
        testWithPartiallyDisabledFirewall();
    }

} finally {
    if (isAnyUbuntu && !firewallIsEnabledAtStart) {
        disableFirewall();
    }
}

sleep(8000);  // Let all health checker threads stuck because of firewall to terminate.
assert.commandWorked(st.s0.adminCommand({"ping": 1}));
try {
    jsTestLog('Shutting down the sharded cluster');
    st.stop();
} catch (err) {
    jsTestLog(`Error during shutdown ${err}`);
}
})();
