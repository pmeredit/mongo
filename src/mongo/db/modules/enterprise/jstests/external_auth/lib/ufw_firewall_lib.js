"use strict";

// Firewall actions require Linux.
const isLinux = getBuildInfo().buildEnvironment.target_os == "linux";
const isAnyUbuntu = (() => {
    if (!isLinux) {
        return false;
    }

    const rc = runProgram('cat', '/etc/issue');
    if (rc != 0) {
        jsTestLog(`Unexpected failure fetching /etc/issue ${rc}`);
        return false;
    }
    var osRelease = rawMongoProgramOutput();
    clearRawMongoProgramOutput();

    return osRelease.match(/Ubuntu/i);
})();

// Invokes the 'ufw' firewall utility with 'args'.
const firewallAction = function(args, allowedToFail = false, fetchOutput = true) {
    clearRawMongoProgramOutput();
    const shellArgs = ['sudo', 'ufw'].concat(args);
    jsTestLog(`${shellArgs}`);
    const rc = runNonMongoProgram.apply(null, shellArgs);
    if (!allowedToFail) {
        assert.eq(rc, 0);
    }
    if (fetchOutput) {
        const mongoOutput = rawMongoProgramOutput();
        jsTestLog(`${mongoOutput}, rc ${rc}`);
        return mongoOutput;
    } else {
        jsTestLog(`rc ${rc}`);
    }
};

// Resolves server name to IP address using dig, works only on Unix platforms.
const resolveIPUnix = function(host) {
    clearRawMongoProgramOutput();
    var result;
    assert.soon(() => {
        runNonMongoProgram('dig', '+short', host);
        const out = rawMongoProgramOutput();
        jsTestLog(out);
        const matchIp = out.match(
            /\b(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)/i);
        jsTestLog(matchIp);
        if (!matchIp) {
            return false;
        }
        result = matchIp[0];
        return true;
    }, 'Cannot resolve using dig', 20000, 1000);
    return result;
};

// Sets or removes the firewall block to the provided server.
// If 'removeRule' is true remove the blocking rule back.
const changeFirewallForServer = function(server, removeRule = false) {
    const removeRuleArg = removeRule ? ['delete'] : [];
    const ip = resolveIPUnix(server);
    jsTestLog(`Change firewall rule for ${server} resolved to ${ip} with ${removeRule}`);
    firewallAction(removeRuleArg.concat(['deny', 'out', 'to', ip]));
};

const checkFirewallIsEnabled = function() {
    const out = firewallAction(['status']);
    const active = !/inactive/.test(out);
    jsTestLog(`Firewall is active: ${active}`);
    return active;
};

const enableFirewall = function() {
    firewallAction(['--force', 'enable'], false, false);
};

const disableFirewall = function() {
    jsTestLog('Disable firewall');
    firewallAction(['disable'], true /* this can fail */, false);
};
