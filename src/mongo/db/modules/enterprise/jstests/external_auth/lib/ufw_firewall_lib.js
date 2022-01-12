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

// Resolves server name to IP address using dig, works only on Unix platforms.
const resolveIPUnix = function(host) {
    clearRawMongoProgramOutput();
    runMongoProgram('dig', '+short', host);
    const out = rawMongoProgramOutput();
    jsTestLog(out);
    const matchIp = out.match(
        /\b(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)/i);
    jsTestLog(matchIp);
    return matchIp[0];
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
    firewallAction(['enable']);
};

const disableFirewall = function() {
    jsTestLog('Disable firewall');
    firewallAction(['disable'], true /* this can fail */);
};
