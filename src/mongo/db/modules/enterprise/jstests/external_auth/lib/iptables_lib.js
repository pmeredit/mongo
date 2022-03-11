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

// This function is not idempotent.  If it is called multiple times on a server
// then the below function has to be called a matching number of times.
function enableFirewallFromServer(targetHost, allowedToFail = false) {
    const shellCmd = ['sudo', 'iptables', '-I', 'INPUT', '1', '-s', targetHost, '-j', 'DROP'];
    jsTestLog(`${shellCmd}`);
    const rc = runNonMongoProgram.apply(null, shellCmd);
    if (!allowedToFail) {
        assert.eq(rc, 0);
    }
}

// This function needs to be called for every time the above function is called for every server
function disableFirewallFromServer(host, allowedToFail = false) {
    const shellCmd = ['sudo', 'iptables', '-D', 'INPUT', '-s', host, '-j', 'DROP'];
    jsTestLog(`${shellCmd}`);
    const rc = runNonMongoProgram.apply(null, shellCmd);
    if (!allowedToFail) {
        assert.eq(rc, 0);
    }
}

function isFirewallEnabledFromServer(host) {
    const shellCmd = ['sudo', 'iptables', '-C', 'INPUT', '-s', host, '-j', 'DROP'];
    jsTestLog(`${shellCmd}`);
    const rc = runNonMongoProgram.apply(null, shellCmd);
    return rc == 0;
}
