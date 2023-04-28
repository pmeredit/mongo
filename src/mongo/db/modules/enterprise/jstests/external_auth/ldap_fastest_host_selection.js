// @tags: [requires_ldap_pool]

(function() {
'use strict';

load("src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldap_authz_lib.js");

let proxyPids = [];
let ldapServers = [];
let fastLDAPProxy;
// start four LDAP proxies that will proxy connections to ldaptest.10gen.cc. 3 out of 4 will
// delay all requests by 5 seconds. 1 of them will not delay at all. We'll make sure that one
// actually works and responds to a root dse query.
for (let i = 0; i < 4; i++) {
    let delay = 5;
    let proxyPort = allocatePort();

    if (i == 3) {
        fastLDAPProxy = `localhost:${proxyPort}`;
        delay = 0;
    }

    const proxyPath = "src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldapproxy.py";
    let pid = startMongoProgramNoConnect("python",
                                         proxyPath,
                                         "--port",
                                         proxyPort,
                                         "--targetHost",
                                         "ldaptest.10gen.cc",
                                         "--targetPort",
                                         "389",
                                         "--delay",
                                         delay);
    if (delay == 0) {
        assert.soon(function() {
            let exitCode = runNonMongoProgram("python",
                                              proxyPath,
                                              "--testClient",
                                              "--targetHost",
                                              "localhost",
                                              "--targetPort",
                                              proxyPort);
            return exitCode == 0;
        });
    }

    proxyPids.push(pid);
    ldapServers.push(`localhost:${proxyPort}`);
}

// setup a mongod that connects to these four hosts
var configGenerator = new LDAPTestConfigGenerator();
configGenerator.ldapServers = ldapServers;
configGenerator.ldapAuthzQueryTemplate = "ou=Groups,dc=10gen,dc=cc" +
    "??one?(&(objectClass=groupOfNames)(member={USER}))";
// Increase timeout from default of 10 seconds to 20 seconds to account for delayed responses.
configGenerator.ldapTimeoutMS = 20000;

const mongodOptions = configGenerator.generateMongodConfig();
const mongod = MongoRunner.runMongod(mongodOptions);
setupTest(mongod);

const adminConn = new Mongo(mongod.host);
const adminDB = adminConn.getDB("admin");
adminDB.auth("siteRootAdmin", "secret");

// ୧ʕ•̀ᴥ•́ʔ୨ Do a whole bunch of auth! Let's try authenticating 625 times in parallel shells.
for (let j = 0; j < 25; j++) {
    let parallelShells = [];
    for (let i = 0; i < 25; i++) {
        let shell = startMongoProgramNoConnect("mongo",
                                               "--username",
                                               adminUserDN,
                                               "--password",
                                               defaultPwd,
                                               "--authenticationDatabase",
                                               "$external",
                                               "--authenticationMechanism",
                                               "PLAIN",
                                               "--host",
                                               mongod.host,
                                               "--eval",
                                               ";");
        parallelShells.push(shell);
    }

    // Wait for all the shells we've started to finish successfully
    parallelShells.forEach((shell) => assert.eq(waitProgram(shell), 0));
}

const stats = adminDB.runCommand({serverStatus: 1, ldapConnPool: 1})["ldapConnPool"];
let perHostStats = stats['ldapServerStats'];
jsTestLog(tojson(stats));

// check that we connected to all the hosts
assert.lte(perHostStats.length, 4);
assert.gte(perHostStats.length, 1);

// check that the most used host was the fast proxy
perHostStats.sort((a, b) => a.uses < b.uses);
assert.eq(perHostStats[0].host, fastLDAPProxy);

// check that the host with the lowest latency was the fast proxy
const latencyForHost = (host) => host.latencyMillis ? host.latencyMillis : Number.MAX_VALUE;
perHostStats.sort((a, b) => latencyForHost(a) < latencyForHost(b));
assert.eq(perHostStats[perHostStats.length - 1].host, fastLDAPProxy);

MongoRunner.stopMongod(mongod);
proxyPids.forEach((pid) => stopMongoProgramByPid(pid));
})();
