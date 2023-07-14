// @tags: [requires_ldap_pool]

(function() {
'use strict';

load("src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldap_authz_lib.js");
load("src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldap_mock_server_utils.js");

function runParallelAuth(iterations) {
    // ୧ʕ•̀ᴥ•́ʔ୨ Do a whole bunch of auth! Let's try authenticating iterations^2 times in parallel
    // shells.
    for (let j = 0; j < iterations; j++) {
        let parallelShells = [];
        for (let i = 0; i < iterations; i++) {
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

        // Wait for all the shells we've started to finish.
        parallelShells.forEach((shell) => waitProgram(shell));
    }
}

function assertExpectedHostStats(fastLDAPProxy) {
    const stats = adminDB.runCommand({serverStatus: 1, ldapConnPool: 1})["ldapConnPool"];
    let perHostStats = stats['ldapServerStats'];
    jsTestLog(tojson(stats));

    // check that we connected to all the hosts
    assert.lte(perHostStats.length, 4);
    assert.gte(perHostStats.length, 1);

    // check that the most used host was the fast proxy
    perHostStats.sort((a, b) => a.uses < b.uses);
    assert.eq(perHostStats[0].host, fastLDAPProxy);
}

const proxies = [];
const ldapServers = [];
let fastLDAPProxy;
// start four LDAP proxies that will proxy connections to ldaptest.10gen.cc. 3 out of 4 will
// delay all requests by 5 seconds. 1 of them will not delay at all. We'll make sure that one
// actually works and responds to a root dse query.
for (let i = 0; i < 4; i++) {
    // Set the delay to 5 seconds for the first 3 and 0 for the fourth.
    let delay = 5;
    if (i === 3) {
        delay = 0;
    }

    // Create, start, and save the proxy and its host:port.
    const proxy = new LDAPProxy('ldaptest.10gen.cc', 389, delay);
    proxy.start();
    proxies.push(proxy);
    ldapServers.push(proxy.getHostAndPort());

    // Separately store the name of the fast proxy and run a DSE query against it to check it's
    // working.
    if (i === 3) {
        fastLDAPProxy = proxy.getHostAndPort();
        const testProxyClient = new LDAPProxy('localhost', proxy.getPort(), 0);
        testProxyClient.runTestClient();
    }
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

runParallelAuth(10);
assertExpectedHostStats(fastLDAPProxy);

// Now, make the first LDAP server fast and the last LDAP server slow and assert that mongod
// changes its priority accordingly.
proxies[0].setDelay(0);
proxies[3].setDelay(5);
fastLDAPProxy = proxies[0].getHostAndPort();
runParallelAuth(20);
assertExpectedHostStats(fastLDAPProxy);

MongoRunner.stopMongod(mongod);
proxies.forEach((proxy) => proxy.stop());
})();
