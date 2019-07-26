// this tests ability to bind to LDAP server on an IPv6 address

(function() {
'use strict';

load("src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldap_authz_lib.js");

let ldapServers = [];
let proxyPort = allocatePort();
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
                                     0);

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

// LDAP server on IPv6 address
ldapServers.push(`[::1]:${proxyPort}`);

// setup a mongod that connects to these four hosts
var configGenerator = new LDAPTestConfigGenerator();
configGenerator.ldapServers = ldapServers;
configGenerator.ldapAuthzQueryTemplate = "ou=Groups,dc=10gen,dc=cc" +
    "??one?(&(objectClass=groupOfNames)(member={USER}))";

const mongodOptions = configGenerator.generateMongodConfig();
const mongod = MongoRunner.runMongod(mongodOptions);
setupTest(mongod);

const adminConn = new Mongo(mongod.host);
const adminDB = adminConn.getDB("admin");
adminDB.auth("siteRootAdmin", "secret");

// try to authenticate
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

// Wait for the shell we've started to finish successfully
assert.eq(waitProgram(shell), 0);
const stats = assert.commandWorked(adminDB.runCommand({serverStatus: 1, ldapConnPool: 1}),
                                   "Server could not run status command");
jsTestLog(tojson(stats));

MongoRunner.stopMongod(mongod);
stopMongoProgramByPid(pid);
})();
