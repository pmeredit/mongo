// Test that an LDAP backend returning LDAP_INSUFFICIENT_PRIVILEGES(50)
// will not prevent connection (only real queries).

(function() {
'use strict';

if (_isWindows()) {
    // ldaptor fails to return message on windows.
    return;
}

const enterprise = 'src/mongo/db/modules/enterprise/';
load(enterprise + '/jstests/external_auth/lib/ldap_authz_lib.js');
const proxy = enterprise + '/jstests/external_auth/lib/ldapproxy.py';

function doTestWithAuthFail(code) {
    const port = allocatePort();
    const pid = startMongoProgramNoConnect(
        'python', proxy, '--port', port, '--delay', 0, '--unauthorizedRootDSE', code);

    // Wait for the proxy to actually start up and accept connections.
    assert.soon(function() {
        return 0 ===
            runNonMongoProgram('python',
                               proxy,
                               '--testClient',
                               '--targetHost',
                               '127.0.0.1',
                               '--targetPort',
                               port);
    });

    const configGenerator = new LDAPTestConfigGenerator();
    configGenerator.ldapServers = ['localhost:' + port];
    configGenerator.ldapAuthzQueryTemplate = "ou=Groups,dc=10gen,dc=cc" +
        "??one?(&(objectClass=groupOfNames)(member={USER}))";

    const mongodOptions = configGenerator.generateMongodConfig();
    const mongod = MongoRunner.runMongod(mongodOptions);
    setupTest(mongod);
    MongoRunner.stopMongod(mongod);

    stopMongoProgramByPid(pid);
}

doTestWithAuthFail(47);  // LDAP_X_PROXY_FAILURE
doTestWithAuthFail(48);  // LDAP_INAPPROPRIATE_AUTH
doTestWithAuthFail(49);  // LDAP_INVALID_CREDENTIALS
doTestWithAuthFail(50);  // LDAP_INSUFFICIENT_RIGHTS
})();
