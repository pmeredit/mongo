// Tests that the start up smoke tests can be disabled.

(function() {
    load("src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldap_authz_lib.js");

    var configGenerator = new LDAPTestConfigGenerator();
    // 192.0.2.1/24 is reserved for documentation per RFC 5737
    configGenerator.ldapServers = ["192.0.2.1"];

    // Generate a config which uses an LDAP server which doesn't exist. This should fail.
    var config = configGenerator.generateMongodConfig();

    var conn = MongoRunner.runMongod(config);
    assert.eq(null, conn);

    // Edit the config, to disable the smoke test. The server should now start.
    config.ldapValidateLDAPServerConfig = false;
    conn = MongoRunner.runMongod();
    assert.neq(null, conn);
    MongoRunner.stopMongod(conn);
})();
