// Test that you can set multiple passwords for the ldapQueryPassword during an LDAP password
// rollover and that it actually allows successful authentication.

(function() {
    load("src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldap_authz_lib.js");

    var authOptions =
        {user: adminUserDN, pwd: defaultPwd, mechanism: "PLAIN", digestPassword: false};

    // Test a query for users which are listed as attributes on groups
    // FIXME: This should be merged into the lib configuration somehow
    var configGenerator = new LDAPTestConfigGenerator();
    configGenerator.ldapQueryPassword = "BadPasswordThatIsntValidAtAll";
    configGenerator.ldapAuthzQueryTemplate =
        "ou=Groups,dc=10gen,dc=cc" + "??one?(&(objectClass=groupOfNames)(member={USER}))";

    const mongodOptions = configGenerator.generateMongodConfig();
    mongodOptions.ldapValidateLDAPServerConfig = "false";

    const mongod = MongoRunner.runMongod(mongodOptions);
    setupTest(mongod);

    externalDB = mongod.getDB("$external");

    assert.eq(0, externalDB.auth(authOptions));

    const adminDB = mongod.getDB("admin");
    adminDB.auth("siteRootAdmin", "secret");
    assert.commandWorked(adminDB.runCommand(
        {setParameter: 1, ldapQueryPassword: ["BadPasswordThatIsntValidAtAll", "Admin001"]}));
    adminDB.logout();

    assert(externalDB.auth(authOptions));

    MongoRunner.stopMongod(mongod);
})();
