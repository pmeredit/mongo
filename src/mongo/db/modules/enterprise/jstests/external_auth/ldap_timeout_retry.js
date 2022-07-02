'use strict';

(function() {
load("jstests/libs/log.js");
load("src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldap_timeout_lib.js");

function testBindTimeoutOnStartup() {
    const configGenerator = new LDAPTestConfigGenerator();
    const config = MongoRunner.mongodOptions(configGenerator.generateMongodConfig());
    config.setParameter[`failpoint.${kBindTimeoutFailPoint}`] = "{'mode':{'times':2}}";

    const conn = MongoRunner.runMongod(config);
    assert(conn);

    setupTest(conn);

    const adminDB = conn.getDB("admin");
    assert(adminDB.auth("siteRootAdmin", "secret"));
    checkLog.containsJson(adminDB, 6709402, {}, 100);
    adminDB.logout();

    MongoRunner.stopMongod(conn);
}

function testQueryTimeout() {
    const configGenerator = new LDAPTestConfigGenerator();
    configGenerator.ldapAuthzQueryTemplate = "{USER}?memberOf";
    configGenerator.ldapUseConnectionPool = false;
    configGenerator.ldapUserToDNMapping = [
        {match: "(ldapz_ldap1)", substitution: "cn={0}," + defaultUserDNSuffix},
        {match: "(ldapz_ldap2)", ldapQuery: defaultUserDNSuffix + "??one?(cn={0})"}
    ];

    const config = MongoRunner.mongodOptions(configGenerator.generateMongodConfig());
    config.setParameter[`failpoint.${kQueryTimeoutFailPoint}`] = "{'mode':{'times':2}}";

    const conn = MongoRunner.runMongod(config);
    assert(conn);

    setupTest(conn);

    const user1 = "ldapz_ldap1";
    const authOptions1 = {user: user1, pwd: defaultPwd, mechanism: "PLAIN", digestPassword: false};
    authAndVerify({conn: conn, options: {authOptions: authOptions1, user: user1}});

    const adminDB = conn.getDB("admin");
    assert(adminDB.auth("siteRootAdmin", "secret"));
    checkLog.containsJson(adminDB, 6709403, {}, 100);
    adminDB.logout();

    MongoRunner.stopMongod(conn);
}

function testBindTimeoutAfterStartup() {
    const configGenerator = new LDAPTestConfigGenerator();
    configGenerator.ldapAuthzQueryTemplate = "{USER}?memberOf";
    configGenerator.ldapUseConnectionPool = false;
    configGenerator.ldapUserToDNMapping = [
        {match: "(ldapz_ldap1)", substitution: "cn={0}," + defaultUserDNSuffix},
        {match: "(ldapz_ldap2)", ldapQuery: defaultUserDNSuffix + "??one?(cn={0})"}
    ];

    const config = MongoRunner.mongodOptions(configGenerator.generateMongodConfig());
    const conn = MongoRunner.runMongod(config);
    assert(conn);

    setupTest(conn);

    const adminDB = conn.getDB("admin");
    assert(adminDB.auth("siteRootAdmin", "secret"));
    assert.commandWorked(
        adminDB.adminCommand({"configureFailPoint": kBindTimeoutFailPoint, "mode": {times: 2}}));
    adminDB.logout();

    const user1 = "ldapz_ldap1";
    const authOptions1 = {user: user1, pwd: defaultPwd, mechanism: "PLAIN", digestPassword: false};
    authAndVerify({conn: conn, options: {authOptions: authOptions1, user: user1}});

    assert(adminDB.auth("siteRootAdmin", "secret"));
    checkLog.containsJson(adminDB, 6709401, {}, 100);
    adminDB.logout();

    MongoRunner.stopMongod(conn);
}

testBindTimeoutOnStartup();
testQueryTimeout();
testBindTimeoutAfterStartup();
})();
