import {
    authAndVerify,
    defaultPwd,
    defaultUserDNSuffix,
    LDAPTestConfigGenerator,
    setupTest
} from "src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldap_authz_lib.js";

const configGenerator = new LDAPTestConfigGenerator();
configGenerator.ldapAuthzQueryTemplate = "{USER}?memberOf";
configGenerator.ldapUserToDNMapping = [
    {match: "(ldapz_ldap1)", substitution: "cn={0}," + defaultUserDNSuffix},
    {match: "(ldapz_ldap2)", ldapQuery: defaultUserDNSuffix + "??one?(cn={0})"}
];

const config = MongoRunner.mongodOptions(configGenerator.generateMongodConfig());
const conn = MongoRunner.runMongod(config);
assert(conn);
try {
    setupTest(conn);

    const adminDB = conn.getDB("admin");
    assert(adminDB.auth("siteRootAdmin", "secret"));
    checkLog.containsJson(adminDB, 5945602, {}, 15000);
    adminDB.logout();

    const user1 = "ldapz_ldap1";
    const authOptions = {user: user1, pwd: defaultPwd, mechanism: "PLAIN", digestPassword: false};
    authAndVerify({conn: conn, options: {authOptions: authOptions, user: user1}});

    assert(adminDB.auth("siteRootAdmin", "secret"));
    checkLog.containsJson(adminDB, 5945601, {}, 30000);
} finally {
    MongoRunner.stopMongod(conn);
}
