/**
 * Tests that the "Role does not exist" warning log is not emitted when resolving
 * LDAP groups into roles. However, it should still be emitted if a nonexistent role
 * is supplied to UMCs like grantPrivilegesToRole.
 */
import {
    defaultPwd,
    defaultUserDNSuffix,
    LDAPTestConfigGenerator,
    runTests
} from "src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldap_authz_lib.js";

const ldapGroup = "cn=groupA,ou=Groups,dc=10gen,dc=cc";

function testMissingRoleLogMissing({conn, shardingTest}) {
    const adminDB = conn.getDB('admin');
    const externalDB = conn.getDB('$external');
    const testDB = conn.getDB('test');
    const authInfo =
        {mechanism: "PLAIN", user: "ldapz_ldap1", pwd: defaultPwd, digestPassword: false};
    assert(externalDB.auth(authInfo), "LDAP authentication failed");
    externalDB.logout();

    // Assert that there are no "role does not exist" logs emitted even though the LDAP user has
    // groups/roles that do not actually exist.
    // For sharded clusters, perform this check on the config server primary since that
    // node processes the actual role resolution.
    assert(adminDB.auth('siteRootAdmin', 'secret'));
    if (shardingTest !== undefined) {
        const configAdmin = shardingTest.configRS.getPrimary().getDB('admin');
        assert(configAdmin.auth('siteRootAdmin', 'secret'));
        assert(checkLog.checkContainsWithCountJson(configAdmin,
                                                   5029200,
                                                   {},
                                                   /*expectedCount=*/ 0));
    } else {
        assert(checkLog.checkContainsWithCountJson(adminDB,
                                                   5029200,
                                                   {},
                                                   /*expectedCount=*/ 0));
    }

    // Now, we will create a new custom role and try to grant the LDAP group role (groupA) to
    // that custom role. This should fail because the LDAP group role does not exist, and thereby
    // emit the warning log.
    assert.commandFailed(testDB.runCommand({
        grantPrivilegesToRole: ldapGroup,
        privileges: [{resource: {db: 'test', collection: ''}, actions: ['find']}]
    }));

    // For sharded clusters, check that the log appears on the config server primary since that
    // node processes the actual role resolution.
    if (shardingTest !== undefined) {
        const configAdmin = shardingTest.configRS.getPrimary().getDB('admin');
        assert(configAdmin.auth('siteRootAdmin', 'secret'));
        assert(checkLog.checkContainsWithCountJson(configAdmin,
                                                   5029200,
                                                   {},
                                                   /*expectedCount=*/ 1));
    } else {
        assert(checkLog.checkContainsWithCountJson(adminDB,
                                                   5029200,
                                                   {},
                                                   /*expectedCount=*/ 1));
    }
}

const configGenerator = new LDAPTestConfigGenerator();
configGenerator.startMockupServer();
configGenerator.ldapAuthzQueryTemplate = "{USER}?memberOf";
configGenerator.ldapUserToDNMapping = [
    {match: "(ldapz_ldap1)", substitution: "cn={0}," + defaultUserDNSuffix},
    {match: "(ldapz_ldap2)", ldapQuery: defaultUserDNSuffix + "??one?(cn={0})"}
];
runTests(testMissingRoleLogMissing, configGenerator);
configGenerator.stopMockupServer();
