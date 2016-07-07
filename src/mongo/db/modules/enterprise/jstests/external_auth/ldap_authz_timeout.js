// Tests that connections to multiple servers are correctly parsed and that if the client cannot
// connect to the first LDAP server, it will fallback to the next one.

(function() {
    load("src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldap_authz_lib.js");

    var configGenerator = new LDAPTestConfigGenerator();
    // 192.0.2.1/24 is reserved for documentation per RFC 5737
    configGenerator.ldapServers = ["192.0.2.1"].concat(configGenerator.ldapServers);
    configGenerator.ldapUserToDNMapping =
        [{match: "(.+)", substitution: "cn={0}," + defaultUserDNSuffix}];
    configGenerator.ldapAuthzQueryTemplate = "{USER}?memberOf";
    runTests(authAndVerify, configGenerator, {
        authOptions: {user: adminUser, pwd: defaultPwd, mechanism: "PLAIN", digestPassword: false},
        user: adminUser
    });
})();
