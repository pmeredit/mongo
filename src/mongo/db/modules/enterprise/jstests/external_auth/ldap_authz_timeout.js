// Tests that connections to multiple servers are correctly parsed and that if the client cannot
// connect to the first LDAP server, it will fallback to the next one.
// Excluded from AL2 Atlas Enterprise build variant since it depends on libldap_r, which
// is not installed on that variant.
// @tags: [incompatible_with_atlas_environment]

import {
    adminUser,
    authAndVerify,
    defaultPwd,
    defaultUserDNSuffix,
    LDAPTestConfigGenerator,
    runTests
} from "src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldap_authz_lib.js";

var configGenerator = new LDAPTestConfigGenerator();
configGenerator.startMockupServer();
// 192.0.2.1/24 is reserved for documentation per RFC 5737
configGenerator.ldapServers = ["192.0.2.1"].concat(configGenerator.ldapServers);
configGenerator.ldapUserToDNMapping =
    [{match: "(.+)", substitution: "cn={0}," + defaultUserDNSuffix}];
configGenerator.ldapAuthzQueryTemplate = "{USER}?memberOf";
runTests(authAndVerify, configGenerator, {
    authOptions: {user: adminUser, pwd: defaultPwd, mechanism: "PLAIN", digestPassword: false},
    user: adminUser
});

configGenerator.stopMockupServer();
