/**
 * Tests some common LDAP authorization queries
 * 1. users as attributes on groups
 * 2. queries with UTF-8 characters
 *
 * Some queries are covered in other tests
 * a. groups as attributes on users (ldap_authz_authn.js)
 * b. constructed DNs (ldap_authz_permissions.js)
 */

(function() {
    load("src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldap_authz_lib.js");

    var authOptions =
        {user: adminUserDN, pwd: defaultPwd, mechanism: "PLAIN", digetPassword: false};

    // Test a query for users which are listed as attributes on groups
    // FIXME: This should be merged into the lib configuration somehow
    var configGenerator = new LDAPTestConfigGenerator();
    configGenerator.ldapAuthzQueryTemplate =
        "ou=Groups,dc=10gen,dc=cc" + "??one?(&(objectClass=groupOfNames)(member={USER}))";

    runTests(authAndVerify, configGenerator, {authOptions: authOptions, user: adminUserDN});

    // Test a query which contains UTF-8 characters
    configGenerator.ldapAuthzQueryTemplate = "{USER}?memberOf";
    configGenerator.ldapUserToDNMapping =
        [{match: ".*", ldapQuery: defaultUserDNSuffix + "??one?(description=\\u25A0 \\u25A0)"}];

    runTests(authAndVerify, configGenerator, {authOptions: authOptions, user: adminUserDN});
})();
