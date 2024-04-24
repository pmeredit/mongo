// Tests the various authentication methods combined with ldap authorization
// Each test contains two functions xxxxTest and xxxxTestCallback.
// the former is a wrapper test function that sets up the server and the
// callback does the actual testing invoked by runTests
// Excluded from AL2 Atlas Enterprise build variant since it depends on libldap_r, which
// is not installed on that variant.
// @tags: [incompatible_with_atlas_environment]

import {
    assetsPath,
    authAndVerify,
    defaultPwd,
    defaultUserDNSuffix,
    LDAPTestConfigGenerator,
    runTests,
    saslauthdConfigFile,
    saslHostName,
    withSaslauthd
} from "src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldap_authz_lib.js";

// Kerberos authentication + LDAP authorization
function testGSSAPI() {
    var configGenerator = new LDAPTestConfigGenerator();
    configGenerator.startMockupServer();
    configGenerator.authenticationMechanisms = ["GSSAPI", "SCRAM-SHA-1"];
    configGenerator.ldapAuthzQueryTemplate = "{USER}?memberOf";
    configGenerator.ldapUserToDNMapping = [
        {
            match: "(ldapz_kerberos1)@LDAPTEST.10GEN.CC",
            substitution: "cn={0}," + defaultUserDNSuffix
        },
        {
            match: "(ldapz_kerberos2@LDAPTEST.10GEN.CC)",
            ldapQuery: defaultUserDNSuffix + "??one?krbPrincipalName={0}"
        }
    ];

    runTests(testGSSAPICallback, configGenerator);
    configGenerator.stopMockupServer();
}

function testGSSAPICallback({conn}) {
    var user1 = "ldapz_kerberos1@LDAPTEST.10GEN.CC";
    var user2 = "ldapz_kerberos2@LDAPTEST.10GEN.CC";

    run("kdestroy");  // remove any previous tickets
    run("kinit", "-k", "-t", assetsPath + "ldapz_kerberos1.keytab", user1);

    var authOptions = {user: user1, mechanism: "GSSAPI", serviceHostname: "localhost"};

    authAndVerify({conn: conn, options: {authOptions: authOptions, user: user1}});

    run("kdestroy");
    run("kinit", "-k", "-t", assetsPath + "ldapz_kerberos2.keytab", user2);
    authOptions = {user: user2, mechanism: "GSSAPI", serviceHostname: "localhost"};

    authAndVerify({conn: conn, options: {authOptions: authOptions, user: user2}});
}

// LDAP Saslauthd authentication + LDAP authorization
function testLDAPSaslauthd() {
    var configGenerator = new LDAPTestConfigGenerator();
    configGenerator.startMockupServer();
    configGenerator.ldapAuthzQueryTemplate = "{USER}?memberOf";
    configGenerator.ldapUserToDNMapping = [
        {match: "(ldapz_ldap1)", substitution: "cn={0}," + defaultUserDNSuffix},
        {match: "(ldapz_ldap2)", ldapQuery: defaultUserDNSuffix + "??one?(cn={0})"}
    ];
    configGenerator.useSaslauthd = true;

    withSaslauthd(saslauthdConfigFile, configGenerator, function() {
        runTests(ldapTestCallback, configGenerator);
    });
    configGenerator.stopMockupServer();
}

// LDAP native authentication + LDAP authorization
function testNativeLDAP() {
    var configGenerator = new LDAPTestConfigGenerator();
    configGenerator.startMockupServer();
    configGenerator.ldapAuthzQueryTemplate = "{USER}?memberOf";
    configGenerator.ldapUserToDNMapping = [
        {match: "(ldapz_ldap1)", substitution: "cn={0}," + defaultUserDNSuffix},
        {match: "(ldapz_ldap2)", ldapQuery: defaultUserDNSuffix + "??one?(cn={0})"}
    ];

    runTests(ldapTestCallback, configGenerator);
    configGenerator.stopMockupServer();
}

function ldapTestCallback({conn}) {
    var user1 = "ldapz_ldap1";
    var user2 = "ldapz_ldap2";

    var authOptions = {user: user1, pwd: defaultPwd, mechanism: "PLAIN", digestPassword: false};

    authAndVerify({conn: conn, options: {authOptions: authOptions, user: user1}});

    authOptions = {user: user2, pwd: defaultPwd, mechanism: "PLAIN", digestPassword: false};

    authAndVerify({conn: conn, options: {authOptions: authOptions, user: user2}});
}

// LDAP native authentication + LDAP authorization + No DN Mapping
function testNativeLDAPNoDNMapping() {
    var configGenerator = new LDAPTestConfigGenerator();
    configGenerator.startMockupServer();
    configGenerator.ldapAuthzQueryTemplate = "{USER}?memberOf";
    configGenerator.ldapUserToDNMapping = [];

    runTests(ldapTestCallbackUsernameIsDN, configGenerator);
    configGenerator.stopMockupServer();
}

// LDAP native authentication + LDAP authorization + No DN Mapping
function testNativeLDAPUndefinedDNMapping() {
    var configGenerator = new LDAPTestConfigGenerator();
    configGenerator.startMockupServer();
    configGenerator.ldapAuthzQueryTemplate = "{USER}?memberOf";

    runTests(ldapTestCallbackUsernameIsDN, configGenerator);
    configGenerator.stopMockupServer();
}

function ldapTestCallbackUsernameIsDN({conn}) {
    var user1 = "cn=ldapz_ldap1," + defaultUserDNSuffix;
    var user2 = "cn=ldapz_ldap2," + defaultUserDNSuffix;

    var authOptions = {user: user1, pwd: defaultPwd, mechanism: "PLAIN", digestPassword: false};

    authAndVerify({conn: conn, options: {authOptions: authOptions, user: user1}});

    authOptions = {user: user2, pwd: defaultPwd, mechanism: "PLAIN", digestPassword: false};

    authAndVerify({conn: conn, options: {authOptions: authOptions, user: user2}});
}

function testNativeAndGSSAPI() {
    let configGenerator = new LDAPTestConfigGenerator();
    configGenerator.startMockupServer();
    configGenerator.authenticationMechanisms = ["PLAIN", "GSSAPI", "SCRAM-SHA-1"];
    configGenerator.ldapAuthzQueryTemplate = "{USER}?memberOf";
    configGenerator.ldapUserToDNMapping = [
        {
            match: "(ldapz_kerberos1)@LDAPTEST.10GEN.CC",
            substitution: "cn={0}," + defaultUserDNSuffix
        },
        {
            match: "(ldapz_kerberos2@LDAPTEST.10GEN.CC)",
            ldapQuery: defaultUserDNSuffix + "??one?krbPrincipalName={0}"
        },
        {match: "(ldapz_ldap1)", substitution: "cn={0}," + defaultUserDNSuffix},
        {match: "(ldapz_ldap2)", ldapQuery: defaultUserDNSuffix + "??one?(cn={0})"}
    ];

    runTests(testNativeAndGSSAPICallback, configGenerator);
    configGenerator.stopMockupServer();
}

function testNativeAndGSSAPICallback({conn}) {
    var ldapUser1 = "ldapz_ldap1";
    var ldapUser2 = "ldapz_ldap2";

    var authOptions = {user: ldapUser1, pwd: defaultPwd, mechanism: "PLAIN", digestPassword: false};

    authAndVerify({conn: conn, options: {authOptions: authOptions, user: ldapUser1}});

    authOptions = {user: ldapUser2, pwd: defaultPwd, mechanism: "PLAIN", digestPassword: false};

    authAndVerify({conn: conn, options: {authOptions: authOptions, user: ldapUser2}});

    var gssapiUser1 = "ldapz_kerberos1@LDAPTEST.10GEN.CC";
    var gssapiUser2 = "ldapz_kerberos2@LDAPTEST.10GEN.CC";

    run("kdestroy");  // remove any previous tickets
    run("kinit", "-k", "-t", assetsPath + "ldapz_kerberos1.keytab", gssapiUser1);

    authOptions = {user: gssapiUser1, mechanism: "GSSAPI", serviceHostname: "localhost"};

    authAndVerify({conn: conn, options: {authOptions: authOptions, user: gssapiUser1}});

    run("kdestroy");
    run("kinit", "-k", "-t", assetsPath + "ldapz_kerberos2.keytab", gssapiUser2);
    authOptions = {user: gssapiUser2, mechanism: "GSSAPI", serviceHostname: "localhost"};

    authAndVerify({conn: conn, options: {authOptions: authOptions, user: gssapiUser2}});
}

// X509 Authentication + LDAP Authorization
function testX509() {
    var configGenerator = new LDAPTestConfigGenerator();
    configGenerator.startMockupServer();
    configGenerator.ldapAuthzQueryTemplate = "{USER}?memberOf";
    configGenerator.ldapUserToDNMapping = [
        {
            match: "(CN=ldapz_x509_1),OU=KernelUser,O=MongoDB,L=New York City,ST=New York,C=US",
            substitution: "CN=ldapz_x509_1," + defaultUserDNSuffix
        },
        {
            match: "CN=(ldapz_x509_2),OU=KernelUser,O=MongoDB,L=New York City,ST=New York,C=US",
            ldapQuery: defaultUserDNSuffix + "??one?(cn={0})"
        }
    ];
    configGenerator.authenticationMechanisms = ["MONGODB-X509", "SCRAM-SHA-1"];

    runTests(testX509Callback, configGenerator);
    configGenerator.stopMockupServer();
}

function testX509Callback({conn}) {
    // We can't run the command in the current shell since there is no way to
    // change client certificates

    const setupCmd =
        "import {authAndVerify} from \"src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldap_authz_lib.js\";";

    var authCmd1 =
        "\
        var user1 = \"CN=ldapz_x509_1,OU=KernelUser,O=MongoDB,L=New York City,ST=New York,C=US\";\
        \
        var authOptions = {\
            mechanism: \"MONGODB-X509\",\
            user: user1\
        };\
        \
        authAndVerify({conn: null, options: {authOptions: authOptions, user: user1}});\
    ";

    var authCmd2 =
        "\
        var user2 = \"CN=ldapz_x509_2,OU=KernelUser,O=MongoDB,L=New York City,ST=New York,C=US\";\
        \
        var authOptions = {\
            mechanism: \"MONGODB-X509\",\
            user: user2\
        };\
        \
        authAndVerify({conn: null, options: {authOptions: authOptions, user: user2}});\
    ";

    var retVal = run('mongo',
                     '--ssl',
                     '--sslPEMKeyFile',
                     assetsPath + "ldapz_x509_1.pem",
                     '--sslCAFile',
                     'jstests/libs/ca.pem',
                     '--port',
                     conn.port,
                     '--host',
                     saslHostName,
                     '--eval',
                     setupCmd + authCmd1);

    assert(retVal == 0);

    retVal = run('mongo',
                 '--ssl',
                 '--sslPEMKeyFile',
                 assetsPath + "ldapz_x509_2.pem",
                 '--sslCAFile',
                 'jstests/libs/ca.pem',
                 '--port',
                 conn.port,
                 '--host',
                 saslHostName,
                 '--eval',
                 setupCmd + authCmd2);

    assert(retVal == 0);
}

testNativeLDAP();
testNativeLDAPNoDNMapping();
testNativeLDAPUndefinedDNMapping();
testX509();
if (!_isWindows()) {
    // Windows machines must be a part of the Kerberos domain. Move this out with SERVER-24671.
    testGSSAPI();
    // Windows machines don't support saslauthd.
    testLDAPSaslauthd();
    testNativeAndGSSAPI();
}
