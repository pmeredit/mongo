// Tests the various authentication methods combined with ldap authorization
// Each test contains two functions xxxxTest and xxxxTestCallback.
// the former is a wrapper test function that sets up the server and the
// callback does the actual testing invoked by runTests

(function() {
    load("src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldap_authz_lib.js");

    // Kerberos authentication + LDAP authorization
    function testGSSAPI() {
        var configGenerator = new LDAPTestConfigGenerator();
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
        configGenerator.ldapAuthzQueryTemplate = "{USER}?memberOf";
        configGenerator.ldapUserToDNMapping = [
            {match: "(ldapz_ldap1)", substitution: "cn={0}," + defaultUserDNSuffix},
            {match: "(ldapz_ldap2)", ldapQuery: defaultUserDNSuffix + "??one?(cn={0})"}
        ];
        configGenerator.useSaslauthd = true;

        withSaslauthd(saslauthdConfigFile, configGenerator, function() {
            runTests(ldapTestCallback, configGenerator);
        });
    }

    // LDAP native authentication + LDAP authorization
    function testNativeLDAP() {
        var configGenerator = new LDAPTestConfigGenerator();
        configGenerator.ldapAuthzQueryTemplate = "{USER}?memberOf";
        configGenerator.ldapUserToDNMapping = [
            {match: "(ldapz_ldap1)", substitution: "cn={0}," + defaultUserDNSuffix},
            {match: "(ldapz_ldap2)", ldapQuery: defaultUserDNSuffix + "??one?(cn={0})"}
        ];

        runTests(ldapTestCallback, configGenerator);
    }

    function ldapTestCallback({conn}) {
        var user1 = "ldapz_ldap1";
        var user2 = "ldapz_ldap2";

        var authOptions = {user: user1, pwd: defaultPwd, mechanism: "PLAIN", digestPassword: false};

        authAndVerify({conn: conn, options: {authOptions: authOptions, user: user1}});

        authOptions = {user: user2, pwd: defaultPwd, mechanism: "PLAIN", digestPassword: false};

        authAndVerify({conn: conn, options: {authOptions: authOptions, user: user2}});
    }

    // X509 Authentication + LDAP Authorization
    function testX509() {
        var configGenerator = new LDAPTestConfigGenerator();
        configGenerator.ldapAuthzQueryTemplate = "{USER}?memberOf";
        configGenerator.ldapUserToDNMapping = [
            {
              match: "C=US,ST=New York,L=New York City,O=MongoDB,OU=KernelUser,(CN=ldapz_x509_1)",
              substitution: "CN=ldapz_x509_1," + defaultUserDNSuffix
            },
            {
              match: "C=US,ST=New York,L=New York City,O=MongoDB,OU=KernelUser,CN=(ldapz_x509_2)",
              ldapQuery: defaultUserDNSuffix + "??one?(cn={0})"
            }
        ];
        configGenerator.authenticationMechanisms = ["MONGODB-X509", "SCRAM-SHA-1"];

        runTests(testX509Callback, configGenerator);
    }

    function testX509Callback({conn}) {
        // We can't run the command in the current shell since there is no way to
        // change client certificates

        setupCmd = "load(\"src/mongo/db/modules/enterprise/" +
            "jstests/external_auth/lib/ldap_authz_lib.js\");";

        var authCmd1 =
            "\
        var user1 = \"C=US,ST=New York,L=New York City,O=MongoDB,OU=KernelUser," +
            "CN=ldapz_x509_1\";\
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
        var user2 = \"C=US,ST=New York,L=New York City,O=MongoDB,OU=KernelUser," +
            "CN=ldapz_x509_2\";\
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
    testX509();
    if (!_isWindows()) {
        // Windows machines must be a part of the Kerberos domain. Move this out with SERVER-24671.
        testGSSAPI();
        // Windows machines don't support saslauthd.
        testLDAPSaslauthd();
    }

})();
