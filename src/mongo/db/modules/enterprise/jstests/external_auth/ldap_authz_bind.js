// Tests the bind methods and SASL bind mechanims with and without TLS

(function() {
    load("src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldap_authz_lib.js");

    // TLS and port
    var ldapSchemes = [{ldapTransportSecurity: "none"}, {ldapTransportSecurity: "tls"}];

    // bind methods and SASL bind mechanisms
    var bindOptions = [
        {
          user: adminUserDN,
          fragment: {
              ldapAuthzQueryTemplate: "{USER}?memberOf",
              ldapBindMethod: "simple",
              ldapQueryUser: simpleAuthenticationUser
          }
        },
        {
          user: adminUser,
          fragment: {
              ldapBindMethod: "sasl",
              ldapBindSaslMechanisms: "DIGEST-MD5",
              ldapQueryUser: saslAuthenticationUser
          }
        },
        {
          user: adminUser,
          fragment: {
              ldapBindMethod: "sasl",
              ldapBindSaslMechanisms: "GSSAPI",
              ldapQueryUser: saslAuthenticationUser
          }
        },
    ];

    ldapSchemes.forEach(function(s) {
        bindOptions.forEach(function(b) {
            if (b.fragment.ldapBindSaslMechanisms == "GSSAPI" ||
                (_isWindows() && s.ldapTransportSecurity)) {
                // FIXME: Import TLS certificates on Windows builders with SERVER-24672.
                // FIXME: Enable support for GSSAPI on Windows builders with SERVER-24671.
                return;
            }
            var saslString = b.fragment.ldapBindMethod;
            if (b.fragment.ldapBindMethod == "sasl") {
                saslString += " and " + b.fragment.ldapBindSaslMechanisms;
            }
            print("Attempting to bind with protocol security '" + s.ldapTransportSecurity +
                  "' as " + b.user + " with " + saslString);

            // use LDAP for authentication
            var authOptions =
                {user: b.user, pwd: defaultPwd, mechanism: "PLAIN", digestPassword: false};
            var authArgs = {authOptions: authOptions, user: b.user};

            var configGenerator = new LDAPTestConfigGenerator();
            Object.assign(configGenerator, s);
            Object.assign(configGenerator, b.fragment);
            runTests(authAndVerify, configGenerator, authArgs);
        });
    });

    // Tests that the server will fail to start with unsupported configurations
    function testConfiguration(mechName, expect = false, extraOpts = {}) {
        var opts = {
            ldapServers: baseLDAPUrls,
            ldapTransportSecurity: "none",
            ldapBindMethod: "sasl",
            ldapBindSaslMechanisms: mechName,
            ldapQueryUser: saslAuthenticationUser,
            ldapQueryPassword: "Admin001",
        };

        var conn = MongoRunner.runMongod(Object.merge(opts, extraOpts));
        assert.eq(expect, conn !== null);
        if (conn) {
            MongoRunner.stopMongod(conn);
        }
    }

    testConfiguration("unknown");
    if (_isWindows()) {
        // FIXME: Enable support for GSSAPI on Windows builders with SERVER-24671.
        // testConfiguration("GSSAPI", true);
    } else {
        testConfiguration("GSSAPI", false);

        testConfiguration("GSSAPI", true, {
            setParameter: {saslauthdPath: "fdhflkjsadhf"},
            env: new LDAPTestConfigGenerator().generateEnvConfig()
        });

        testConfiguration("GSSAPI", true, {
            config: assetsPath + "mongodb-saslAuthd-snippet.conf",
            env: new LDAPTestConfigGenerator().generateEnvConfig()
        });
    }

})();
