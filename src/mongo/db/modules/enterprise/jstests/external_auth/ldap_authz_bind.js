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
              ldapBindSASLMechs: "DIGEST-MD5",
              ldapQueryUser: saslAuthenticationUser
          }
        },
        {
          user: adminUser,
          fragment: {
              ldapBindMethod: "sasl",
              ldapBindSASLMechs: "GSSAPI",
              ldapQueryUser: saslAuthenticationUser
          }
        },
    ];

    ldapSchemes.forEach(function(s) {
        bindOptions.forEach(function(b) {
            if ((b.fragment.ldapBindSASLMechs == "GSSAPI" || s.ldapTransportSecurity) &&
                _isWindows()) {
                // FIXME: Import TLS certificates on Windows builders with SERVER-24672.
                // FIXME: Enable support for GSSAPI on Windows builders with SERVER-24671.
                return;
            }

            var saslString = b.fragment.ldapBindMethod;
            if (b.fragment.ldapBindMethod == "sasl") {
                saslString += " and " + b.fragment.ldapBindSASLMechs;
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

})();
