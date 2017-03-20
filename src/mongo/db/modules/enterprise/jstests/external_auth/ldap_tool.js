// Tests the LDAP validation tool against several example configurations

(function() {
    load("src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldap_authz_lib.js");

    // No config will result in an error
    assert.neq("0", runProgram("mongoldap", "--debug", "--color=false", "--user", "ldapz_admin"));

    // No config will result in an error, when not printing debug information
    assert.neq("0", runProgram("mongoldap", "--color=false", "--user", "ldapz_admin"));

    // Test a correct authz configuration with a user which exists in LDAP
    assert.eq("0",
              runProgram("mongoldap",
                         "--debug",
                         "--color=false",
                         "--ldapServers",
                         baseLDAPUrls[0],
                         "--ldapAuthzQueryTemplate",
                         "cn={USER}," + defaultUserDNSuffix + "?memberOf",
                         "--ldapTransportSecurity",
                         "none",
                         "--user",
                         "ldapz_admin"));

    // Test a correct authz configuration with a user which exists in LDAP, without debug mode
    assert.eq("0",
              runProgram("mongoldap",
                         "--color=false",
                         "--ldapServers",
                         baseLDAPUrls[0],
                         "--ldapAuthzQueryTemplate",
                         "cn={USER}," + defaultUserDNSuffix + "?memberOf",
                         "--ldapTransportSecurity",
                         "none",
                         "--user",
                         "ldapz_admin"));

    // Test a correct authn configuration with a user which exists in LDAP
    assert.eq("0",
              runProgram("mongoldap",
                         "--debug",
                         "--color=false",
                         "--ldapServers",
                         baseLDAPUrls[0],
                         "--ldapTransportSecurity",
                         "none",
                         "--user",
                         "cn=ldapz_admin," + defaultUserDNSuffix,
                         "--password",
                         "Secret123"));

    // Test a correct authz/authn configuration with a user which exists in LDAP
    assert.eq("0",
              runProgram("mongoldap",
                         "--debug",
                         "--color=false",
                         "--ldapServers",
                         baseLDAPUrls[0],
                         "--ldapAuthzQueryTemplate",
                         "{USER}?memberOf",
                         "--ldapTransportSecurity",
                         "none",
                         "--user",
                         "cn=ldapz_admin," + defaultUserDNSuffix,
                         "--password",
                         "Secret123"));

    // Test a correct configuration with a user which does not exists in LDAP
    assert.neq("0",
               runProgram("mongoldap",
                          "--debug",
                          "--color=false",
                          "--ldapServers",
                          baseLDAPUrls[0],
                          "--ldapAuthzQueryTemplate",
                          "cn={USER}," + defaultUserDNSuffix + "?memberOf",
                          "--ldapTransportSecurity",
                          "none",
                          "--user",
                          "invalid_user"));

    // Test a configuration with an incorrect LDAP server
    assert.neq("0",
               runProgram("mongoldap",
                          "--debug",
                          "--color=false",
                          "--ldapServers",
                          "bad_server.invalid",
                          "--ldapAuthzQueryTemplate",
                          "cn={USER}," + defaultUserDNSuffix + "?memberOf",
                          "--ldapTransportSecurity",
                          "none",
                          "--user",
                          "ldapz_admin"));

})();
