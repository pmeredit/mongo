// Test setting and getting runtime configuration using setParameter and getParameter.

(function() {
    load("src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldap_authz_lib.js");

    function runtimeConfigurationCallback(m) {
        authOptions = {user: adminUser, pwd: defaultPwd, mechanism: "PLAIN", digestPassword: false};

        // Verify that we have a broken configuration.
        assert.throws(authAndVerify,
                      [m, {authOptions: authOptions, user: adminUser}],
                      "expected auth to fail");

        var adminDB = m.getDB("admin");
        adminDB.auth("siteRootAdmin", "secret");

        var ldapServers = baseLDAPUrls[0];
        assert.commandWorked(m.adminCommand({setParameter: 1, "ldapServers": ldapServers}));
        var ret = m.adminCommand({getParameter: 1, "ldapServers": 1});
        assert.commandWorked(ret);
        assert.eq(ldapServers,
                  ret.ldapServers,
                  "Unexpected getParameter return for ldapServers: " + ret.ldapServers);

        var ldapTimeoutMS = 10000;
        assert.commandWorked(m.adminCommand({setParameter: 1, "ldapTimeoutMS": ldapTimeoutMS}));
        ret = m.adminCommand({getParameter: 1, "ldapTimeoutMS": 1});
        assert.commandWorked(ret);
        assert.eq(ldapTimeoutMS,
                  ret.ldapTimeoutMS,
                  "Unexpected getParameter return for ldapServers: " + ret.ldapTimeoutMS);

        var ldapQueryUser = simpleAuthenticationUser;
        assert.commandWorked(m.adminCommand({setParameter: 1, "ldapQueryUser": ldapQueryUser}));
        ret = m.adminCommand({getParameter: 1, "ldapQueryUser": 1});
        assert.commandWorked(ret);
        assert.eq(ldapQueryUser,
                  ret.ldapQueryUser,
                  "Unexpected getParameter return for ldapQueryUser: " + ret.ldapQueryUser);

        assert.commandWorked(m.adminCommand({setParameter: 1, "ldapQueryPassword": "Admin001"}));
        ret = m.adminCommand({getParameter: 1, "ldapQueryPassword": 1});
        assert.commandWorked(ret);
        assert.eq("###",
                  ret.ldapQueryPassword,
                  "Unexpected getParameter return for ldapQueryPassword: " + ret.ldapQueryPassword);

        var ldapUserToDNMapping =
            "{match: \"(.+)\", substitution: \"cn={0}," + defaultUserDNSuffix + "\"}";
        assert.commandWorked(
            m.adminCommand({setParameter: 1, "ldapUserToDNMapping": ldapUserToDNMapping}));
        ret = m.adminCommand({getParameter: 1, "ldapUserToDNMapping": 1});
        assert.commandWorked(ret);
        assert.eq(
            ldapUserToDNMapping,
            ret.ldapUserToDNMapping,
            "Unexpected getParameter return for ldapUserToDNMapping: " + ret.ldapUserToDNMapping);

        var ldapAuthzQueryTemplate = "{USER}?memberOf";
        assert.commandWorked(
            m.adminCommand({setParameter: 1, "ldapAuthzQueryTemplate": ldapAuthzQueryTemplate}));
        ret = m.adminCommand({getParameter: 1, "ldapAuthzQueryTemplate": 1});
        assert.commandWorked(ret);
        assert.eq(ldapAuthzQueryTemplate,
                  ret.ldapAuthzQueryTemplate,
                  "Unexpected getParameter return for ldapAuthzQueryTemplate: " +
                      ret.ldapAuthzQueryTemplate);

        adminDB.logout();

        // Verify that we have a working configuration.
        authAndVerify(m, {authOptions: authOptions, user: adminUser});
    }

    var configGenerator = new LDAPTestConfigGenerator();

    this.ldapServers = ["badserver.invalid"];
    this.ldapAuthzQueryTemplate = "cn={USER}," + defaultUserDNSuffix + "?notMemberOf";
    this.ldapQueryUser = "badUser";
    this.ldapQueryPassword = "badPassword";

    runTests(runtimeConfigurationCallback, configGenerator);
})();
