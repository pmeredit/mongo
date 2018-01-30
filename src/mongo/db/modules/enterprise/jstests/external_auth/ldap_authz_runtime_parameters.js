// Test setting and getting runtime configuration using setParameter and getParameter.

(function() {
    load("src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldap_authz_lib.js");

    function runtimeConfigurationCallback({conn, shardingTest}) {
        authOptions = {user: adminUser, pwd: defaultPwd, mechanism: "PLAIN", digestPassword: false};

        // Verify that we have a broken configuration.
        assert.throws(authAndVerify,
                      [{conn: conn, options: {authOptions: authOptions, user: adminUser}}],
                      "expected auth to fail");

        function executeCommand(cmd) {
            // Always run the command on the 'conn'. This will be either a target mongod or mongos.
            var adminDB = conn.getDB("admin");
            adminDB.auth("siteRootAdmin", "secret");
            var result = assert.commandWorked(conn.adminCommand(cmd));
            adminDB.logout();

            if (shardingTest !== undefined) {
                // If we're on a mongos, run the command against the backing config server
                // replicaset.
                shardingTest.configRS.nodes.forEach((node) => {
                    adminDB = node.getDB("admin");
                    adminDB.auth("siteRootAdmin", "secret");

                    assert.commandWorked(node.adminCommand(cmd));
                    adminDB.logout();
                });
            }

            return result;
        }

        var ldapServers = baseLDAPUrls[0];
        executeCommand({setParameter: 1, "ldapServers": ldapServers});
        var ret = executeCommand({getParameter: 1, "ldapServers": 1});
        assert.eq(ldapServers,
                  ret.ldapServers,
                  "Unexpected getParameter return for ldapServers: " + ret.ldapServers);

        var ldapTimeoutMS = 10000;
        executeCommand({setParameter: 1, "ldapTimeoutMS": ldapTimeoutMS});
        ret = executeCommand({getParameter: 1, "ldapTimeoutMS": 1});
        assert.eq(ldapTimeoutMS,
                  ret.ldapTimeoutMS,
                  "Unexpected getParameter return for ldapServers: " + ret.ldapTimeoutMS);

        var ldapQueryUser = simpleAuthenticationUser;
        executeCommand({setParameter: 1, "ldapQueryUser": ldapQueryUser});
        ret = executeCommand({getParameter: 1, "ldapQueryUser": 1});
        assert.eq(ldapQueryUser,
                  ret.ldapQueryUser,
                  "Unexpected getParameter return for ldapQueryUser: " + ret.ldapQueryUser);

        executeCommand({setParameter: 1, "ldapQueryPassword": "Admin001"});
        ret = executeCommand({getParameter: 1, "ldapQueryPassword": 1});
        assert.eq("###",
                  ret.ldapQueryPassword,
                  "Unexpected getParameter return for ldapQueryPassword: " + ret.ldapQueryPassword);

        var ldapUserToDNMapping =
            "{match: \"(.+)\", substitution: \"cn={0}," + defaultUserDNSuffix + "\"}";
        executeCommand({setParameter: 1, "ldapUserToDNMapping": ldapUserToDNMapping});
        ret = executeCommand({getParameter: 1, "ldapUserToDNMapping": 1});
        assert.eq(
            ldapUserToDNMapping,
            ret.ldapUserToDNMapping,
            "Unexpected getParameter return for ldapUserToDNMapping: " + ret.ldapUserToDNMapping);

        var ldapAuthzQueryTemplate = "{USER}?memberOf";
        executeCommand({setParameter: 1, "ldapAuthzQueryTemplate": ldapAuthzQueryTemplate});
        ret = executeCommand({getParameter: 1, "ldapAuthzQueryTemplate": 1});
        assert.eq(ldapAuthzQueryTemplate,
                  ret.ldapAuthzQueryTemplate,
                  "Unexpected getParameter return for ldapAuthzQueryTemplate: " +
                      ret.ldapAuthzQueryTemplate);

        // Verify that we have a working configuration.
        authAndVerify({conn: conn, options: {authOptions: authOptions, user: adminUser}});
    }

    var configGenerator = new LDAPTestConfigGenerator();

    configGenerator.ldapServers = null;
    configGenerator.ldapAuthzQueryTemplate = "cn={USER}," + defaultUserDNSuffix + "?notMemberOf";
    configGenerator.ldapQueryUser = "badUser";
    configGenerator.ldapQueryPassword = "badPassword";

    runTests(runtimeConfigurationCallback, configGenerator);
})();
