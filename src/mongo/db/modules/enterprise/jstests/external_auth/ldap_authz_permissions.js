// Tests the permissions of roles matched by LDAP authorization are honored.

(function() {
    load("src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldap_authz_lib.js");

    function testPermissions(m) {
        var authInfo =
            {mechanism: "PLAIN", user: adminUserDN, pwd: defaultPwd, digestPassword: false};

        var testDB = m.getDB("test");
        var externalDB = m.getDB("$external");

        // LDAP auth should succeed
        assert(externalDB.auth(authInfo), "LDAP authentication failed");
        assert.writeOK(testDB.test.insert({a: 1}), "No write permission after authorization");
        assert.writeError(testDB.getSiblingDB("should_fail").test.insert({a: 1}),
                          "Write permission in wrong DB after authorization");
        assert.throws(function() {
            testDB.test.find();
        }, [], "Have read permission after write-only authorization");

        externalDB.logout();
        assert.writeError(testDB.test.insert({a: 1}), "can write after logout");

        // internal authn should fail for the same user
        assert(!testDB.getSiblingDB("admin").auth(authInfo), "internal authn succeeded");
    }

    var configGenerator = new LDAPTestConfigGenerator();
    configGenerator.ldapAuthzQueryTemplate = "{USER}?memberOf";
    runTests(testPermissions, configGenerator);
})();
