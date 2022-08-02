
'use strict';

load('jstests/selinux/lib/selinux_base_test.js');

class TestDefinition extends SelinuxBaseTest {
    get config() {
        return {
            "systemLog": {
                "destination": "file",
                "logAppend": true,
                "path": "/var/log/mongodb/mongod.log",
                "verbosity": 0
            },
            "storage": {"dbPath": "/var/lib/mongo"},
            "processManagement": {
                "fork": true,
                "pidFilePath": "/var/run/mongodb/mongod.pid",
                "timeZoneInfo": "/usr/share/zoneinfo"
            },
            "net": {"port": 27017, "bindIp": "127.0.0.1"},
            "security": {
                "ldap": {
                    "servers": "ldaptest.10gen.cc",
                    "bind": {
                        "method": "simple",
                        "queryUser": "cn=ldapz_ldap_bind,ou=Users,dc=10gen,dc=cc",
                        "queryPassword": "Admin001"
                    },
                    "transportSecurity": "none",
                    "authz": {"queryTemplate": "{USER}?memberOf"}
                }
            },
            "setParameter": {"authenticationMechanisms": "PLAIN,SCRAM-SHA-1"}
        };
    }

    setup() {
        assert.eq(0, this.sudo(`
            set -e
            set -x
            setsebool mongod_can_connect_ldap on
            echo 'LD_PRELOAD=libldap_r.so' >/etc/sysconfig/mongod
        `));
    }

    run() {
        load('src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldap_authz_lib.js');

        function ldapTestCallbackUsernameIsDN({conn}) {
            var user1 = "cn=ldapz_ldap1," + defaultUserDNSuffix;
            var user2 = "cn=ldapz_ldap2," + defaultUserDNSuffix;

            var authOptions =
                {user: user1, pwd: defaultPwd, mechanism: "PLAIN", digestPassword: false};

            authAndVerify({conn: conn, options: {authOptions: authOptions, user: user1}});

            authOptions = {user: user2, pwd: defaultPwd, mechanism: "PLAIN", digestPassword: false};

            authAndVerify({conn: conn, options: {authOptions: authOptions, user: user2}});
        }

        runTestsLocal(ldapTestCallbackUsernameIsDN);
        jsTest.log("LDAP test suite ran successfully");
    }
}
