
import {SelinuxBaseTest} from "jstests/selinux/lib/selinux_base_test.js";

export class TestDefinition extends SelinuxBaseTest {
    get config() {
        const config = super.config;
        config.security = {
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
        };

        config.setParameter = {"authenticationMechanisms": "PLAIN,SCRAM-SHA-1"};
        return config;
    }

    setup() {
        assert.eq(0, this.sudo(`
            set -e
            set -x
            setsebool mongod_can_connect_ldap on
            echo 'LD_PRELOAD=libldap_r.so' >/etc/sysconfig/mongod
        `));
    }

    async run() {
        const {defaultUserDNSuffix, defaultPwd, authAndVerify, runTestsLocal} = await import(
            "src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldap_authz_lib.js");

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
