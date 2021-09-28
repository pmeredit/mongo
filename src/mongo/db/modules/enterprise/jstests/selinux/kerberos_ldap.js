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
            "storage": {"dbPath": "/var/lib/mongo", "journal": {"enabled": true}},
            "processManagement": {
                "fork": true,
                "pidFilePath": "/var/run/mongodb/mongod.pid",
                "timeZoneInfo": "/usr/share/zoneinfo"
            },
            "net": {
                "port": 27017,
                "bindIp": "0.0.0.0",
                "tls": {
                    "mode": "preferTLS",
                    "certificateKeyFile": "/etc/mongod/server.pem",
                    "CAFile": "/etc/mongod/ca.pem"
                }
            },
            "security": {
                "ldap": {
                    "servers": "ldaptest.10gen.cc",
                    "bind": {
                        "method": "simple",
                        "queryUser": "cn=ldapz_ldap_bind,ou=Users,dc=10gen,dc=cc",
                        "queryPassword": "Admin001"
                    },
                    "transportSecurity": "none",
                    "authz": {"queryTemplate": "{USER}?memberOf"},
                    "userToDNMapping":
                        "[{ \"match\":\"(ldapz_kerberos1)@LDAPTEST.10GEN.CC\", \"substitution\":\"cn={0},ou=Users,dc=10gen,dc=cc\" },{ \"match\":\"(ldapz_kerberos2@LDAPTEST.10GEN.CC)\", \"ldapQuery\":\"ou=Users,dc=10gen,dc=cc??one?krbPrincipalName={0}\" }]"
                }
            },
            "setParameter": {
                "authenticationMechanisms": "GSSAPI,SCRAM-SHA-1",
                "saslHostName": "localhost",
                "saslServiceName": "mockservice"
            }
        };
    }

    setup() {
        assert.eq(0, this.sudo(`
            set -e
            set -x

            setsebool mongod_can_connect_ldap on
            setsebool mongod_can_use_kerberos on

            cp -v src/mongo/db/modules/enterprise/jstests/external_auth/assets/krb5.conf /etc/krb5.conf

            mkdir -p /etc/mongod
            cp -v jstests/libs/{mockservice.keytab,server.pem,ca.pem} /etc/mongod/
            cp -v src/mongo/db/modules/enterprise/jstests/external_auth/assets/{ldapz_ldap_bind.keytab,ldaptest-ca.pem,ldapz_kerberos1.keytab,ldapz_kerberos2.keytab} /etc/mongod/
            chown mongod /etc/mongod -R

            echo 'LD_PRELOAD=libldap_r.so
KRB5_CONFIG=/etc/krb5.conf
KRB5_TRACE=/dev/stdout
KRB5_KTNAME=/etc/mongod/mockservice.keytab
KRB5_CLIENT_KTNAME=/etc/mongod/ldapz_ldap_bind.keytab
LDAPTLS_CACERT=/etc/mongod/ldaptest-ca.pem
LDAPSASL_NOCANON=on' >/etc/sysconfig/mongod

        `));
    }

    run() {
        load('src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldap_authz_lib.js');

        function testGSSAPICallbackLocal({conn}) {
            const user1 = "ldapz_kerberos1@LDAPTEST.10GEN.CC";
            const user2 = "ldapz_kerberos2@LDAPTEST.10GEN.CC";

            run("kdestroy");  // remove any previous tickets
            run("kinit", "-k", "-t", "/etc/mongod/ldapz_kerberos1.keytab", user1);

            let authOptions = {user: user1, mechanism: "GSSAPI", serviceHostname: "localhost"};

            authAndVerify({conn: conn, options: {authOptions: authOptions, user: user1}});

            run("kdestroy");
            run("kinit", "-k", "-t", "/etc/mongod/ldapz_kerberos2.keytab", user2);
            authOptions = {user: user2, mechanism: "GSSAPI", serviceHostname: "localhost"};

            authAndVerify({conn: conn, options: {authOptions: authOptions, user: user2}});
        }

        runTestsLocal(testGSSAPICallbackLocal);
    }
}
