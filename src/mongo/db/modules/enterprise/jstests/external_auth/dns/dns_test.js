// Same test to run in ldap_dns_container
// ldap.mock.mongodb.org is a fake A record to 127.0.0.1 returned by dnsmasq
(function() {
"use strict";

const ldapURLS = [
    "srv:foo.mock.mongodb.org",
    "srv_raw:service.mock.mongodb.org",
    "cname-ldap.mock.mongodb.org",
    "cname-cname-ldap.mock.mongodb.org"
];

for (let ldapURL of ldapURLS) {
    const conn = MongoRunner.runMongod({
        ldapServers: ldapURL,
        ldapBindMethod: "simple",
        ldapTransportSecurity: "none",
        ldapQueryUser: "cn=ldapz_ldap_bind,ou=Users,dc=10gen,dc=cc",
        ldapQueryPassword: "Admin001",
        ldapAuthzQueryTemplate: "{USER}?memberOf",
        ldapUserToDNMapping:
            "[{'match':'(ldapz_ldap1)','substitution':'cn={0},ou=Users,dc=10gen,dc=cc'},{'match':'(ldapz_ldap2)','ldapQuery':'ou=Users,dc=10gen,dc=cc??one?(cn={0})'}]",
    });
    assert.neq(null, conn, 'mongod was unable to start up');

    const smoke = runMongoProgram(
        "mongo", "--host", "local.mock.mongodb.org", "--port", conn.port, "--eval", "1");
    assert.eq(smoke, 0, "Could not connect with mongo");
    MongoRunner.stopMongod(conn);
}
}());
