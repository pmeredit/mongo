// Same test to run in ldap_dns_container
// ldap.mock.mongodb.org is a fake A record to 127.0.0.1 returned by dnsmasq
(function() {
"use strict";

const conn = MongoRunner.runMongod({});
assert.neq(null, conn, 'mongod was unable to start up');

const smoke = runMongoProgram("mongo", "--host", "ldap.mock.mongodb.org", "--port", conn.port, "1");
assert.eq(smoke, 0, "Could not connect with mongo");
MongoRunner.stopMongod(conn);
}());
