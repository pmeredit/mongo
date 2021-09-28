// This test serves as a basic test of mongodb's snmp support. It launches a mongod as an
// snmp-master, then it has snmpwalk connect and retrieve all the variables, and it asserts that
// no assertions occurred

"use strict";

load("src/mongo/db/modules/enterprise/jstests/snmp/lib/snmplib.js");

let conn = MongoRunner.runMongod({"snmp-master": ""});
run_snmpwalk_test(conn, "127.0.0.1:1161");
MongoRunner.stopMongod(conn);

jsTest.log("snmp test suite run successfully");
