// This test serves as a basic test of mongodb's snmp support. It launches a mongod as an
// snmp-master, then it has snmpwalk connect and retrieve all the variables, and it asserts that
// no assertions occurred

"use strict";

var conn = MongoRunner.runMongod({"snmp-master": ""});

// Run snmp mapper
var status;

status = run('snmpwalk', '--version');
assert.eq(0, status, "snmpwalk not found. status="+ status);

status = run('snmpwalk', '-v', '2c', '-c',
             'mongodb', '127.0.0.1:1161', '1.3.6.1.4.1.34601');
assert.eq(0, status, "snmpwalk invocation failed, status="+ status);

print("retrieving mongod log");
var fullLog = conn.getDB('admin').runCommand({getLog: "global"});

// Confirms net-snmp is configured correctly.
// Failure implies that the mongod.conf net-snmp configuration file is missing or misconfigured
print("Check log for missing snmp config");
var regex = RegExp("no access control information configured");
for (var i=0; i<fullLog.log.length; i++) {
    var logLine = fullLog.log[i];
    if (regex.test(logLine)) {
        doassert("net-snmp not configured correctly: "+ logLine);
    }
}

// Ensures mongod did not log any asserts
print("check log for assertions");
var regex = RegExp("assert", "i");
for (var i=0; i<fullLog.log.length; i++) {
    var logLine = fullLog.log[i];
    if (regex.test(logLine)) {
        doassert("mongod log contains assert during snmpwalk: "+ logLine);
    }
}

MongoRunner.stopMongod(conn);
print("snmp test suite run successfully");
