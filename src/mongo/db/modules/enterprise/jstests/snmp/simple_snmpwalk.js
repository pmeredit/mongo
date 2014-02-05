// This test serves as a basic test of mongodb's snmp support. It launches a mongod as an
// snmp-master, then it has snmpwalk connect and retrieve all the variables, and it asserts that
// no assertions occurred
var conn = MongoRunner.runMongod({"snmp-master": ""});
// run snmp mapper
run('snmpwalk', '-m', 'src/mongo/db/modules/enterprise/docs/MONGOD-MIB.txt', '-v', '2c', '-c',
    'mongodb', '127.0.0.1:1161', '1.3.6.1.4.1.34601');
// ensure mongod did not log any asserts
var fullLog = conn.getDB('admin').runCommand({getLog: "global"});
var asserted = false;
var regex = RegExp("assert", "i");
for (i=0; i<fullLog.length; i++) {
    if (regex.test(fullLog[i])) {
        asserted = true;
    }
}
assert.eq(asserted, false, "mongod asserted while snmpmapper was running");
MongoRunner.stopMongod(conn);
print("snmp test suite run successfully");
