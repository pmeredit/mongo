function run_snmpwalk_test(conn, address) {
    // Run snmp mapper
    let status = run('snmpwalk', '-V');
    assert.eq(0, status, "snmpwalk not found. status=" + status);

    checkLog.contains(conn, '"msg":"SNMPAgent running as master"', 15000);

    status = run('snmpwalk', '-v', '2c', '-c', 'mongodb', address, '1.3.6.1.4.1.34601');
    assert.eq(0, status, "snmpwalk invocation failed, status=" + status);

    jsTest.log("retrieving mongod log");
    const fullLog = conn.getDB('admin').runCommand({getLog: "global"});

    // Confirms net-snmp is configured correctly.
    // Failure implies that the mongod.conf net-snmp configuration file is missing or misconfigured
    jsTest.log("Check log for missing snmp config");
    let regex = RegExp("no access control information configured");
    for (let i = 0; i < fullLog.log.length; i++) {
        let logLine = fullLog.log[i];
        if (regex.test(logLine)) {
            doassert("net-snmp not configured correctly: " + logLine);
        }
    }

    // Ensures mongod did not log any asserts
    jsTest.log("check log for assertions");
    regex = RegExp("SnmpAgent.*(Assertion|Exception|\"error\")");
    for (let i = 0; i < fullLog.log.length; i++) {
        let logLine = fullLog.log[i];
        if (regex.test(logLine)) {
            doassert("mongod log contains assert during snmpwalk: " + logLine);
        }
    }
}
