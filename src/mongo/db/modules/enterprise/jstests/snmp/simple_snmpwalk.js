// This test serves as a basic test of mongodb's snmp support. It launches a mongod as an
// snmp-master, then it has snmpwalk connect and retrieve all the variables, and it asserts that
// no assertions occurred

"use strict";

load("src/mongo/db/modules/enterprise/jstests/snmp/lib/snmplib.js");

{
    jsTest.log("====Testing that basic SNMP support works====");

    let conn = MongoRunner.runMongod({"snmp-master": ""});
    run_snmpwalk_test(conn, "127.0.0.1:1161");
    MongoRunner.stopMongod(conn);
}

const deprecationWarningMessage =
    "DeprecationWarning: SNMP is deprecated, and will be removed in a future version.";

{
    jsTest.log(
        "====Testing That Deprecation Warning Is not Displayed when SNMP is not enabled====");
    const conn = MongoRunner.runMongod({});
    try {
        assert(!checkLog.checkContainsOnce(conn, deprecationWarningMessage));
    } finally {
        MongoRunner.stopMongod(conn);
    }
}

{
    jsTest.log("====Testing That Deprecation Warning Is Displayed when SNMP enabled====");
    const conn = MongoRunner.runMongod({"snmp-master": ""});
    try {
        checkLog.contains(conn, deprecationWarningMessage);
    } finally {
        MongoRunner.stopMongod(conn);
    }
}