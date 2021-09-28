// This test serves as a basic test of mongodb's snmp support. It launches a mongod as an
// snmp-master, then it has snmpwalk connect and retrieve all the variables, and it asserts that
// no assertions occurred

'use strict';

load('jstests/selinux/lib/selinux_base_test.js');

class TestDefinition extends SelinuxBaseTest {
    get config() {
        return {
            "systemLog":
                {"destination": "file", "logAppend": true, "path": "/var/log/mongodb/mongod.log"},
            "storage": {"dbPath": "/var/lib/mongo", "journal": {"enabled": true}},
            "processManagement": {
                "fork": true,
                "pidFilePath": "/var/run/mongodb/mongod.pid",
                "timeZoneInfo": "/usr/share/zoneinfo"
            },
            "net": {"port": 27017, "bindIp": "127.0.0.1"},
            "snmp": {"master": true}
        };
    }

    setup() {
        assert.eq(0, this.sudo(`
            set -e
            set -x

            setsebool mongod_can_connect_snmp on

            mkdir -p /var/lib/mongo/.snmp
            cp src/mongo/db/modules/enterprise/docs/mongod.conf.master.selinux /var/lib/mongo/.snmp/mongod.conf
            chown mongod /var/lib/mongo -R
        `));
    }

    run() {
        load("src/mongo/db/modules/enterprise/jstests/snmp/lib/snmplib.js");

        const db = connect("localhost");
        run_snmpwalk_test(db.getMongo(), "tcp:127.0.0.1:1161");
        jsTest.log("snmp test suite ran successfully");
    }
}
