// This test verifies that replSetReconfig emits audit events.

(function() {
    'use strict';
    load('src/mongo/db/modules/enterprise/jstests/audit/audit.js');

    var m = MongoRunner.runMongodAuditLogger({replSet: "foo"});
    var audit = m.auditSpooler();

    var admin = m.getDB("admin");
    assert.commandWorked(admin.runCommand({replSetInitiate: {}}));

    assert.commandWorked(admin.runCommand({
        replSetReconfig:
            {_id: "foo", version: 2, protocolVersion: 1, members: [{_id: 0, host: m.name}]},
        force: true
    }));

    audit.assertEntryRelaxed("replSetReconfig", {
        "old": {"_id": "foo", "protocolVersion": 1, "members": [{"_id": 0, "host": m.name}]},
        "new": {
            "_id": "foo",
            "version": 1,
            "protocolVersion": {"$numberLong": "1"},
            "writeConcernMajorityJournalDefault": true,
            "members": [{
                "_id": 0,
                "host": m.name,
                "arbiterOnly": false,
                "buildIndexes": true,
                "hidden": false,
                "priority": 1,
                "tags": {},
                "slaveDelay": {"$numberLong": "0"},
                "votes": 1
            }],
            "settings": {
                "chainingAllowed": true,
                "heartbeatIntervalMillis": 2000,
                "heartbeatTimeoutSecs": 10,
                "electionTimeoutMillis": 10000,
                "catchUpTimeoutMillis": -1,
                "catchUpTakeoverDelayMillis": 30000,
                "getLastErrorModes": {},
                "getLastErrorDefaults": {"w": 1, "wtimeout": 0}
            }
        }
    });

    MongoRunner.stopMongod(m);
}());
