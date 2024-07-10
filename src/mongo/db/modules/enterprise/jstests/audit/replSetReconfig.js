// This test verifies that replSetReconfig emits audit events.
import "src/mongo/db/modules/enterprise/jstests/audit/lib/audit.js";

function runTest(m, assertion) {
    const admin = m.getDB("admin");
    assert.commandWorked(admin.runCommand({replSetInitiate: {}}));

    // Verify that the node can accept writes before running the reconfig. If the node
    // goes through an election during the reconfig, the reconfig will fail with an
    // InterruptedDueToReplStateChange error.
    assert.soon(() => admin.runCommand({hello: 1}).isWritablePrimary);

    assert.commandWorked(admin.runCommand({
        replSetReconfig:
            {_id: "foo", version: 2, protocolVersion: 1, members: [{_id: 0, host: m.name}]},
        force: true
    }));

    assertion("replSetReconfig", {
        "old": {
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
                "secondaryDelaySecs": {"$numberLong": "0"},
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
        },
        "new": {
            "_id": "foo",
            "protocolVersion": {"$numberLong": "1"},
            "members": [{"_id": 0, "host": m.name}]
        }
    });
}

{
    jsTest.log('schema === mongo');
    const m = MongoRunner.runMongodAuditLogger({replSet: "foo"});
    const audit = m.auditSpooler();
    runTest(m, function(atype, params) {
        audit.assertEntryRelaxed(atype, params);
    });
    MongoRunner.stopMongod(m);
}

{
    const kDiscovery = 5;
    const kDeviceConfigState = 5002;
    const kDeviceConfigStateLog = 1;

    jsTest.log('schema === OCSF');
    const m = MongoRunner.runMongodAuditLogger({replSet: "foo"}, 'JSON', 'ocsf');
    const audit = m.auditSpooler();
    runTest(m, function(atype, params) {
        const expect = Object.assign({atype: atype}, params);
        const entry = audit.assertEntry(kDiscovery, kDeviceConfigState, kDeviceConfigStateLog);
        assert(audit.deepPartialEquals(entry.unmapped, expect));
    });
    MongoRunner.stopMongod(m);
}
