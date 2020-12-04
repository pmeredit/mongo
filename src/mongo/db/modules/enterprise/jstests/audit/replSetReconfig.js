// This test verifies that replSetReconfig emits audit events.

(function() {
'use strict';
load('src/mongo/db/modules/enterprise/jstests/audit/lib/audit.js');

var m = MongoRunner.runMongodAuditLogger({replSet: "foo"});
var audit = m.auditSpooler();

var admin = m.getDB("admin");
assert.commandWorked(admin.runCommand({replSetInitiate: {}}));

// Verify that the node can accept writes before running the reconfig. If the node
// goes through an election during the reconfig, the reconfig will fail with an
// InterruptedDueToReplStateChange error.
assert.soon(() => admin.runCommand({isMaster: 1}).ismaster);

assert.commandWorked(admin.runCommand({
    replSetReconfig:
        {_id: "foo", version: 2, protocolVersion: 1, members: [{_id: 0, host: m.name}]},
    force: true
}));

const useSecondaryDelaySecs =
    admin.runCommand({getParameter: 1, featureFlagUseSecondaryDelaySecs: 1})
        .featureFlagUseSecondaryDelaySecs.value;
const delayFieldName = useSecondaryDelaySecs ? "secondaryDelaySecs" : "slaveDelay";

audit.assertEntryRelaxed("replSetReconfig", {
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
            [delayFieldName]: {"$numberLong": "0"},
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

MongoRunner.stopMongod(m);
}());
