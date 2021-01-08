// This test verifies that startup options are sent to the audit log
// when mongod or mongos is started.

(function() {
'use strict';

load('src/mongo/db/modules/enterprise/jstests/audit/lib/audit.js');

const options = {
    name: 'startup_audit_sharded',
    mongos: [{
        auth: null,
        auditDestination: 'file',
        auditPath: MongoRunner.dataPath + '/mongos_audit.log',
        auditFormat: 'JSON',
    }],
    config: [{auth: null}],
    shards: [{
        auth: null,
        auditDestination: 'file',
        auditPath: MongoRunner.dataPath + '/mongod_audit.log',
        auditFormat: 'JSON',
    }],
    keyFile: 'jstests/libs/key1',
    other: {shardAsReplicaSet: false}
};
const st = new ShardingTest(options);
const admin = st.s0.getDB('admin');
assert.commandWorked(admin.runCommand({createUser: "admin", pwd: "pwd", roles: ['root']}));
assert(admin.auth("admin", "pwd"));

if (!isImprovedAuditingEnabled(st.s0)) {
    jsTest.log('Skipping test as Improved Auditing is not enabled in this environment');
    st.stop();
    return;
}

const mongosAuditSpooler = new AuditSpooler(options.mongos[0].auditPath, false);
const mongodAuditSpooler = new AuditSpooler(options.shards[0].auditPath, false);

print("Testing mongos audit startup log");
mongosAuditSpooler.assertEntryRelaxed('startup', {
    options: {
        auditLog:
            {destination: 'file', format: 'JSON', path: MongoRunner.dataPath + '/mongos_audit.log'},
        net: {bindIp: '0.0.0.0'},
        security: {keyFile: 'jstests/libs/key1'},
        systemLog: {verbosity: 1}
    }
});
print("SUCCESS mongos audit startup log");

print("Testing mongod audit startup log");
mongodAuditSpooler.assertEntryRelaxed('startup', {
    options: {
        auditLog:
            {destination: 'file', format: 'JSON', path: MongoRunner.dataPath + '/mongod_audit.log'},
        net: {bindIp: '0.0.0.0'},
        security: {keyFile: 'jstests/libs/key1'},
        sharding: {clusterRole: 'shardsvr'},
    }
});
print("SUCCESS mongod audit startup log");

st.stop();
print("SUCCESS audit-startup.js");
}());
