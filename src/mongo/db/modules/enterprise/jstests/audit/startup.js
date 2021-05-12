// This test verifies that startup options are sent to the audit log
// when mongod or mongos is started.

(function() {
'use strict';

load('src/mongo/db/modules/enterprise/jstests/audit/lib/audit.js');

const st = MongoRunner.runShardedClusterAuditLogger({keyFile: 'jstests/libs/key1'});
const admin = st.s0.getDB('admin');
assert.commandWorked(admin.runCommand({createUser: "admin", pwd: "pwd", roles: ['root']}));
assert(admin.auth("admin", "pwd"));

const runTest = function(conn) {
    conn.auditSpooler().assertEntryOptionsRelaxed('startup', {
        auditLog: {
            destination: conn.fullOptions.auditDestination,
            format: conn.fullOptions.auditFormat,
            path: conn.fullOptions.auditPath
        },
        net: {bindIp: '0.0.0.0'},
        security: {keyFile: 'jstests/libs/key1'},
    });
};

jsTest.log("Testing mongos audit startup log");
runTest(st.s0);
jsTest.log("SUCCESS mongos audit startup log");

jsTest.log("Testing mongod audit startup log");
runTest(st.rs0.nodes[0]);
jsTest.log("SUCCESS mongod audit startup log");

st.stop();
}());