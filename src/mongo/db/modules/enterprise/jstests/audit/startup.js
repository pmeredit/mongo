/**
 * This test verifies that startup options are sent to the audit log. It also checks that
 * cluster server parameters are properly logged at startup after being initialized from disk.
 * @tags: [
 *   # Requires all nodes to be running the latest binary.
 *   requires_fcv_61,
 *   featureFlagClusterWideConfigM2,
 *   does_not_support_stepdowns,
 *   requires_replication,
 *   requires_sharding
 *  ]
 */

(function() {
'use strict';

load('src/mongo/db/modules/enterprise/jstests/audit/lib/audit.js');

let st = MongoRunner.runShardedClusterAuditLogger({keyFile: 'jstests/libs/key1'});
const admin = st.s0.getDB('admin');
assert.commandWorked(admin.runCommand({createUser: "admin", pwd: "pwd", roles: ['root']}));
assert(admin.auth("admin", "pwd"));

const runTest = function(conn, expectedIntData) {
    conn.auditSpooler().assertEntryRelaxed('startup', {
        startupOptions: {
            auditLog: {
                destination: conn.fullOptions.auditDestination,
                format: conn.fullOptions.auditFormat,
                path: conn.fullOptions.auditPath
            },
            net: {bindIp: '0.0.0.0'},
            security: {keyFile: 'jstests/libs/key1'},
        },
        initialClusterServerParameters: [
            {
                _id: "changeStreamOptions",
                preAndPostImages: {
                    expireAfterSeconds: "off",
                },
            },
            {
                _id: "testStrClusterParameter",
                strData: "off",
            },
            {
                _id: "testIntClusterParameter",
                intData: NumberLong(expectedIntData),
            },
            {
                _id: "testBoolClusterParameter",
                boolData: false,
            },
        ]
    });
};

const restartShardedClusterSpooler = function(st) {
    st.stop();
    return MongoRunner.runShardedClusterAuditLogger({keyFile: 'jstests/libs/key1'});
};

jsTest.log("Testing mongos audit startup log");
runTest(st.s0, 16);
jsTest.log("SUCCESS mongos audit startup log");

jsTest.log("Testing mongod audit startup log");
runTest(st.rs0.nodes[0], 16);
jsTest.log("SUCCESS mongod audit startup log");

// Set a cluster parameter and restart the sharded cluster, then check that the audit log on restart
// shows the updated value on the mongods.
assert.commandWorked(
    admin.runCommand({setClusterParameter: {testIntClusterParameter: {intData: 256}}}));
st = restartShardedClusterSpooler(st);

jsTest.log("Testing mongod audit startup log after restart");
runTest(st.rs0.nodes[0], 256);
jsTest.log('SUCCESS mongod audit startup log after restart');

st.stop();
}());
