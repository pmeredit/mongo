/**
 * Verify the AWS IAM Auth can rewind if the remote side closes the connection early
 */

import {
    aws_common,
    MockSTSServer,
    STS_FAULT_CLOSE_ONCE
} from "src/mongo/db/modules/enterprise/jstests/external_auth/lib/mock_sts.js";

// Curl is only available on Linux & macOS
if (_isWindows()) {
    quit();
}

const mock_sts = new MockSTSServer(STS_FAULT_CLOSE_ONCE);
mock_sts.start();

const conn = MongoRunner.runMongod({
    setParameter: {
        "awsSTSUrl": mock_sts.getURL(),
        "authenticationMechanisms": "MONGODB-AWS,SCRAM-SHA-256",
        "awsSTSUseConnectionPool": true,
        "httpVerboseLogging": true,
        "logComponentVerbosity": '{"network":{"verbosity":1}}',
    },
    auth: "",
});

const external = conn.getDB("$external");
const admin = conn.getDB("admin");
assert.commandWorked(admin.runCommand({createUser: "admin", pwd: "pwd", roles: ['root']}));
assert(admin.auth("admin", "pwd"));

assert.commandWorked(
    external.runCommand({createUser: aws_common.users.permanentUser.simplifiedArn, roles: []}));
admin.logout();

// Make connection
assert(external.auth({
    user: aws_common.users.permanentUser.id,
    pwd: aws_common.users.permanentUser.secretKey,
    mechanism: 'MONGODB-AWS'
}));
external.logout();

// Force rewind
assert(external.auth({
    user: aws_common.users.permanentUser.id,
    pwd: aws_common.users.permanentUser.secretKey,
    mechanism: 'MONGODB-AWS'
}));
external.logout();

// Try a normal third time
assert(external.auth({
    user: aws_common.users.permanentUser.id,
    pwd: aws_common.users.permanentUser.secretKey,
    mechanism: 'MONGODB-AWS'
}));
external.logout();

assert(admin.auth("admin", "pwd"));

// Check for Curl message
checkLog.checkContainsOnce(conn, "state.rewindbeforesend = TRUE");

mock_sts.stop();

MongoRunner.stopMongod(conn);
