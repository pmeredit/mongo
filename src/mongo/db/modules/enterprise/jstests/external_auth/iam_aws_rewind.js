/**
 * Verify the AWS IAM Auth can rewind if the remote side closes the connection early
 */

load("src/mongo/db/modules/enterprise/jstests/external_auth/lib/mock_sts.js");

(function() {
"use strict";

const mock_sts = new MockSTSServer(STS_FAULT_CLOSE_ONCE);
mock_sts.start();

const conn = MongoRunner.runMongod({
    setParameter: {
        "awsSTSUrl": mock_sts.getURL(),
        "authenticationMechanisms": "MONGODB-AWS,SCRAM-SHA-256",
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

mock_sts.stop();

MongoRunner.stopMongod(conn);
}());
