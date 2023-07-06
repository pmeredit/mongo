/**
 * Verify the AWS IAM Auth gracefully retries when receiving a 5xx error.
 */

import {
    aws_common,
    MockSTSServer,
    STS_FAULT_500_ONCE
} from "src/mongo/db/modules/enterprise/jstests/external_auth/lib/mock_sts.js";

const mock_sts = new MockSTSServer(STS_FAULT_500_ONCE);
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

conn.setLogLevel(5, 'accessControl');
admin.logout();

assert(external.auth({
    user: aws_common.users.permanentUser.id,
    pwd: aws_common.users.permanentUser.secretKey,
    mechanism: 'MONGODB-AWS'
}));
external.logout();

assert(admin.auth("admin", "pwd"));
conn.setLogLevel(0, 'accessControl');

const kAWSSTSRetryLog = 6205300;
const retryLog =
    checkLog.getGlobalLog(conn).map((l) => JSON.parse(l)).filter((l) => l.id === kAWSSTSRetryLog);
jsTest.log(retryLog);
assert.eq(retryLog.length, 1, "Unexpected number of retry entries");
assert.eq(retryLog[0].attr.code, 500, "Unexpected retriable code");
assert.eq(retryLog[0].attr.body, "Something went wrong.", "Unexpected server error message");
assert.eq(retryLog[0].attr.willRetry, true, "Unexpected retry state");

mock_sts.stop();

MongoRunner.stopMongod(conn);
