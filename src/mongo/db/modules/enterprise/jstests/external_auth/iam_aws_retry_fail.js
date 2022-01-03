/**
 * Verify the AWS IAM Auth gracefully retries when receiving a 5xx error,
 * and eventually gives up.
 */

load("src/mongo/db/modules/enterprise/jstests/external_auth/lib/mock_sts.js");

(function() {
"use strict";

const mock_sts = new MockSTSServer(STS_FAULT_500);
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
}) == 0);

assert(admin.auth("admin", "pwd"));
conn.setLogLevel(0, 'accessControl');

const kAWSSTSRetryLog = 6205300;
const retryLog =
    checkLog.getGlobalLog(conn).map((l) => JSON.parse(l)).filter((l) => l.id === kAWSSTSRetryLog);
jsTest.log(retryLog);
assert.eq(retryLog.length, 3, "Unexpected number of retry entries");
for (let i = 0; i < 3; ++i) {
    const entry = retryLog[i].attr;
    assert.eq(entry.code, 500, "Unexpected retriable code");
    assert.eq(entry.body, "Something went wrong.", "Unexpected server error message");
    assert.eq(entry.willRetry, (i < 2), "Unexpected retry state");
}

mock_sts.stop();

MongoRunner.stopMongod(conn);
}());
