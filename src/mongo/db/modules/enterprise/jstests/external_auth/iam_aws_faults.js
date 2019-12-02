/**
 * Verify the AWS IAM Auth gracefully fails with AWS returns 403
 */

load("src/mongo/db/modules/enterprise/jstests/external_auth/lib/mock_sts.js");

(function() {
"use strict";

const mock_sts = new MockSTSServer(STS_FAULT_403);
mock_sts.start();

const conn = MongoRunner.runMongod({
    setParameter: {
        "awsSTSUrl": mock_sts.getURL(),
        "authenticationMechanisms": "MONGODB-IAM,SCRAM-SHA-256",
    },
    auth: "",
});

const external = conn.getDB("$external");
const admin = conn.getDB("admin");
assert.commandWorked(admin.runCommand({createUser: "admin", pwd: "pwd", roles: ['root']}));
assert(admin.auth("admin", "pwd"));

assert.commandWorked(external.runCommand({createUser: MOCK_AWS_ACCOUNT_ARN, roles: []}));

assert(
    external.auth(
        {user: MOCK_AWS_ACCOUNT_ID, pwd: MOCK_AWS_ACCOUNT_SECRET_KEY, mechanism: 'MONGODB-IAM'}) ==
    0);

mock_sts.stop();

MongoRunner.stopMongod(conn);
}());