/**
 * Verify the AWS IAM Auth gracefully fails with AWS returns 403
 */

import {
    aws_common,
    MockSTSServer,
    STS_FAULT_403
} from "src/mongo/db/modules/enterprise/jstests/external_auth/lib/mock_sts.js";

const mock_sts = new MockSTSServer(STS_FAULT_403);
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

assert(external.auth({
    user: aws_common.users.permanentUser.id,
    pwd: aws_common.users.permanentUser.secretKey,
    mechanism: 'MONGODB-AWS'
}) == 0);

mock_sts.stop();

MongoRunner.stopMongod(conn);