/**
 * Verify the AWS IAM Auth works with temporary credentials
 */

load("src/mongo/db/modules/enterprise/jstests/external_auth/lib/mock_sts.js");

(function() {
"use strict";

const mock_sts = new MockSTSServer();
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
    external.runCommand({createUser: aws_common.users.tempUser.simplifiedArn, roles: []}));

const smoke = runMongoProgram("mongo",
                              "--host",
                              "localhost",
                              "--port",
                              conn.port,
                              '--authenticationMechanism',
                              'MONGODB-AWS',
                              '--authenticationDatabase',
                              '$external',
                              '-u',
                              aws_common.users.tempUser.id,
                              '-p',
                              aws_common.users.tempUser.secretKey,
                              '--awsIamSessionToken',
                              aws_common.users.tempUser.sessionToken,
                              "--eval",
                              "1");
assert.eq(smoke, 0, "Could not auth with smoke user");

assert((new Mongo(conn.host)).getDB('$external').auth({
    user: aws_common.users.tempUser.id,
    pwd: aws_common.users.tempUser.secretKey,
    awsIamSessionToken: aws_common.users.tempUser.sessionToken,
    mechanism: 'MONGODB-AWS'
}));

mock_sts.stop();

MongoRunner.stopMongod(conn);
}());
