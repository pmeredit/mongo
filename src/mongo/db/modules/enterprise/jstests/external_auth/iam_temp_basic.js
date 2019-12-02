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
        "authenticationMechanisms": "MONGODB-IAM,SCRAM-SHA-256",
    },
    auth: "",
});

const external = conn.getDB("$external");
const admin = conn.getDB("admin");
assert.commandWorked(admin.runCommand({createUser: "admin", pwd: "pwd", roles: ['root']}));
assert(admin.auth("admin", "pwd"));

assert.commandWorked(external.runCommand({createUser: MOCK_AWS_TEMP_ACCOUNT_ARN, roles: []}));

const smoke = runMongoProgram("mongo",
                              "--host",
                              "localhost",
                              "--port",
                              conn.port,
                              '--authenticationMechanism',
                              'MONGODB-IAM',
                              '--authenticationDatabase',
                              '$external',
                              '-u',
                              MOCK_AWS_TEMP_ACCOUNT_ID,
                              '-p',
                              MOCK_AWS_TEMP_ACCOUNT_SECRET_KEY,
                              '--awsIamSessionToken',
                              MOCK_AWS_TEMP_ACCOUNT_SESSION_TOKEN,
                              "--eval",
                              "1");
assert.eq(smoke, 0, "Could not auth with smoke user");

assert(external.auth({
    user: MOCK_AWS_TEMP_ACCOUNT_ID,
    pwd: MOCK_AWS_TEMP_ACCOUNT_SECRET_KEY,
    awsIamSessionToken: MOCK_AWS_TEMP_ACCOUNT_SESSION_TOKEN,
    mechanism: 'MONGODB-IAM'
}));

mock_sts.stop();

MongoRunner.stopMongod(conn);
}());