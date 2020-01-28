/**
 * Verify the AWS IAM Auth works
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
// assert.commandWorked(admin.adminCommand({setParameter: 1, traceExceptions: 1}));
assert.commandWorked(admin.runCommand({createUser: "admin", pwd: "pwd", roles: ['root']}));
assert(admin.auth("admin", "pwd"));

assert.commandWorked(external.runCommand({createUser: MOCK_AWS_ACCOUNT_ARN, roles: []}));

// Try the command line
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
                              MOCK_AWS_ACCOUNT_ID,
                              '-p',
                              MOCK_AWS_ACCOUNT_SECRET_KEY,
                              "--eval",
                              "1");
assert.eq(smoke, 0, "Could not auth with smoke user");

// Try the auth function
assert(external.auth(
    {user: MOCK_AWS_ACCOUNT_ID, pwd: MOCK_AWS_ACCOUNT_SECRET_KEY, mechanism: 'MONGODB-AWS'}));

mock_sts.stop();

MongoRunner.stopMongod(conn);
}());