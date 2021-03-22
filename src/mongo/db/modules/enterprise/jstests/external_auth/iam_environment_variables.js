/**
 * Verify the AWS IAM Auth works by using environment variables.
 *
 * Environment variables are used by AWS lambda.
 */

load("src/mongo/db/modules/enterprise/jstests/external_auth/lib/mock_sts.js");

(function() {
"use strict";

function runWithEnv(args, env) {
    const pid = _startMongoProgram({args: args, env: env});
    return waitProgram(pid);
}

function testAuthWithEnv(env) {
    // Try the command line
    const smoke = runWithEnv(
        [
            "mongo",
            "--host",
            "localhost",
            "--port",
            conn.port,
            '--authenticationMechanism',
            'MONGODB-AWS',
            '--authenticationDatabase',
            '$external',
            "--eval",
            "1"
        ],
        env);
    assert.eq(smoke, 0, "Could not auth with smoke user");

    // // Try via db.auth()
    const smoke_auth = runWithEnv(
        [
            "mongo",
            "--host",
            "localhost",
            "--port",
            conn.port,
            '-authenticationDatabase',
            '$external',
            "--eval",
            "assert(db.getSiblingDB('$external').auth({mechanism: 'MONGODB-AWS'}))"
        ],
        env);
    assert.eq(smoke_auth, 0, "Could not auth with smoke_auth user");
}

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
    external.runCommand({createUser: aws_common.users.permanentUser.simplifiedArn, roles: []}));

// Test with regular creds
testAuthWithEnv({
    AWS_ACCESS_KEY_ID: aws_common.users.permanentUser.id,
    AWS_SECRET_ACCESS_KEY: aws_common.users.permanentUser.secretKey,
});

assert.commandWorked(
    external.runCommand({createUser: aws_common.users.tempUser.simplifiedArn, roles: []}));

// Test with temporary creds
testAuthWithEnv({
    AWS_ACCESS_KEY_ID: aws_common.users.tempUser.id,
    AWS_SECRET_ACCESS_KEY: aws_common.users.tempUser.secretKey,
    AWS_SESSION_TOKEN: aws_common.users.tempUser.sessionToken,
});

mock_sts.stop();

MongoRunner.stopMongod(conn);
}());
