/**
 * Verify the AWS IAM Auth works by accessing the local ec2 metadata server.
 */

load("src/mongo/db/modules/enterprise/jstests/external_auth/lib/mock_sts.js");
load("src/mongo/db/modules/enterprise/jstests/external_auth/lib/mock_ecs_metadata.js");

(function() {
"use strict";

function runWithEnv(args, env) {
    const pid = _startMongoProgram({args: args, env: env});
    return waitProgram(pid);
}

const mock_sts = new MockSTSServer();
mock_sts.start();

const mock_ecs = new MockECSMetadataServer();
mock_ecs.start();

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

const env = {
    AWS_CONTAINER_CREDENTIALS_RELATIVE_URI: MOCK_AWS_CONTAINER_CREDENTIALS_RELATIVE_URI,

};

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
        '--setShellParameter',
        "awsECSInstanceMetadataUrl=" + mock_ecs.getURL(),
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
        '--setShellParameter',
        "awsECSInstanceMetadataUrl=" + mock_ecs.getURL(),
        "--eval",
        "assert(db.getSiblingDB('$external').auth({mechanism: 'MONGODB-AWS'}))"
    ],
    env);
assert.eq(smoke_auth, 0, "Could not auth with smoke_auth user");

mock_ecs.stop();

mock_sts.stop();

MongoRunner.stopMongod(conn);
}());