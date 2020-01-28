/**
 * Verify the AWS IAM Auth works by accessing the local ec2 metadata server.
 */

load("src/mongo/db/modules/enterprise/jstests/external_auth/lib/mock_sts.js");
load("src/mongo/db/modules/enterprise/jstests/external_auth/lib/mock_ec2_metadata.js");

(function() {
"use strict";

const mock_sts = new MockSTSServer();
mock_sts.start();

const mock_ec2 = new MockEC2MetadataServer();
mock_ec2.start();

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

assert.commandWorked(external.runCommand({createUser: MOCK_AWS_TEMP_ACCOUNT_ARN, roles: []}));

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
                              '--setShellParameter',
                              "awsEC2InstanceMetadataUrl=" + mock_ec2.getURL(),
                              "--eval",
                              "1");
assert.eq(smoke, 0, "Could not auth with smoke user");

// Try via db.auth()
const smoke_auth =
    runMongoProgram("mongo",
                    "--host",
                    "localhost",
                    "--port",
                    conn.port,
                    '--authenticationMechanism',
                    'MONGODB-AWS',
                    '-authenticationDatabase',
                    '$external',
                    '--setShellParameter',
                    "awsEC2InstanceMetadataUrl=" + mock_ec2.getURL(),
                    "--eval",
                    "assert(db.getSiblingDB('$external').auth({mechanism: 'MONGODB-AWS'}))");
assert.eq(smoke_auth, 0, "Could not auth with smoke_auth user");

mock_ec2.stop();

mock_sts.stop();

MongoRunner.stopMongod(conn);
}());