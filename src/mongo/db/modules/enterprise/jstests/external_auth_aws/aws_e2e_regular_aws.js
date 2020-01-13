/**
 * Validate that the server supports real credentials from AWS and can talk to a real AWS STS
 * service
 */
load("src/mongo/db/modules/enterprise/jstests/external_auth_aws/lib/aws_e2e_lib.js");

(function() {
"use strict";

const conn = MongoRunner.runMongod({
    setParameter: {
        "authenticationMechanisms": "MONGODB-IAM,SCRAM-SHA-256",
    },
    auth: "",
});

const external = conn.getDB("$external");
const admin = conn.getDB("admin");
assert.commandWorked(admin.runCommand({createUser: "admin", pwd: "pwd", roles: ['root']}));
assert(admin.auth("admin", "pwd"));

const config = readSetupJson();

assert.commandWorked(
    external.runCommand({createUser: config["iam_auth_ecs_account_arn"], roles: []}));

assert(external.auth({
    user: config["iam_auth_ecs_account"],
    pwd: config["iam_auth_ecs_secret_access_key"],
    mechanism: 'MONGODB-IAM'
}));

MongoRunner.stopMongod(conn);
}());