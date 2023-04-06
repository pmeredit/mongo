/**
 * Verify the AWS IAM EC2 hosted auth works
 */
load("src/mongo/db/modules/enterprise/jstests/external_auth_aws/lib/aws_e2e_lib.js");

(function() {
"use strict";

// This varies based on hosting EC2 as the account id and role name can vary
const AWS_ACCOUNT_ARN =
    "arn:aws:sts::557821124784:assumed-role/evergreen_task_hosts_instance_role_production/*";

const conn = MongoRunner.runMongod({
    setParameter: {
        "authenticationMechanisms": "MONGODB-AWS,SCRAM-SHA-256",
    },
    auth: "",
});

const external = conn.getDB("$external");
const admin = conn.getDB("admin");

assert.commandWorked(admin.runCommand({createUser: "admin", pwd: "pwd", roles: ['root']}));
assert(admin.auth("admin", "pwd"));

assert.commandWorked(external.runCommand({createUser: AWS_ACCOUNT_ARN, roles: []}));

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
                              "--eval",
                              "1");
assert.eq(smoke, 0, "Could not auth with smoke user");

// Try the auth function
const testConn = new Mongo(conn.host);
const testExternal = testConn.getDB('$external');
assert(testExternal.auth({mechanism: 'MONGODB-AWS'}));

MongoRunner.stopMongod(conn);
}());
