/**
 * Verify the AWS IAM EC2 hosted auth works
 */
load("src/mongo/db/modules/enterprise/jstests/external_auth_aws/lib/aws_e2e_lib.js");

(function() {
"use strict";

// This varies based on hosting EC2 as the account id and role name can vary
const AWS_ACCOUNT_ARN = "arn:aws:sts::557821124784:assumed-role/authtest_instance_profile_role/*";

function assignInstanceProfile() {
    const config = readSetupJson();

    const env = {
        AWS_ACCESS_KEY_ID: config["iam_auth_ec2_instance_account"],
        AWS_SECRET_ACCESS_KEY: config["iam_auth_ec2_instance_secret_access_key"],
    };

    const instanceProfileName = config["iam_auth_ec2_instance_profile"];
    const python_command = getPython3Binary() +
        " -u src/mongo/db/modules/enterprise/jstests/external_auth_aws/lib/aws_assign_instance_profile.py" +
        ` --instance_profile_arn=${instanceProfileName}`;

    const ret = runShellCmdWithEnv(python_command, env);
    assert.eq(ret, 0, "Failed to assign an instance profile to the current machine");
}

assignInstanceProfile();

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

assert.commandWorked(external.runCommand({createUser: AWS_ACCOUNT_ARN, roles: []}));

// Try the command line
const smoke = runMongoProgram("mongo",
                              "--host",
                              "localhost",
                              "--port",
                              conn.port,
                              '--authenticationMechanism',
                              'MONGODB-IAM',
                              '--authenticationDatabase',
                              '$external',
                              "--eval",
                              "1");
assert.eq(smoke, 0, "Could not auth with smoke user");

// Try the auth function
assert(external.auth({mechanism: 'MONGODB-IAM'}));

MongoRunner.stopMongod(conn);
}());
