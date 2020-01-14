/**
 * Verify the AWS IAM Auth works with temporary credentials from sts:AssumeRole
 */

load("src/mongo/db/modules/enterprise/jstests/external_auth_aws/lib/aws_e2e_lib.js");

(function() {
"use strict";

const ASSUMED_ROLE = "arn:aws:sts::557821124784:assumed-role/authtest_user_assume_role/*";

function getAssumeCredentials() {
    const config = readSetupJson();

    const env = {
        AWS_ACCESS_KEY_ID: config["iam_auth_assume_aws_account"],
        AWS_SECRET_ACCESS_KEY: config["iam_auth_assume_aws_secret_access_key"],
    };

    const role_name = config["iam_auth_assume_role_name"];

    const python_command = getPython3Binary() +
        " -u src/mongo/db/modules/enterprise/jstests/external_auth_aws/lib/aws_assume_role.py" +
        ` --role_name=${role_name} > creds.json`;

    const ret = runShellCmdWithEnv(python_command, env);
    assert.eq(ret, 0, "Failed to assume role on the current machine");

    const result = cat("creds.json");
    try {
        return JSON.parse(result);
    } catch (e) {
        jsTestLog("Failed to parse: " + result + "\n" + result);
        throw e;
    }
}

const credentials = getAssumeCredentials();

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

assert.commandWorked(external.runCommand({createUser: ASSUMED_ROLE, roles: []}));

assert(external.auth({
    user: credentials["AccessKeyId"],
    pwd: credentials["SecretAccessKey"],
    awsIamSessionToken: credentials["SessionToken"],
    mechanism: 'MONGODB-IAM'
}));

MongoRunner.stopMongod(conn);
}());
