/**
 * Validate that MONGODB-AWS auth works from ECS temporary credentials.
 */
load("src/mongo/db/modules/enterprise/jstests/external_auth_aws/lib/aws_e2e_lib.js");

(function() {
"use strict";

if (_isWindows()) {
    return;
}

function isUbuntu1804() {
    if (_isWindows()) {
        return false;
    }

    const grep_result = runProgram('grep', 'UBUNTU_CODENAME=bionic', '/etc/os-release');
    if (grep_result === 0) {
        return true;
    }

    return false;
}

// Since the container is Ubuntu 18.04, it does to make sense to run binaries from other distros on
// it.
if (!isUbuntu1804()) {
    return;
}

const config = readSetupJson();

const lib_dir = "src/mongo/db/modules/enterprise/jstests/external_auth_aws/lib";
const container_tester = `${lib_dir}/container_tester.py`;
const base_command = getPython3Binary() + ` -u  ${container_tester}`;

const run_prune_command = base_command + " -v remote_gc_services " +
    " --cluster " + config["iam_auth_ecs_cluster"];

const run_test_command = base_command + " -d -v run_e2e_test" +
    " --cluster " + config["iam_auth_ecs_cluster"] + " --task_definition " +
    config["iam_auth_ecs_task_definition"] + " --subnets " + config["iam_auth_ecs_subnet_a"] +
    " --subnets " + config["iam_auth_ecs_subnet_b"] + " --security_group " +
    config["iam_auth_ecs_security_group"] +
    " --files dist-test/bin/mongod:/root/mongod dist-test/bin/mongo:/root/mongo " +
    ` ${lib_dir}/ecs_hosted_test.js:/root/ecs_hosted_test.js ` +
    ` --script ${lib_dir}/ecs_hosted_test.sh`;

// Pass in the AWS credentials as environment variables
// AWS_SHARED_CREDENTIALS_FILE does not work in evergreen for an unknown reason
const env = {
    AWS_ACCESS_KEY_ID: config["iam_auth_ecs_account"],
    AWS_SECRET_ACCESS_KEY: config["iam_auth_ecs_secret_access_key"],
};

// Prune other containers
let ret = runWithEnv(['/bin/sh', '-c', run_prune_command], env);
assert.eq(ret, 0, "Prune Container failed");

// Run the test in a container
ret = runWithEnv(['/bin/sh', '-c', run_test_command], env);
assert.eq(ret, 0, "Container Test failed");
}());
