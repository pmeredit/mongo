/**
 * Validate that the server supports real credentials from AWS and can talk to a real AWS STS
 * service
 */
import {
    readSetupJson
} from "src/mongo/db/modules/enterprise/jstests/external_auth_aws/lib/aws_e2e_lib.js";

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

const config = readSetupJson();

assert.commandWorked(
    external.runCommand({createUser: config["iam_auth_ecs_account_arn"], roles: []}));

const testConn = new Mongo(conn.host);
const testExternal = testConn.getDB('$external');
assert(testExternal.auth({
    user: config["iam_auth_ecs_account"],
    pwd: config["iam_auth_ecs_secret_access_key"],
    mechanism: 'MONGODB-AWS'
}));

MongoRunner.stopMongod(conn);
