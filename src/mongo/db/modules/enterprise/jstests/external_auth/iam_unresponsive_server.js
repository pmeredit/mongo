/**
 * Verify the if the IAM server is unresponsive, we timeout correctly.
 */

load("src/mongo/db/modules/enterprise/jstests/external_auth/lib/mock_sts.js");

(function() {
"use strict";

const mock_sts = new MockSTSServer(STS_FAULT_UNRESPONSIVE);
mock_sts.start();

const conn = MongoRunner.runMongod({
    setParameter: {
        "awsSTSUrl": mock_sts.getURL(),
        "authenticationMechanisms": "MONGODB-AWS,SCRAM-SHA-256",
        "awsSTSUseConnectionPool": true,
    },
    auth: "",
});

const external = conn.getDB("$external");
const admin = conn.getDB("admin");
assert.commandWorked(admin.runCommand({createUser: "admin", pwd: "pwd", roles: ['root']}));
assert(admin.auth("admin", "pwd"));

const http_status = admin.adminCommand({serverStatus: 1, http_client: 1});
const http_client = assert.commandWorked(http_status).http_client;
const isCURL = http_client.type === 'curl';

assert.commandWorked(
    external.runCommand({createUser: aws_common.users.permanentUser.simplifiedArn, roles: []}));

admin.logout();

// assert.soon with a timeout of 140 seconds, which is greater than the default timeout of 120
// seconds, to ensure we are timing out correctly. Note that we are not using assert.soonNoExcept --
// If the inner assert fails, we want to fail the test.
assert.soon(() => {
    // Ensure that this request to the IAM server will timeout and not hang forever.
    assert(external.auth({
        user: aws_common.users.permanentUser.id,
        pwd: aws_common.users.permanentUser.secretKey,
        mechanism: 'MONGODB-AWS'
    }) == 0);
    // Ensure that the correct CURL failure is propagated to the connection pool.
    if (isCURL) {
        assert(admin.auth("admin", "pwd"));
        checkLog.containsJson(conn, 22566, {
            'error': 'OperationFailed: Bad HTTP response from API server: Timeout was reached'
        });
        admin.logout();
    }
    return true;
}, "HTTPClient did not timeout in expected interval.", 140000, 1000);

mock_sts.stop();

MongoRunner.stopMongod(conn);
}());
