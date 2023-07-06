/**
 * Verify STS failures are logged appropriately.
 */

import {
    aws_common,
    MockSTSServer
} from "src/mongo/db/modules/enterprise/jstests/external_auth/lib/mock_sts.js";

const mock_sts = new MockSTSServer();
mock_sts.start();

const conn = MongoRunner.runMongod({
    setParameter: {
        "awsSTSUrl": mock_sts.getURL() + '/invalid_path',
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

assert(!external.auth({
    user: aws_common.users.permanentUser.id,
    pwd: aws_common.users.permanentUser.secretKey,
    mechanism: 'MONGODB-AWS'
}));

let warning = {};
assert.soon(function() {
    // Find the warning which would have been emitted by the above auth attempt.
    const log = assert.commandWorked(admin.runCommand({getLog: "global"})).log;
    const targetLines = log.map((line) => JSON.parse(line)).filter((line) => line.id === 4690900);

    if (targetLines.length < 0) {
        return false;
    }

    // We should have this warning precisely once.
    assert.eq(targetLines.length, 1);

    warning = targetLines[0];
    return true;
});

// It should complain of a 404 error.
const response = warning.attr.HTTPReply;
assert.eq(response.code, 404);
assert.eq(response.body, 'Unknown URL');

mock_sts.stop();

MongoRunner.stopMongod(conn);
