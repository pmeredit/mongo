// Library functions used by oidc e2e tests.

const kExpectedAuthSuccessLogId = 5286306;

// Authenticates as userName, verifies that the expected name and roles appear via connectionStatus,
// and calls the registered callback to verify that the connection can perform operations
// corresponding to the role mapping.
export function authAsUser(conn, userName, authNamePrefix, expectedRoles, authzCheckCallback) {
    const externalDB = conn.getDB('$external');
    assert(externalDB.auth({user: userName, mechanism: 'MONGODB-OIDC'}));
    const connStatus = assert.commandWorked(conn.adminCommand({connectionStatus: 1}));
    assert.eq(connStatus.authInfo.authenticatedUsers.length, 1);
    assert.eq(connStatus.authInfo.authenticatedUsers[0].db, '$external');
    assert.eq(connStatus.authInfo.authenticatedUsers[0].user, authNamePrefix + '/' + userName);
    const sortedActualRoles =
        connStatus.authInfo.authenticatedUserRoles.map((r) => r.db + '.' + r.role).sort();
    const sortedExpectRoles = expectedRoles.sort();
    assert.eq(sortedActualRoles, sortedExpectRoles);
    authzCheckCallback();
    externalDB.logout();
}

// Authenticates as userName and then sleeps for sleep_time (configured expiry time of tokens).
// Operations should still succeed after the expiry without ErrorCodes.ReauthenticationRequired
// thanks to the shell's built-in refresh flow.
export function runRefreshFlowTest(conn, userName, expected_security_one_auth_log, sleep_time) {
    const externalDB = conn.getDB('$external');
    assert(externalDB.auth({user: userName, mechanism: 'MONGODB-OIDC'}));
    assert.commandWorked(conn.adminCommand({oidcListKeys: 1}));

    // In a parallel shell, assert that there have already been 2 auth logs as
    // the user before token expiry.
    const adminShell = new Mongo(conn.host);
    const parallelShellAdminDB = adminShell.getDB('admin');
    assert(parallelShellAdminDB.auth('admin', 'pwd'));
    checkLog.containsRelaxedJson(
        adminShell, kExpectedAuthSuccessLogId, expected_security_one_auth_log, 2);

    // Wait for access token expiration...
    sleep(sleep_time);

    // The updated configuration will result in ErrorCodes.ReauthenticationRequired from the server.
    // However, the shell should implicitly reauth via the refresh flow and then successfully retry
    // the command. We check the logs for proof of that implicit reauth.
    assert.commandWorked(conn.adminCommand({oidcListKeys: 1}));
    checkLog.containsRelaxedJson(
        adminShell, kExpectedAuthSuccessLogId, expected_security_one_auth_log, 3);
    externalDB.logout();
}
