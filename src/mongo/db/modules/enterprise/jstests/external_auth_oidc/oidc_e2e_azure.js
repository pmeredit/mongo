// Tests end-to-end OIDC authentication with Azure. Shell uses device authorization grant flow to
// acquire tokens.
// @tags: [ requires_fcv_70 ]

(function() {
'use strict';

load("jstests/libs/python.js");
load('jstests/ssl/libs/ssl_helpers.js');

if (determineSSLProvider() !== 'openssl') {
    print('Skipping test, OIDC is only available with OpenSSL');
    return;
}

const kAzureConfig = [{
    issuer: 'https://login.microsoftonline.com/c96563a8-841b-4ef9-af16-33548de0c958/v2.0',
    audience: '67b54975-2545-4fad-89cf-1dbd174d2e60',
    authNamePrefix: 'issuerAzure',
    matchPattern: '@outlook.com$',
    clientId: '67b54975-2545-4fad-89cf-1dbd174d2e60',
    requestScopes: ['api://67b54975-2545-4fad-89cf-1dbd174d2e60/email'],
    principalName: 'preferred_username',
    authorizationClaim: 'groups',
    logClaims: ['sub', 'aud', 'groups'],
    JWKSPollSecs: 86400,
}];
const kAzureStartupOptions = {
    authenticationMechanisms: 'SCRAM-SHA-256,MONGODB-OIDC',
    logComponentVerbosity: tojson({accessControl: 3}),
    oidcIdentityProviders: tojson(kAzureConfig),
};
const kExpectedTestServerSecurityOneRoles = [
    'admin.issuerAzure/21c0bd78-e5a7-4cee-91e6-f559e20cbcba',
    'admin.hostManager',
    'test.read',
    'test.readWrite'
];
const kExpectedTestServerSecurityTwoRoles = [
    'admin.issuerAzure/95712d55-ff83-48c6-b77b-1bfbc955698e',
    'test.read',
    'test.readWrite',
    'test.userAdmin'
];
const kExpectedTestServerSecurityThreeRoles = [
    'admin.issuerAzure/21c0bd78-e5a7-4cee-91e6-f559e20cbcba',
    'admin.issuerAzure/95712d55-ff83-48c6-b77b-1bfbc955698e',
    'admin.hostManager',
    'test.read',
    'test.readWrite',
    'test.userAdmin'
];
const kExpectedAuthSuccessLogId = 5286306;
const kExpectedTestServerSecurityOneAuthLog = {
    mechanism: 'MONGODB-OIDC',
    user: 'issuerAzure/tD548GwE@outlook.com',
    db: '$external',
};

// Authenticates as userName, verifies that the expected name and roles appear via connectionStatus,
// and calls the registered callback to verify that the connection can perform operations
// corresponding to the role mapping.
function authAsUser(conn, userName, expectedRoles, authzCheckCallback) {
    const externalDB = conn.getDB('$external');
    assert(externalDB.auth({user: userName, mechanism: 'MONGODB-OIDC'}));
    const connStatus = assert.commandWorked(conn.adminCommand({connectionStatus: 1}));
    assert.eq(connStatus.authInfo.authenticatedUsers.length, 1);
    assert.eq(connStatus.authInfo.authenticatedUsers[0].db, '$external');
    assert.eq(connStatus.authInfo.authenticatedUsers[0].user, 'issuerAzure/' + userName);
    const sortedActualRoles =
        connStatus.authInfo.authenticatedUserRoles.map((r) => r.db + '.' + r.role).sort();
    const sortedExpectRoles = expectedRoles.sort();
    assert.eq(sortedActualRoles, sortedExpectRoles);
    authzCheckCallback();
    externalDB.logout();
}

// Authenticates as tD548GwE@outlook.com and then sleeps for 10 minutes (configured
// expiry time of tokens from Azure test instance). Operations should still succeed after the
// expiry without ErrorCodes.ReauthenticationRequired thanks to the shell's built-in refresh flow.
function runRefreshFlowTest(conn) {
    const externalDB = conn.getDB('$external');
    assert(externalDB.auth({user: 'tD548GwE@outlook.com', mechanism: 'MONGODB-OIDC'}));
    assert.commandWorked(conn.adminCommand({oidcListKeys: 1}));

    // In a parallel shell, assert that there have already been 2 auth logs as
    // 'tD548GwE@outlook.com' before token expiry.
    const adminShell = new Mongo(conn.host);
    const parallelShellAdminDB = adminShell.getDB('admin');
    assert(parallelShellAdminDB.auth('admin', 'pwd'));
    checkLog.containsRelaxedJson(
        adminShell, kExpectedAuthSuccessLogId, kExpectedTestServerSecurityOneAuthLog, 2);

    // Wait for access token expiration... (10 mins + 30 seconds for clock skew)
    sleep(630 * 1000);

    // The updated configuration will result in ErrorCodes.ReauthenticationRequired from the server.
    // However, the shell should implicitly reauth via the refresh flow and then successfully retry
    // the command. We check the logs for proof of that implicit reauth.
    assert.commandWorked(conn.adminCommand({oidcListKeys: 1}));
    checkLog.containsRelaxedJson(
        adminShell, kExpectedAuthSuccessLogId, kExpectedTestServerSecurityOneAuthLog, 3);
    externalDB.logout();
}

function runTest(conn) {
    // Create the roles that the Azure groups will map to.
    assert.commandWorked(conn.adminCommand({createUser: 'admin', pwd: 'pwd', roles: ['root']}));
    const adminDB = conn.getDB('admin');
    assert(adminDB.auth('admin', 'pwd'));
    assert.commandWorked(conn.adminCommand({
        createRole: 'issuerAzure/21c0bd78-e5a7-4cee-91e6-f559e20cbcba',
        roles: [
            {role: 'readWrite', db: 'test'},
            {role: 'hostManager', db: 'admin'},
            {role: 'read', db: 'test'}
        ],
        privileges: []
    }));
    assert.commandWorked(conn.adminCommand({
        createRole: 'issuerAzure/95712d55-ff83-48c6-b77b-1bfbc955698e',
        roles: [
            {role: 'read', db: 'test'},
            {role: 'readWrite', db: 'test'},
            {role: 'userAdmin', db: 'test'}
        ],
        privileges: []
    }));
    adminDB.logout();

    // Set the OIDC IdP auth callback function.
    conn._setOIDCIdPAuthCallback(String(function() {
        runNonMongoProgram(getPython3Binary(),
                           'jstests/auth/lib/automated_idp_authn_simulator_azure.py',
                           '--activationEndpoint',
                           this.activationEndpoint,
                           '--userCode',
                           this.userCode,
                           '--username',
                           this.userName,
                           '--setupFile',
                           'oidc_e2e_setup.json');
    }));

    // Auth as tD548GwE@outlook.com. They should have readWrite and hostManager
    // roles.
    const testDB = conn.getDB('test');
    authAsUser(conn, 'tD548GwE1@outlook.com', kExpectedTestServerSecurityOneRoles, () => {
        assert(testDB.coll1.insert({name: 'testserversecurityone'}));
        assert.throws(() => testDB.createUser({
            user: 'fakeUser',
            pwd: 'fakePwd',
            roles: [
                {role: 'read', db: 'test'},
            ],
        }));
        assert.commandWorked(conn.adminCommand({oidcListKeys: 1}));
    });

    // Auth as tD548GwE2@outlook.com. They should have readWrite and userAdmin roles.
    authAsUser(conn, 'tD548GwE2@outlook.com', kExpectedTestServerSecurityTwoRoles, () => {
        assert(testDB.coll1.insert({name: 'testserversecuritytwo'}));
        testDB.createUser({
            user: 'fakeUser',
            pwd: 'fakePwd',
            roles: [
                {role: 'read', db: 'test'},
            ],
        });
        assert.commandFailed(conn.adminCommand({oidcListKeys: 1}));
    });

    // Auth as tD548GwE3@outlook.com. They should have readWrite, dbAdmin, and
    // userAdmin roles.
    authAsUser(conn, 'tD548GwE3@outlook.com', kExpectedTestServerSecurityThreeRoles, () => {
        assert(testDB.coll1.insert({name: 'testserversecuritythree'}));
        assert.commandWorked(conn.adminCommand({oidcListKeys: 1}));
        testDB.dropUser('fakeUser');
    });
}

{
    const shardedCluster = new ShardingTest({
        mongos: 1,
        config: 1,
        shards: 1,
        other: {mongosOptions: {setParameter: kAzureStartupOptions}},
        keyFile: 'jstests/libs/key1',
    });
    runTest(shardedCluster.s0);
    shardedCluster.stop();
}
})();
