// Tests adding, removing and modifying IDP configurations for OIDC.
// @tags: [ requires_fcv_70 ]

(function() {
'use strict';

load("jstests/libs/parallel_shell_helpers.js");
load('jstests/ssl/libs/ssl_helpers.js');

const assetsDir = 'src/mongo/db/modules/enterprise/jstests/external_auth/lib';
load(assetsDir + '/oidc_utils.js');
load(assetsDir + '/oidc_vars.js');

if (determineSSLProvider() !== 'openssl') {
    print('Skipping test, OIDC is only available with OpenSSL');
    return;
}

const singleKey = assetsDir + '/custom-key-1.json';
const multipleKeys = assetsDir + '/custom-keys_1_2.json';

// Each issuer has its own key server endpoint and associated metadata.
let keyMap = {
    issuerOne: singleKey,
    issuerTwo: multipleKeys,
};
const KeyServer = new OIDCKeyServer(JSON.stringify(keyMap));
const issuerOneRefreshIntervalSecs = 15;
const issuerTwoRefreshIntervalSecs = 30;
const issuerOne = 'https://test.kernel.mongodb.com/oidc/issuer1';
const issuerTwo = 'https://test.kernel.mongodb.com/oidc/issuer2';
const issuerOneJWKSUri = KeyServer.getURL() + '/issuerOne';
const issuerTwoJWKSUri = KeyServer.getURL() + '/issuerTwo';

const expectedRolesIssuer1 = ['issuer1/myReadRole', 'readAnyDatabase'];
const expectedRolesIssuer1Admin = ['issuer1/myReadWriteRole', 'readWriteAnyDatabase'];
const expectedRolesIssuer2 = ['issuer2/myReadRole', 'read'];

// Startup parameters and constants.
const issuerOneConfig = {
    issuer: issuerOne,
    audience: 'jwt@kernel.mongodb.com',
    authNamePrefix: 'issuer1',
    matchPattern: '@mongodb.com$',
    clientId: 'deadbeefcafe',
    clientSecret: 'hunter2',
    requestScopes: ['email'],
    principalName: 'sub',
    authorizationClaim: 'mongodb-roles',
    logClaims: ['sub', 'aud', 'mongodb-roles', 'does-not-exist'],
    JWKSPollSecs: issuerOneRefreshIntervalSecs,
    deviceAuthorizationEndpoint: 'https://test.kernel.mongodb.com/oidc/device',
    authorizationEndpoint: 'https://test.kernel.mongodb.com/oidc/auth',
    tokenEndpoint: 'https://test.kernel.mongodb.com/oidc/token',
    JWKSUri: issuerOneJWKSUri,
};
const issuerTwoConfig = {
    issuer: issuerTwo,
    audience: 'jwt@kernel.mongodb.com',
    authNamePrefix: 'issuer2',
    matchPattern: '@10gen.com$',
    clientId: 'deadbeefcafe',
    authorizationClaim: 'mongodb-roles',
    JWKSPollSecs: issuerTwoRefreshIntervalSecs,
    deviceAuthorizationEndpoint: 'https://test.kernel.mongodb.com/oidc/device',
    tokenEndpoint: 'https://test.kernel.mongodb.com/oidc/token',
    JWKSUri: issuerTwoJWKSUri,
};

const startupOptions = {
    authenticationMechanisms: 'SCRAM-SHA-256,MONGODB-OIDC'
};
const issuerOneKeyOneToken = kOIDCTokens['Token_OIDCAuth_user1'];
const issuerOneKeyAdminToken = kOIDCTokens['Token_OIDCAuth_user4'];
const issuerTwoKeyOneToken = kOIDCTokens['Token_OIDCAuth_user1@10gen'];
const issuerTwoKeyTwoToken = kOIDCTokens['Token_OIDCAuth_user1@10gen_custom_key_2'];

// Set up the node for the test.
function setup(conn) {
    const adminDB = conn.getDB('admin');
    assert.commandWorked(conn.adminCommand({createUser: 'admin', 'pwd': 'admin', roles: ['root']}));
    assert(adminDB.auth('admin', 'admin'));

    // Create the roles corresponding to user1@mongodb.com and user1@10gen.com's groups.
    assert.commandWorked(conn.adminCommand(
        {createRole: expectedRolesIssuer1[0], roles: [expectedRolesIssuer1[1]], privileges: []}));
    assert.commandWorked(conn.adminCommand({
        createRole: expectedRolesIssuer1Admin[0],
        roles: [expectedRolesIssuer1Admin[1]],
        privileges: []
    }));
    assert.commandWorked(conn.adminCommand(
        {createRole: expectedRolesIssuer2[0], roles: [expectedRolesIssuer2[1]], privileges: []}));

    // Create a user with the hostManager role to run OIDC commands.
    assert.commandWorked(
        conn.adminCommand({createUser: 'oidcAdmin', 'pwd': 'oidcAdmin', roles: ['hostManager']}));

    // Increase logging verbosity.
    assert.commandWorked(adminDB.setLogLevel(3));
}

function assertAuthSuccessful(conn, token, expectUser, expectedRoles, logout = true) {
    const admin = conn.getDB('admin');
    const external = conn.getDB('$external');

    assert(external.auth({oidcAccessToken: token, mechanism: 'MONGODB-OIDC'}));
    assert.commandWorked(admin.runCommand({listDatabases: 1}));

    const userInfo = assert.commandWorked(admin.runCommand({connectionStatus: 1}));
    assert.eq(userInfo.authInfo.authenticatedUsers.length, 1, "Unexpected number of users");
    assert.eq(userInfo.authInfo.authenticatedUsers[0].user, expectUser);

    const sortedActualRoles =
        userInfo.authInfo.authenticatedUserRoles.map((r) => r.db + '.' + r.role).sort();
    const sortedExpectRoles = expectedRoles.map((r) => 'admin.' + r).sort();
    assert.eq(sortedActualRoles, sortedExpectRoles);

    if (logout) {
        external.logout();
    }
}

function modifyAndTestParameters(conn, shell, params, shouldFail) {
    // Set the configuration as the default for issuerOne.
    assert.commandWorked(
        conn.adminCommand({setParameter: 1, oidcIdentityProviders: [issuerOneConfig]}));

    // Tokens signed by issuerOne should be able to authenticate.
    assertAuthSuccessful(
        shell, issuerOneKeyOneToken, 'issuer1/user1@mongodb.com', expectedRolesIssuer1, false);

    // Modify params and verify we get an appropriate error.
    const newConfig = Object.assign({}, issuerOneConfig, params);
    assert.commandWorked(conn.adminCommand({setParameter: 1, oidcIdentityProviders: [newConfig]}));

    if (shouldFail) {
        assert.commandFailedWithCode(shell.adminCommand({listDatabases: 1}),
                                     ErrorCodes.ReauthenticationRequired);
    } else {
        assert.commandWorked(shell.adminCommand({listDatabases: 1}));
    }
}

// Test adding IDP during runtime works as expected.
function testRuntimeAddIDP(conn) {
    const addKeyShell = new Mongo(conn.host);
    const externalDB = addKeyShell.getDB('$external');

    // Assert authentication fails before OIDC configuration is added.
    assert(!externalDB.auth({oidcAccessToken: issuerOneKeyOneToken, mechanism: 'MONGODB-OIDC'}));
    assert.commandFailed(addKeyShell.adminCommand({listDatabases: 1}));

    // Add OIDC configuration with issuerOne.
    assert.commandWorked(
        conn.adminCommand({setParameter: 1, oidcIdentityProviders: [issuerOneConfig]}));

    assert(!externalDB.auth({oidcAccessToken: issuerTwoKeyOneToken, mechanism: 'MONGODB-OIDC'}));
    assert.commandFailedWithCode(addKeyShell.adminCommand({listDatabases: 1}),
                                 ErrorCodes.Unauthorized);

    assert(externalDB.auth({oidcAccessToken: issuerOneKeyOneToken, mechanism: 'MONGODB-OIDC'}));
    assert.commandWorked(addKeyShell.adminCommand({listDatabases: 1}));

    // Add issuerTwo to the config, allowing users with tokens signed by issuerTwo to authenticate.
    assert.commandWorked(conn.adminCommand(
        {setParameter: 1, oidcIdentityProviders: [issuerOneConfig, issuerTwoConfig]}));

    // Token signed by issuerOne should still be authenticated.
    assert.commandWorked(addKeyShell.adminCommand({listDatabases: 1}));
    externalDB.logout();

    // Assert token issued by issuerTwo can now authenticate.
    assertAuthSuccessful(
        addKeyShell, issuerTwoKeyOneToken, 'issuer2/user1@10gen.com', expectedRolesIssuer2);
}

// Test modifying certain attributes to IDP's invalidates users.
function testRuntimeModifyIDP(conn) {
    const shell = new Mongo(conn.host);

    // Audience
    modifyAndTestParameters(conn, shell, {audience: 'jwt@differentdomain.com'}, true);

    // Issuer
    modifyAndTestParameters(
        conn, shell, {issuer: 'https://test.kernel.mongodb.com/oidc/issuer3'}, true);

    // Principal Name
    modifyAndTestParameters(conn, shell, {principalName: 'user'}, true);

    // Authorization Endpoint
    modifyAndTestParameters(
        conn,
        shell,
        {authorizationEndpoint: 'https://modified.kernel.mongodb.com/oidc/auth'},
        false);

    // Token Endpoint
    modifyAndTestParameters(
        conn, shell, {tokenEndpoint: 'https://modified.kernel.mongodb.com/oidc/token'}, false);

    // Log Claims
    modifyAndTestParameters(conn, shell, {logClaims: ['does-not-exist']}, false);

    // Authorization Name Prefix
    modifyAndTestParameters(conn, shell, {authNamePrefix: 'issuer3'}, true);
}

function testRuntimeModifyAuthClaim(conn) {
    const shell = new Mongo(conn.host);
    const testDb = shell.getDB("test");

    assertAuthSuccessful(shell,
                         issuerOneKeyAdminToken,
                         'issuer1/user4@mongodb.com',
                         expectedRolesIssuer1Admin,
                         false);

    // Current user has readWriteAnyDatabase role, assert it can write to collection foo.
    assert.commandWorked(testDb.runCommand({insert: "foo", documents: [{"foo": "bar"}]}));

    // Modify config to change authorizationClaim to mongodb-roles2, this is mapped with
    // readAnyDatabase role.
    const newConfig = Object.assign({}, issuerOneConfig, {authorizationClaim: 'mongodb-roles2'});
    assert.commandWorked(conn.adminCommand({setParameter: 1, oidcIdentityProviders: [newConfig]}));

    // Assert user can no longer insert since it does not have write privilages.
    assert.commandFailedWithCode(testDb.runCommand({insert: "foo", documents: [{"foo2": "bar"}]}),
                                 ErrorCodes.Unauthorized);
}

// Test removing an IDP during runtime invalidate users belonging to that IDP.
function testRuntimeRemoveIDP(conn) {
    const addKeyShell = new Mongo(conn.host);
    const externalDB = addKeyShell.getDB('$external');

    // Set issuerOne and issuerTwo as IDP's.
    assert.commandWorked(conn.adminCommand(
        {setParameter: 1, oidcIdentityProviders: [issuerOneConfig, issuerTwoConfig]}));

    // Assert tokens from each issuer are able to authenticate.
    assertAuthSuccessful(
        addKeyShell, issuerOneKeyOneToken, 'issuer1/user1@mongodb.com', expectedRolesIssuer1);
    assertAuthSuccessful(
        addKeyShell, issuerTwoKeyTwoToken, 'issuer2/user1@10gen.com', expectedRolesIssuer2);

    // Authenticate user with issuerTwoToken.
    assert(externalDB.auth({oidcAccessToken: issuerTwoKeyTwoToken, mechanism: 'MONGODB-OIDC'}));
    assert.commandWorked(addKeyShell.adminCommand({listDatabases: 1}));

    // Remove issuerTwo from the IDP's.
    assert.commandWorked(
        conn.adminCommand({setParameter: 1, oidcIdentityProviders: [issuerOneConfig]}));

    // Assert that user that used token signed by issuerTwo should be invalidated immediately.
    assert.commandFailedWithCode(addKeyShell.adminCommand({listDatabases: 1}),
                                 ErrorCodes.ReauthenticationRequired);
    assert(!externalDB.auth({oidcAccessToken: issuerTwoKeyTwoToken, mechanism: 'MONGODB-OIDC'}));
}

function runTests(conn) {
    setup(conn);
    testRuntimeAddIDP(conn);
    testRuntimeModifyIDP(conn);
    testRuntimeRemoveIDP(conn);
    testRuntimeModifyAuthClaim(conn);
}

KeyServer.start();

{
    const mongod = MongoRunner.runMongod({auth: '', setParameter: startupOptions});
    runTests(mongod);
    MongoRunner.stopMongod(mongod);
}

{
    const shardedCluster = new ShardingTest({
        mongos: 1,
        config: 1,
        shards: 1,
        other: {mongosOptions: {setParameter: startupOptions}},
        keyFile: 'jstests/libs/key1',
    });
    runTests(shardedCluster.s0);
    shardedCluster.stop();
}

KeyServer.stop();
})();
