// Tests adding, removing and modifying IDP configurations for OIDC.
// @tags: [ requires_fcv_70 ]

import {determineSSLProvider} from "jstests/ssl/libs/ssl_helpers.js";
import {
    OIDCKeyServer,
    tryTokenAuth
} from "src/mongo/db/modules/enterprise/jstests/external_auth/lib/oidc_utils.js";
import {OIDCVars} from "src/mongo/db/modules/enterprise/jstests/external_auth/lib/oidc_vars.js";

if (determineSSLProvider() !== 'openssl') {
    print('Skipping test, OIDC is only available with OpenSSL');
    quit();
}

const assetsDir = 'src/mongo/db/modules/enterprise/jstests/external_auth/lib';
const singleKey = assetsDir + '/custom-key-1.json';
const multipleKeys = assetsDir + '/custom-keys_1_2.json';

// Each issuer has its own key server endpoint and associated metadata.
let keyMap = {
    issuer1: singleKey,
    issuer2: multipleKeys,
    issuer3: singleKey,
};
const KeyServer = new OIDCKeyServer(JSON.stringify(keyMap));
const issuerOneRefreshIntervalSecs = 15;
const issuerTwoRefreshIntervalSecs = 30;
const issuer1 = KeyServer.getURL() + '/issuer1';
const issuer2 = KeyServer.getURL() + '/issuer2';

const expectedRolesIssuer1 = ['issuer1/myReadRole', 'readAnyDatabase'];
const expectedRolesIssuer1Admin = ['issuer1/myReadWriteRole', 'readWriteAnyDatabase'];
const expectedRolesIssuer2 = ['issuer2/myReadRole', 'read'];
const expectedRolesIssuer1AltAudience = ['issuer1-alt/myReadRole', 'read'];

// Startup parameters and constants.
const issuerOneConfig = {
    issuer: issuer1,
    audience: 'jwt@kernel.mongodb.com',
    authNamePrefix: 'issuer1',
    matchPattern: '@mongodb.com$',
    clientId: 'deadbeefcafe',
    requestScopes: ['email'],
    principalName: 'sub',
    authorizationClaim: 'mongodb-roles',
    logClaims: ['sub', 'aud', 'mongodb-roles', 'does-not-exist'],
    JWKSPollSecs: issuerOneRefreshIntervalSecs,
};
const issuerTwoConfig = {
    issuer: issuer2,
    audience: 'jwt@kernel.mongodb.com',
    authNamePrefix: 'issuer2',
    matchPattern: '@10gen.com$',
    clientId: 'deadbeefcafe',
    authorizationClaim: 'mongodb-roles',
    JWKSPollSecs: issuerTwoRefreshIntervalSecs,
};
const issuerOneAltAudienceConfig = {
    issuer: issuer1,
    audience: 'jwt@kernel.10gen.com',
    authNamePrefix: 'issuer1-alt',
    authorizationClaim: 'mongodb-roles',
    supportsHumanFlows: false,
    JWKSPollSecs: issuerOneRefreshIntervalSecs,
};

const startupOptions = {
    authenticationMechanisms: 'SCRAM-SHA-256,MONGODB-OIDC'
};

const {
    'Token_OIDCAuth_user1': issuerOneKeyOneToken,
    'Token_OIDCAuth_user4': issuerOneKeyAdminToken,
    'Token_OIDCAuth_user1@10gen': issuerTwoKeyOneToken,
    'Token_OIDCAuth_user1@10gen_custom_key_2': issuerTwoKeyTwoToken,
    'Token_OIDCAuth_user1_alt_audience': issuerOneKeyOneAltAudienceToken,
} = OIDCVars(KeyServer.getURL()).kOIDCTokens;

const kAlternateAuthNamePrefixes = [
    '-',
    '_',
    'my-prefix',
    'your_prefix',
    '_-_',
    '-_-',
];

const kInvalidAuthNamePrefixes = [
    '',
    'foo.bar',
    'foo/bar',
    '$foobar',
    'hello world',
    '"howdy"',
];

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

    assert.commandWorked(conn.adminCommand({
        createRole: expectedRolesIssuer1AltAudience[0],
        roles: [expectedRolesIssuer1AltAudience[1]],
        privileges: []
    }));

    // Create roles for user1 with alternate authNamePrefixes.
    const roleSuffix = expectedRolesIssuer1[0].substring(expectedRolesIssuer1[0].indexOf('/'));
    kAlternateAuthNamePrefixes.forEach(
        (prefix) => assert.commandWorked(conn.adminCommand(
            {createRole: prefix + roleSuffix, roles: [expectedRolesIssuer1[1]], privileges: []})));

    // Create a user with the hostManager role to run OIDC commands.
    assert.commandWorked(
        conn.adminCommand({createUser: 'oidcAdmin', 'pwd': 'oidcAdmin', roles: ['hostManager']}));

    // Increase logging verbosity.
    assert.commandWorked(adminDB.setLogLevel(3));
}

function assertAuthSuccessful(conn, token, expectUser, expectedRoles, logout = true) {
    const admin = conn.getDB('admin');
    const external = conn.getDB('$external');

    assert(tryTokenAuth(conn, token));
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

function testInvalidConfig(conn, params) {
    const newConfig = Object.assign({}, issuerOneConfig, params);
    assert.commandFailed(conn.adminCommand({setParameter: 1, oidcIdentityProviders: [newConfig]}));
}

// Test adding IDP during runtime works as expected.
function testRuntimeAddIDP(conn) {
    const addKeyShell = new Mongo(conn.host);
    const externalDB = addKeyShell.getDB('$external');

    // Assert all token authentication fails before OIDC configuration is added.
    assert(!tryTokenAuth(addKeyShell, issuerOneKeyOneToken));
    assert(!tryTokenAuth(addKeyShell, issuerTwoKeyOneToken));
    assert(!tryTokenAuth(addKeyShell, issuerOneKeyOneAltAudienceToken));
    assert.commandFailedWithCode(addKeyShell.adminCommand({listDatabases: 1}),
                                 ErrorCodes.Unauthorized);

    // Add OIDC configuration with issuerOne.
    assert.commandWorked(
        conn.adminCommand({setParameter: 1, oidcIdentityProviders: [issuerOneConfig]}));

    // Assert only issuerOneKeyOneToken can authenticate
    assert(!tryTokenAuth(addKeyShell, issuerTwoKeyOneToken));
    assert(!tryTokenAuth(addKeyShell, issuerOneKeyOneAltAudienceToken));
    assert.commandFailedWithCode(addKeyShell.adminCommand({listDatabases: 1}),
                                 ErrorCodes.Unauthorized);
    assert(tryTokenAuth(addKeyShell, issuerOneKeyOneToken));
    assert.commandWorked(addKeyShell.adminCommand({listDatabases: 1}));

    // Add issuerTwo to the config, allowing users with tokens signed by issuerTwo to authenticate.
    assert.commandWorked(conn.adminCommand(
        {setParameter: 1, oidcIdentityProviders: [issuerOneConfig, issuerTwoConfig]}));

    // Token signed by issuerOne should still be authenticated.
    assert.commandWorked(addKeyShell.adminCommand({listDatabases: 1}));
    externalDB.logout();

    // Assert only issuerOneKeyOneAltAudienceToken can't authenticate
    assert(!tryTokenAuth(addKeyShell, issuerOneKeyOneAltAudienceToken));

    // Assert token issued by issuerTwo can now authenticate.
    assertAuthSuccessful(
        addKeyShell, issuerTwoKeyOneToken, 'issuer2/user1@10gen.com', expectedRolesIssuer2);
    externalDB.logout();

    // Add another config with same issuer as issuerOne, but for different audience
    assert.commandWorked(conn.adminCommand({
        setParameter: 1,
        oidcIdentityProviders: [issuerOneConfig, issuerTwoConfig, issuerOneAltAudienceConfig]
    }));

    // Assert can now auth using issuerOneKeyOneAltAudienceToken
    assertAuthSuccessful(addKeyShell,
                         issuerOneKeyOneAltAudienceToken,
                         'issuer1-alt/user1@mongodb.com',
                         expectedRolesIssuer1AltAudience,
                         true);

    // Assert can still auth using issuerOneKeyOneToken
    assertAuthSuccessful(
        addKeyShell, issuerOneKeyOneToken, 'issuer1/user1@mongodb.com', expectedRolesIssuer1, true);
}

// Test modifying certain attributes to IDP's invalidates users.
function testRuntimeModifyIDP(conn) {
    const shell = new Mongo(conn.host);

    // Audience
    modifyAndTestParameters(conn, shell, {audience: 'jwt@differentdomain.com'}, true);

    // Issuer
    modifyAndTestParameters(conn, shell, {issuer: KeyServer.getURL() + '/issuer3'}, true);

    // Principal Name (non-existent in token)
    modifyAndTestParameters(conn, shell, {principalName: 'user'}, true);

    // Principal Name (exists in token)
    modifyAndTestParameters(conn, shell, {principalName: 'nonce'}, true);

    // Log Claims
    modifyAndTestParameters(conn, shell, {logClaims: ['does-not-exist']}, false);

    // Authorization Name Prefix
    modifyAndTestParameters(conn, shell, {authNamePrefix: 'issuer3'}, true);

    // Edge case auth name prefixes.
    kAlternateAuthNamePrefixes.forEach(
        (prefix) => modifyAndTestParameters(conn, shell, {authNamePrefix: prefix}, true));

    // Invalid authNamePrefix
    kInvalidAuthNamePrefixes.forEach((prefix) => testInvalidConfig(conn, {authNamePrefix: prefix}));

    // Duplicate (issuer, audience) pair
    assert.commandFailedWithCode(
        conn.adminCommand(
            {setParameter: 1, oidcIdentityProviders: [issuerOneConfig, issuerOneConfig]}),
        ErrorCodes.BadValue);

    // Missing clientId without supportsHumanFlows: false.
    const {clientId, ...noClientIdConfig} = issuerOneConfig;
    assert.commandFailed(
        conn.adminCommand({setParameter: 1, oidcIdentityProviders: [noClientIdConfig]}));

    // Missing authorizationClaim without useAuthorizationClaim: false.
    const {authorizationClaim, ...noAuthzClaimConfig} = issuerOneConfig;
    assert.commandFailed(
        conn.adminCommand({setParameter: 1, oidcIdentityProviders: [noAuthzClaimConfig]}));
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
    let addKeyShell = new Mongo(conn.host);
    let externalDB = addKeyShell.getDB('$external');

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

    // Reset the connection
    addKeyShell = new Mongo(conn.host);
    externalDB = addKeyShell.getDB('$external');

    // Set issuerOne and issuerOneAltAudience as IDP's.
    assert.commandWorked(conn.adminCommand(
        {setParameter: 1, oidcIdentityProviders: [issuerOneConfig, issuerOneAltAudienceConfig]}));

    // Assert tokens from same issuer w/ different audience are able to authenticate.
    assertAuthSuccessful(
        addKeyShell, issuerOneKeyOneToken, 'issuer1/user1@mongodb.com', expectedRolesIssuer1);
    assertAuthSuccessful(addKeyShell,
                         issuerOneKeyOneAltAudienceToken,
                         'issuer1-alt/user1@mongodb.com',
                         expectedRolesIssuer1AltAudience);

    // Authenticate user with issuerOneToken.
    assert(tryTokenAuth(addKeyShell, issuerOneKeyOneToken));
    assert.commandWorked(addKeyShell.adminCommand({listDatabases: 1}));

    // Remove issuerOneAltAudience from the IDP's.
    assert.commandWorked(
        conn.adminCommand({setParameter: 1, oidcIdentityProviders: [issuerOneConfig]}));

    // Assert issuerOneToken is still authenticated
    assert.commandWorked(addKeyShell.adminCommand({listDatabases: 1}));

    // Assert issuerOneAltAudience can no longer authenticate
    externalDB.logout();
    assert(!tryTokenAuth(addKeyShell, issuerOneKeyOneAltAudienceToken));
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
