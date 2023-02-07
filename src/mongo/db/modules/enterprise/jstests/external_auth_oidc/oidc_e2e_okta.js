// Tests end-to-end OIDC authentication with Okta. Shell uses device authorization grant flow to
// acquire tokens.
// @tags: [ featureFlagOIDC ]

(function() {
'use strict';

load('jstests/ssl/libs/ssl_helpers.js');

if (determineSSLProvider() !== 'openssl') {
    print('Skipping test, OIDC is only available with OpenSSL');
    return;
}

const kOktaConfig = [{
    issuer: 'https://mongodb-dev.okta.com/oauth2/ausp9mlunyae9WiWb357',
    audience: 'api://core-server-test-cluster',
    authNamePrefix: 'issuerOkta',
    matchPattern: '@okta-test.com$',
    clientId: '0oap9n0aklzBH41cz357',
    requestScopes: ['openid', 'offline_access', 'email'],
    principalName: 'sub',
    authorizationClaim: 'groups',
    logClaims: ['sub', 'aud', 'groups'],
    JWKSPollSecs: 86400,
    deviceAuthorizationEndpoint:
        'https://mongodb-dev.okta.com/oauth2/ausp9mlunyae9WiWb357/v1/device/authorize',
    tokenEndpoint: 'https://mongodb-dev.okta.com/oauth2/ausp9mlunyae9WiWb357/v1/token',
    JWKSUri: 'https://mongodb-dev.okta.com/oauth2/ausp9mlunyae9WiWb357/v1/keys',
}];
const kOktaStartupOptions = {
    authenticationMechanisms: 'SCRAM-SHA-256,MONGODB-OIDC',
    logComponentVerbosity: tojson({accessControl: 3}),
    oidcIdentityProviders: tojson(kOktaConfig),
};
const kExpectedTestServerSecurityOneRoles = [
    'admin.issuerOkta/Everyone',
    'admin.issuerOkta/TOS Accepted & Email Verified',
    'admin.issuerOkta/server-security-test-group-one',
    'admin.hostManager',
    'test.read',
    'test.readWrite'
];
const kExpectedTestServerSecurityTwoRoles = [
    'admin.issuerOkta/Everyone',
    'admin.issuerOkta/TOS Accepted & Email Verified',
    'admin.issuerOkta/server-security-test-group-two',
    'test.read',
    'test.readWrite',
    'test.userAdmin'
];
const kExpectedTestServerSecurityThreeRoles = [
    'admin.issuerOkta/Everyone',
    'admin.issuerOkta/TOS Accepted & Email Verified',
    'admin.issuerOkta/server-security-test-group-one',
    'admin.issuerOkta/server-security-test-group-two',
    'admin.hostManager',
    'test.read',
    'test.readWrite',
    'test.userAdmin'
];

// Authenticates as userName, verifies that the expected name and roles appear via connectionStatus,
// and calls the registered callback to verify that the connection can perform operations
// corresponding to the role mapping.
function authAsUser(conn, userName, expectedRoles, authzCheckCallback) {
    const externalDB = conn.getDB('$external');
    assert(externalDB.auth({user: userName, mechanism: 'MONGODB-OIDC'}));
    const connStatus = assert.commandWorked(conn.adminCommand({connectionStatus: 1}));
    assert.eq(connStatus.authInfo.authenticatedUsers.length, 1);
    assert.eq(connStatus.authInfo.authenticatedUsers[0].db, '$external');
    assert.eq(connStatus.authInfo.authenticatedUsers[0].user, 'issuerOkta/' + userName);
    const sortedActualRoles =
        connStatus.authInfo.authenticatedUserRoles.map((r) => r.db + '.' + r.role).sort();
    const sortedExpectRoles = expectedRoles.sort();
    assert.eq(sortedActualRoles, sortedExpectRoles);
    authzCheckCallback();
    externalDB.logout();
}

function runTest(conn) {
    // Create the roles that the Okta groups will map to.
    assert.commandWorked(conn.adminCommand({createUser: 'admin', pwd: 'pwd', roles: ['root']}));
    const adminDB = conn.getDB('admin');
    assert(adminDB.auth('admin', 'pwd'));
    assert.commandWorked(conn.adminCommand(
        {createRole: 'issuerOkta/Everyone', roles: [{role: 'read', db: 'test'}], privileges: []}));
    assert.commandWorked(conn.adminCommand({
        createRole: 'issuerOkta/TOS Accepted & Email Verified',
        roles: [{role: 'readWrite', db: 'test'}],
        privileges: []
    }));
    assert.commandWorked(conn.adminCommand({
        createRole: 'issuerOkta/server-security-test-group-one',
        roles: [{role: 'hostManager', db: 'admin'}],
        privileges: []
    }));
    assert.commandWorked(conn.adminCommand({
        createRole: 'issuerOkta/server-security-test-group-two',
        roles: [{role: 'userAdmin', db: 'test'}],
        privileges: []
    }));
    adminDB.logout();

    // Set the OIDC IdP auth callback function.
    conn._setOIDCIdPAuthCallback(String(function() {
        runNonMongoProgram('python',
                           'jstests/auth/lib/automated_idp_authn_simulator.py',
                           '--activationEndpoint',
                           this.activationEndpoint,
                           '--username',
                           this.userName,
                           '--setupFile',
                           'oidc_e2e_setup.json');
    }));

    // Auth as testserversecurityone@okta-test.com. They should have readWrite and hostManager
    // roles.
    const testDB = conn.getDB('test');
    authAsUser(
        conn, 'testserversecurityone@okta-test.com', kExpectedTestServerSecurityOneRoles, () => {
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

    // Auth as testserversecuritytwo@okta-test.com. They should have readWrite and userAdmin roles.
    authAsUser(
        conn, 'testserversecuritytwo@okta-test.com', kExpectedTestServerSecurityTwoRoles, () => {
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

    // Auth as testserversecuritythree@okta-test.com. They should have readWrite, dbAdmin, and
    // userAdmin roles.
    authAsUser(conn,
               'testserversecuritythree@okta-test.com',
               kExpectedTestServerSecurityThreeRoles,
               () => {
                   assert(testDB.coll1.insert({name: 'testserversecuritythree'}));
                   assert.commandWorked(conn.adminCommand({oidcListKeys: 1}));
                   testDB.dropUser('fakeUser');
               });
}

{
    const standalone = MongoRunner.runMongod({auth: '', setParameter: kOktaStartupOptions});
    runTest(standalone);
    MongoRunner.stopMongod(standalone);
}

{
    const shardedCluster = new ShardingTest({
        mongos: 1,
        config: 1,
        shards: 1,
        other: {mongosOptions: {setParameter: kOktaStartupOptions}},
        keyFile: 'jstests/libs/key1',
    });
    runTest(shardedCluster.s0);
    shardedCluster.stop();
}
})();
