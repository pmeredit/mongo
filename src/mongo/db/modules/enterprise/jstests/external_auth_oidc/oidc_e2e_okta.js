// Tests end-to-end OIDC authentication with Okta. Shell uses device authorization grant flow to
// acquire tokens.
// @tags: [ requires_fcv_70 ]

import {getPython3Binary} from "jstests/libs/python.js";
import {determineSSLProvider} from "jstests/ssl/libs/ssl_helpers.js";
import {
    authAsUser,
    runRefreshFlowTest
} from "src/mongo/db/modules/enterprise/jstests/external_auth_oidc/lib/oidc_e2e_lib.js";

if (determineSSLProvider() !== 'openssl') {
    print('Skipping test, OIDC is only available with OpenSSL');
    quit();
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
}];
const kOktaStartupOptions = {
    authenticationMechanisms: 'SCRAM-SHA-256,MONGODB-OIDC',
    logComponentVerbosity: tojson({accessControl: 3}),
    oidcIdentityProviders: tojson(kOktaConfig),
    JWKSMinimumQuiescePeriodSecs: 0,
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
const kExpectedTestServerSecurityOneAuthLog = {
    mechanism: 'MONGODB-OIDC',
    user: 'issuerOkta/testserversecurityone@okta-test.com',
    db: '$external',
};

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

    const pythonBinary = getPython3Binary();

    // Set the OIDC IdP auth callback function.
    conn._setOIDCIdPAuthCallback(`function() {
        runNonMongoProgram('${pythonBinary}',
                           'jstests/auth/lib/automated_idp_authn_simulator_okta.py',
                           '--activationEndpoint',
                           this.activationEndpoint,
                           '--userCode',
                           this.userCode,
                           '--username',
                           this.userName,
                           '--setupFile',
                           'oidc_e2e_setup.json');
    }`);

    // Auth as testserversecurityone@okta-test.com. They should have readWrite and hostManager
    // roles.
    const testDB = conn.getDB('test');
    authAsUser(conn,
               'testserversecurityone@okta-test.com',
               kOktaConfig[0].authNamePrefix,
               kExpectedTestServerSecurityOneRoles,
               () => {
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
    authAsUser(conn,
               'testserversecuritytwo@okta-test.com',
               kOktaConfig[0].authNamePrefix,
               kExpectedTestServerSecurityTwoRoles,
               () => {
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
               kOktaConfig[0].authNamePrefix,
               kExpectedTestServerSecurityThreeRoles,
               () => {
                   assert(testDB.coll1.insert({name: 'testserversecuritythree'}));
                   assert.commandWorked(conn.adminCommand({oidcListKeys: 1}));
                   testDB.dropUser('fakeUser');
               });

    // Test that the refresh flow works as expected from the shell to the server.
    runRefreshFlowTest(conn,
                       'testserversecurityone@okta-test.com',
                       kExpectedTestServerSecurityOneAuthLog,
                       330 * 1000 /* sleep_time */);
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
