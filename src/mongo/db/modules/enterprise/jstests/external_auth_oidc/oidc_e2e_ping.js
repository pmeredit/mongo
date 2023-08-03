// Tests end-to-end OIDC authentication with PingFederate. Shell uses device authorization grant
// flow to acquire tokens.
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

const kPingFederateConfig = [{
    issuer: 'https://ping-federate-image.core-server.staging.corp.mongodb.com',
    audience: 'core-server-test-ping',
    authNamePrefix: 'issuerPing',
    matchPattern: '@ping-test.com$',
    clientId: 'ac_oic_client',
    requestScopes: ['openid'],
    principalName: 'sub',
    authorizationClaim: 'groups',
    logClaims: ['sub', 'aud', 'groups'],
    JWKSPollSecs: 86400,
}];
const kPingFederateStartupOptions = {
    authenticationMechanisms: 'SCRAM-SHA-256,MONGODB-OIDC',
    logComponentVerbosity: tojson({accessControl: 3}),
    oidcIdentityProviders: tojson(kPingFederateConfig),
};
const kExpectedTestServerSecurityOneRoles =
    ['admin.issuerPing/group1', 'admin.hostManager', 'test.read', 'test.readWrite'];
const kExpectedTestServerSecurityTwoRoles =
    ['admin.issuerPing/group2', 'test.read', 'test.readWrite', 'test.userAdmin'];
const kExpectedTestServerSecurityThreeRoles = [
    'admin.issuerPing/group1',
    'admin.issuerPing/group2',
    'admin.hostManager',
    'test.read',
    'test.readWrite',
    'test.userAdmin'
];
const kExpectedTestServerSecurityOneAuthLog = {
    mechanism: 'MONGODB-OIDC',
    user: 'issuerPing/testserversecurityone@ping-test.com',
    db: '$external',
};

function runTest(conn) {
    // Create the roles that the Ping groups will map to.
    assert.commandWorked(conn.adminCommand({createUser: 'admin', pwd: 'pwd', roles: ['root']}));
    const adminDB = conn.getDB('admin');
    assert(adminDB.auth('admin', 'pwd'));
    assert.commandWorked(conn.adminCommand({
        createRole: 'issuerPing/group1',
        roles: [
            {role: 'readWrite', db: 'test'},
            {role: 'hostManager', db: 'admin'},
            {role: 'read', db: 'test'}
        ],
        privileges: []
    }));
    assert.commandWorked(conn.adminCommand({
        createRole: 'issuerPing/group2',
        roles: [
            {role: 'read', db: 'test'},
            {role: 'readWrite', db: 'test'},
            {role: 'userAdmin', db: 'test'}
        ],
        privileges: []
    }));
    adminDB.logout();

    const pythonBinary = getPython3Binary();

    // Set the OIDC IdP auth callback function.
    conn._setOIDCIdPAuthCallback(`function() {
        runNonMongoProgram('${pythonBinary}',
                           'jstests/auth/lib/automated_idp_authn_simulator_ping.py',
                           '--activationEndpoint',
                           this.activationEndpoint,
                           '--userCode',
                           this.userCode,
                           '--username',
                           this.userName,
                           '--setupFile',
                           'oidc_e2e_setup.json');
    }`);

    // Auth as testserversecurityone@ping-test.com. They should have readWrite and hostManager
    // roles.
    const testDB = conn.getDB('test');
    authAsUser(conn,
               'testserversecurityone@ping-test.com',
               kPingFederateConfig[0].authNamePrefix,
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

    // Auth as testserversecuritytwo@ping-test.com. They should have readWrite and userAdmin roles.
    authAsUser(conn,
               'testserversecuritytwo@ping-test.com',
               kPingFederateConfig[0].authNamePrefix,
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

    // Auth as testserversecuritythree@ping-test.com. They should have readWrite, dbAdmin, and
    // userAdmin roles.
    authAsUser(conn,
               'testserversecuritythree@ping-test.com',
               kPingFederateConfig[0].authNamePrefix,
               kExpectedTestServerSecurityThreeRoles,
               () => {
                   assert(testDB.coll1.insert({name: 'testserversecuritythree'}));
                   assert.commandWorked(conn.adminCommand({oidcListKeys: 1}));
                   testDB.dropUser('fakeUser');
               });

    // Test that the refresh flow works as expected from the shell to the server.
    runRefreshFlowTest(conn,
                       'testserversecurityone@ping-test.com',
                       kExpectedTestServerSecurityOneAuthLog,
                       330 * 1000 /* sleep_time */);
}

{
    const shardedCluster = new ShardingTest({
        mongos: 1,
        config: 1,
        shards: 1,
        other: {mongosOptions: {setParameter: kPingFederateStartupOptions}},
        keyFile: 'jstests/libs/key1',
    });
    runTest(shardedCluster.s0);
    shardedCluster.stop();
}
