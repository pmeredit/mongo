// Tests end-to-end OIDC authentication with Azure. Shell uses device authorization grant flow to
// acquire tokens.
// @tags: [ requires_fcv_70 ]

import {getPython3Binary} from "jstests/libs/python.js";
import {determineSSLProvider} from "jstests/ssl/libs/ssl_helpers.js";
import {
    authAsUser
} from "src/mongo/db/modules/enterprise/jstests/external_auth_oidc/lib/oidc_e2e_lib.js";

if (determineSSLProvider() !== 'openssl') {
    print('Skipping test, OIDC is only available with OpenSSL');
    quit();
}

// We have two applications registered in the Azure AD portal:
// - OIDC_EVG_TESTING (client id: 67b54975-2545-4fad-89cf-1dbd174d2e60)
// - OIDC_EVG_TESTING_ALT (client id: 19f0245e-a4c6-48dd-91a4-d9755f67b42c)
// Azure will issue tokens with identical "issuer" claims for both applications.
// Any of the three test Users (tD548GwE[1,2,3]@outlook.com) may be used to authorize
// either application, but in the test below, tD548GwE1 is used to authorize OIDC_EVG_TESTING_ALT,
// while the other two users are used to authorize OIDC_EVG_TESTING.
const kUseAltAudience = true;
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
if (kUseAltAudience) {
    kAzureConfig.unshift({
        issuer: 'https://login.microsoftonline.com/c96563a8-841b-4ef9-af16-33548de0c958/v2.0',
        audience: '19f0245e-a4c6-48dd-91a4-d9755f67b42c',
        authNamePrefix: 'issuerAzureAlt',
        matchPattern: '1@outlook.com$',
        clientId: '19f0245e-a4c6-48dd-91a4-d9755f67b42c',
        requestScopes: ['api://19f0245e-a4c6-48dd-91a4-d9755f67b42c/email'],
        principalName: 'preferred_username',
        authorizationClaim: 'groups',
        logClaims: ['sub', 'aud', 'groups'],
        JWKSPollSecs: 86400,
    });
}
const kAltAuthNamePrefix = kUseAltAudience ? "issuerAzureAlt" : "issuerAzure";

const kAzureStartupOptions = {
    authenticationMechanisms: 'SCRAM-SHA-256,MONGODB-OIDC',
    logComponentVerbosity: tojson({accessControl: 3}),
    oidcIdentityProviders: tojson(kAzureConfig),
};
const kExpectedTestServerSecurityOneRoles = [
    `admin.${kAltAuthNamePrefix}/21c0bd78-e5a7-4cee-91e6-f559e20cbcba`,
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
    assert.commandWorked(conn.adminCommand({
        createRole: 'issuerAzureAlt/21c0bd78-e5a7-4cee-91e6-f559e20cbcba',
        roles: [
            {role: 'readWrite', db: 'test'},
            {role: 'hostManager', db: 'admin'},
            {role: 'read', db: 'test'}
        ],
        privileges: []
    }));
    adminDB.logout();

    const pythonBinary = getPython3Binary();

    // Set the OIDC IdP auth callback function.
    conn._setOIDCIdPAuthCallback(`function() {
        runNonMongoProgram('${pythonBinary}',
                           'jstests/auth/lib/automated_idp_authn_simulator_azure.py',
                           '--activationEndpoint',
                           this.activationEndpoint,
                           '--userCode',
                           this.userCode,
                           '--username',
                           this.userName,
                           '--setupFile',
                           'azure_e2e_config.json');
    }`);

    // Auth as tD548GwE@outlook.com. They should have readWrite and hostManager
    // roles.
    const testDB = conn.getDB('test');
    authAsUser(conn,
               'tD548GwE1@outlook.com',
               kAltAuthNamePrefix,
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

    // Auth as tD548GwE2@outlook.com. They should have readWrite and userAdmin roles.
    authAsUser(
        conn, 'tD548GwE2@outlook.com', 'issuerAzure', kExpectedTestServerSecurityTwoRoles, () => {
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
    authAsUser(
        conn, 'tD548GwE3@outlook.com', 'issuerAzure', kExpectedTestServerSecurityThreeRoles, () => {
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
