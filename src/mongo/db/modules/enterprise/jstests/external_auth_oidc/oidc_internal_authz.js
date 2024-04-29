// Test OIDC authentication with internal authorization only when it should work.
// @tags: [ requires_fcv_70 ]

import {determineSSLProvider} from "jstests/ssl/libs/ssl_helpers.js";
import {
    OIDCKeyServer
} from "src/mongo/db/modules/enterprise/jstests/external_auth/lib/oidc_utils.js";
import {OIDCVars} from "src/mongo/db/modules/enterprise/jstests/external_auth/lib/oidc_vars.js";

if (determineSSLProvider() !== 'openssl') {
    print('Skipping test, OIDC is only available with OpenSSL');
    quit();
}

const LIB = 'src/mongo/db/modules/enterprise/jstests/external_auth/lib/';
const keyMap = {
    issuer1: LIB + '/custom-key-1.json',
    issuer2: LIB + '/custom-keys_1_2.json',
    issuer3: LIB + '/custom-key-3.json',
};
const KeyServer = new OIDCKeyServer(JSON.stringify(keyMap));
const kOIDCTokens = OIDCVars(KeyServer.getURL()).kOIDCTokens;
const issuer1 = KeyServer.getURL() + '/issuer1';
const issuer2 = KeyServer.getURL() + '/issuer2';
const issuer3 = KeyServer.getURL() + '/issuer3';

function runAdminCommand(conn, db, command) {
    const adminDB = conn.getDB('admin');
    assert(adminDB.auth('admin', 'pwd'));
    assert.commandWorked(conn.getDB(db).runCommand(command));
    adminDB.logout();
}

function assertAuthSuccessful(conn, expectedDB, expectedUser, expectedRoles) {
    const connStatus = assert.commandWorked(conn.adminCommand({connectionStatus: 1}));
    assert.eq(connStatus.authInfo.authenticatedUsers.length, 1);
    assert.eq(connStatus.authInfo.authenticatedUsers[0].db, expectedDB);
    assert.eq(connStatus.authInfo.authenticatedUsers[0].user, expectedUser);
    let sortedActualRoles =
        connStatus.authInfo.authenticatedUserRoles.map((r) => r.db + '.' + r.role).sort();
    let sortedExpectRoles = expectedRoles.map((r) => 'admin.' + r).sort();
    assert.eq(sortedActualRoles, sortedExpectRoles);
}

function runTest(conn) {
    // Create role documents corresponding to the roles specified in the tokens.
    assert.commandWorked(conn.adminCommand({createUser: 'admin', pwd: 'pwd', roles: ['root']}));
    const adminDB = conn.getDB('admin');
    runAdminCommand(conn,
                    adminDB,
                    {createRole: 'issuer1/myReadRole', roles: ['readAnyDatabase'], privileges: []});
    runAdminCommand(conn,
                    adminDB,
                    {createRole: 'issuer2/myReadRole', roles: ['readAnyDatabase'], privileges: []});

    // Auth with a token issued by issuer1. This should cause the connection to be authorized with
    // the roles from the token's authorizationClaim.
    const external = conn.getDB('$external');
    const issuerOneToken = kOIDCTokens['Token_OIDCAuth_user1'];
    assert(external.auth({oidcAccessToken: issuerOneToken, mechanism: 'MONGODB-OIDC'}));

    const issuerOneExpectedRoles = ['issuer1/myReadRole', 'readAnyDatabase'];
    assertAuthSuccessful(conn, '$external', 'issuer1/user1@mongodb.com', issuerOneExpectedRoles);
    external.logout();

    // Auth with a token issued by issuer2. This should fail since OIDC authorization is disabled
    // and there is no user document on-disk corresponding to the token's principal.
    const issuerTwoToken = kOIDCTokens['Token_OIDCAuth_user1@10gen'];
    const issuerTwoExpectedRoles = ['readWriteAnyDatabase'];
    assert(!external.auth({oidcAccessToken: issuerTwoToken, mechanism: 'MONGODB-OIDC'}));

    // Create a user document for the token's principal and ensure that the connection is authorized
    // with the roles corresponding to that user.
    runAdminCommand(conn, external, {
        createUser: 'issuer2/user1@10gen.com',
        roles: [{role: 'readWriteAnyDatabase', db: 'admin'}]
    });
    assert(external.auth({oidcAccessToken: issuerTwoToken, mechanism: 'MONGODB-OIDC'}));
    assertAuthSuccessful(conn, '$external', 'issuer2/user1@10gen.com', issuerTwoExpectedRoles);
    external.logout();
}

// Issuer1 has an authorizationClaim, so it uses OIDC authz, while issuer2 has useAuthorizationClaim
// set to false, so it should use internal authorization.
KeyServer.start();
{
    jsTestLog('Testing valid issuer with useAuthorizationClaim=false and no authorizationClaim');
    const validOIDCConfig = [
        {
            issuer: issuer1,
            audience: 'jwt@kernel.mongodb.com',
            authNamePrefix: 'issuer1',
            matchPattern: '@mongodb.com$',
            clientId: 'deadbeefcafe',
            requestScopes: ['email'],
            principalName: 'sub',
            authorizationClaim: 'mongodb-roles',
            logClaims: ['sub', 'aud', 'mongodb-roles', 'does-not-exist'],
            JWKSPollSecs: 86400,
        },
        {
            issuer: issuer2,
            audience: 'jwt@kernel.mongodb.com',
            authNamePrefix: 'issuer2',
            matchPattern: '@10gen.com$',
            supportsHumanFlows: false,
            useAuthorizationClaim: false,
            JWKSPollSecs: 86400,
        }
    ];
    const startupOptions = {
        authenticationMechanisms: 'SCRAM-SHA-256,MONGODB-OIDC',
        oidcIdentityProviders: tojson(validOIDCConfig),
        JWKSMinimumQuiescePeriodSecs: 0,
    };

    const mongod = MongoRunner.runMongod({auth: '', setParameter: startupOptions});
    runTest(mongod);
    MongoRunner.stopMongod(mongod);

    const shardedCluster = new ShardingTest({
        mongos: 1,
        config: 1,
        shards: 1,
        other: {mongosOptions: {setParameter: startupOptions}},
        keyFile: 'jstests/libs/key1',
    });
    runTest(shardedCluster.s0);
    shardedCluster.stop();
}

{
    jsTestLog('Testing authentication with matchless configurations');
    const validOIDCConfig = [
        {
            issuer: issuer1,
            audience: 'jwt@kernel.mongodb.com',
            authNamePrefix: 'issuer1',
            clientId: 'deadbeefcafe',
            requestScopes: ['email'],
            principalName: 'sub',
            authorizationClaim: 'mongodb-roles',
            logClaims: ['sub', 'aud', 'mongodb-roles', 'does-not-exist'],
            JWKSPollSecs: 86400,
        },
        {
            issuer: issuer2,
            audience: 'jwt@kernel.mongodb.com',
            authNamePrefix: 'issuer2',
            supportsHumanFlows: false,
            useAuthorizationClaim: false,
            JWKSPollSecs: 86400,
        }
    ];
    const startupOptions = {
        authenticationMechanisms: 'SCRAM-SHA-256,MONGODB-OIDC',
        oidcIdentityProviders: tojson(validOIDCConfig),
        JWKSMinimumQuiescePeriodSecs: 0,
    };

    const mongod = MongoRunner.runMongod({auth: '', setParameter: startupOptions});
    runTest(mongod);
    MongoRunner.stopMongod(mongod);

    const shardedCluster = new ShardingTest({
        mongos: 1,
        config: 1,
        shards: 1,
        other: {mongosOptions: {setParameter: startupOptions}},
        keyFile: 'jstests/libs/key1',
    });
    runTest(shardedCluster.s0);
    shardedCluster.stop();
}

// Same test as above except that issuer2 also has authorizationClaim specified while
// useAuthorizationClaim is false. This is a valid configuration but causes the server to ignore
// authorizationClaim and use internal authorization instead.
{
    jsTestLog(
        'Testing valid issuer with useAuthorizationClaim=false and non-empty authorizationClaim');
    const validOIDCConfig = [
        {
            issuer: issuer1,
            audience: 'jwt@kernel.mongodb.com',
            authNamePrefix: 'issuer1',
            matchPattern: '@mongodb.com$',
            clientId: 'deadbeefcafe',
            requestScopes: ['email'],
            principalName: 'sub',
            authorizationClaim: 'mongodb-roles',
            logClaims: ['sub', 'aud', 'mongodb-roles', 'does-not-exist'],
            JWKSPollSecs: 86400,
        },
        {
            issuer: issuer2,
            audience: 'jwt@kernel.mongodb.com',
            authNamePrefix: 'issuer2',
            matchPattern: '@10gen.com$',
            clientId: 'deadbeefcafe',
            useAuthorizationClaim: false,
            authorizationClaim: 'mongodb-roles',
            JWKSPollSecs: 86400,
        }
    ];
    const startupOptions = {
        authenticationMechanisms: 'SCRAM-SHA-256,MONGODB-OIDC',
        oidcIdentityProviders: tojson(validOIDCConfig),
        JWKSMinimumQuiescePeriodSecs: 0,
    };

    const mongod = MongoRunner.runMongod({auth: '', setParameter: startupOptions});
    runTest(mongod);
    MongoRunner.stopMongod(mongod);

    const shardedCluster = new ShardingTest({
        mongos: 1,
        config: 1,
        shards: 1,
        other: {mongosOptions: {setParameter: startupOptions}},
        keyFile: 'jstests/libs/key1',
    });
    runTest(shardedCluster.s0);
    shardedCluster.stop();
}

{
    jsTestLog(
        'Testing that a server may not start with an invalid issuer set containing two human-flow IdPs without matchPatterns');
    const invalidOIDCConfig = [
        {
            issuer: issuer1,
            audience: 'jwt@kernel.mongodb.com',
            authNamePrefix: 'issuer1',
            clientId: 'deadbeefcafe',
            requestScopes: ['email'],
            principalName: 'sub',
            authorizationClaim: 'mongodb-roles',
            logClaims: ['sub', 'aud', 'mongodb-roles', 'does-not-exist'],
            JWKSPollSecs: 86400,
        },
        {
            issuer: issuer3,
            audience: 'jwt@kernel.mongodb.com',
            authNamePrefix: 'issuer3',
            clientId: 'deadbeefcafe',
            requestScopes: ['email'],
            principalName: 'sub',
            authorizationClaim: 'mongodb-roles',
            logClaims: ['sub', 'aud', 'mongodb-roles', 'does-not-exist'],
            JWKSPollSecs: 86400,
        },
        {
            issuer: issuer2,
            audience: 'jwt@kernel.mongodb.com',
            authNamePrefix: 'issuer2',
            useAuthorizationClaim: false,
            supportsHumanFlows: false,
            JWKSPollSecs: 86400,
        }
    ];
    const startupOptions = {
        authenticationMechanisms: 'SCRAM-SHA-256,MONGODB-OIDC',
        oidcIdentityProviders: tojson(invalidOIDCConfig),
        JWKSMinimumQuiescePeriodSecs: 0,
    };
    assert.throws(() => MongoRunner.runMongod({auth: '', setParameter: startupOptions}));
}

// Omitting authorizationClaim without explicitly setting useAuthorizationClaim to false prevents
// the server from starting up.
{
    jsTestLog('Testing invalid issuer with no authorizationClaim or useAuthorizationClaim');
    const invalidOIDCConfig = [
        {
            issuer: issuer1,
            audience: 'jwt@kernel.mongodb.com',
            authNamePrefix: 'issuer1',
            matchPattern: '@mongodb.com$',
            clientId: 'deadbeefcafe',
            requestScopes: ['email'],
            principalName: 'sub',
            authorizationClaim: 'mongodb-roles',
            logClaims: ['sub', 'aud', 'mongodb-roles', 'does-not-exist'],
            JWKSPollSecs: 86400,
        },
        {
            issuer: issuer2,
            audience: 'jwt@kernel.mongodb.com',
            authNamePrefix: 'issuer2',
            matchPattern: '@10gen.com$',
            clientId: 'deadbeefcafe',
            JWKSPollSecs: 86400,
        }
    ];
    const startupOptions = {
        authenticationMechanisms: 'SCRAM-SHA-256,MONGODB-OIDC',
        oidcIdentityProviders: tojson(invalidOIDCConfig),
        JWKSMinimumQuiescePeriodSecs: 0,
    };
    assert.throws(() => MongoRunner.runMongod({auth: '', setParameter: startupOptions}));
}
KeyServer.stop();
