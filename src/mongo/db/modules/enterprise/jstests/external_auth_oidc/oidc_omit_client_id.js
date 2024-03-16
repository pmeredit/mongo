// Tests that the server can be configured without clientIDs for certain IdPs.
// @tags: [ requires_fcv_70 ]

import {determineSSLProvider} from "jstests/ssl/libs/ssl_helpers.js";
import {
    OIDCgenerateBSON,
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
};
const KeyServer = new OIDCKeyServer(JSON.stringify(keyMap));
const kOIDCPayloads = OIDCVars(KeyServer.getURL()).kOIDCPayloads;
const issuer1 = KeyServer.getURL() + '/issuer1';
const issuer2 = KeyServer.getURL() + '/issuer2';

const expectedIssuer1Payload = OIDCgenerateBSON({
    issuer: issuer1,
    clientId: 'deadbeefcafe',
});
const expectedIssuer2Payload = OIDCgenerateBSON({
    issuer: issuer2,
});

function runTest(conn) {
    // Run saslStart while providing a principal name hint that matches issuer1.
    const external = conn.getDB('$external');
    const issuerOneReply = assert.commandWorked(external.runCommand({
        saslStart: 1,
        mechanism: 'MONGODB-OIDC',
        payload: kOIDCPayloads['Advertize_OIDCAuth_user1']
    }));

    // Assert that the reply matches the expected payload, which includes a clientID.
    assert.eq(issuerOneReply.payload, expectedIssuer1Payload);

    // Run saslStart while providing a principal name hint that matches issuer2.
    const issuerTwoReply = assert.commandWorked(external.runCommand({
        saslStart: 1,
        mechanism: 'MONGODB-OIDC',
        payload: kOIDCPayloads['Advertize_OIDCAuth_user1@10gen']
    }));

    // Assert that the reply matches the expected payload, which omits the clientID.
    assert.eq(issuerTwoReply.payload, expectedIssuer2Payload);
}

// Issuer1 has a clientID, so the server includes it in its SASL reply, while issuer2 has
// supportsHumanFlows set to false, so it should omit it.
KeyServer.start();
{
    jsTestLog('Testing valid issuer with supportsHumanFlows=false and no clientId');
    const validOIDCConfig = [
        {
            issuer: issuer1,
            audience: 'jwt@kernel.mongodb.com',
            authNamePrefix: 'issuer1',
            matchPattern: '@mongodb.com$',
            clientId: 'deadbeefcafe',
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
            authorizationClaim: 'mongodb-roles',
            JWKSPollSecs: 86400,
        }
    ];
    const startupOptions = {
        authenticationMechanisms: 'SCRAM-SHA-256,MONGODB-OIDC',
        oidcIdentityProviders: tojson(validOIDCConfig),
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

// Same test as above except that issuer2 also has the clientID specified while
// supportsHumanFlows is false. This is a valid configuration but causes the server to ignore
// the supplied clientID and not include it in its SASL reply anyway.
{
    jsTestLog('Testing valid issuer with supportsHumanFlows=false and non-empty clientID');
    const validOIDCConfig = [
        {
            issuer: issuer1,
            audience: 'jwt@kernel.mongodb.com',
            authNamePrefix: 'issuer1',
            matchPattern: '@mongodb.com$',
            clientId: 'deadbeefcafe',
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
            clientId: 'deadbeefcafe',
            authorizationClaim: 'mongodb-roles',
            JWKSPollSecs: 86400,
        }
    ];
    const startupOptions = {
        authenticationMechanisms: 'SCRAM-SHA-256,MONGODB-OIDC',
        oidcIdentityProviders: tojson(validOIDCConfig),
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

// Omitting clientID without explicitly setting supportsHumanFlows to false prevents
// the server from starting up.
{
    jsTestLog('Testing invalid issuer with no clientID or supportsHumanFlows');
    const invalidOIDCConfig = [
        {
            issuer: issuer1,
            audience: 'jwt@kernel.mongodb.com',
            authNamePrefix: 'issuer1',
            matchPattern: '@mongodb.com$',
            clientId: 'deadbeefcafe',
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
            JWKSPollSecs: 86400,
        }
    ];
    const startupOptions = {
        authenticationMechanisms: 'SCRAM-SHA-256,MONGODB-OIDC',
        oidcIdentityProviders: tojson(invalidOIDCConfig),
    };
    assert.throws(() => MongoRunner.runMongod({auth: '', setParameter: startupOptions}));
}

{
    jsTestLog('Testing all configs have common issuer but varied in clientId');
    const validOIDCConfig = [
        {
            issuer: issuer1,
            audience: 'jwt-h1@kernel.mongodb.com',
            authNamePrefix: 'issuer1-h1',
            matchPattern: "h1@mongodb.com$",
            clientId: 'h1_deadbeefcafe',
            authorizationClaim: 'mongodb-roles',
        },
        {
            issuer: issuer1,
            audience: 'jwt-h2@kernel.10gen.com',
            authNamePrefix: 'issuer1-h2',
            matchPattern: "h2@mongodb.com$",
            clientId: 'h2_deadbeefcafe',
            authorizationClaim: 'mongodb-roles',
        },
        {
            issuer: issuer1,
            audience: 'jwt-m1@kernel.mongodb.com',
            authNamePrefix: 'issuer1-m1',
            matchPattern: "m1@mongodb.com$",
            supportsHumanFlows: false,
            authorizationClaim: 'mongodb-roles',
        },
        {
            issuer: issuer1,
            audience: 'jwt-m2@kernel.10gen.com',
            authNamePrefix: 'issuer1-m2',
            supportsHumanFlows: false,
            clientId: 'irrelevant',
            authorizationClaim: 'mongodb-roles',
        }
    ];
    const startupOptions = {
        authenticationMechanisms: 'SCRAM-SHA-256,MONGODB-OIDC',
        oidcIdentityProviders: tojson(validOIDCConfig),
    };

    const runTest = (conn) => {
        const external = conn.getDB('$external');
        const tests = [
            {principal: "user1-h1@mongodb.com", clientId: 'h1_deadbeefcafe'},
            {principal: "user1-h2@mongodb.com", clientId: 'h2_deadbeefcafe'},
            {principal: "user1-m1@mongodb.com"},
            {principal: "user1-m2@mongodb.com"},
        ];
        for (let test of tests) {
            jsTestLog(`Testing with principal ${test.principal}`);
            const reply = assert.commandWorked(external.runCommand({
                saslStart: 1,
                mechanism: 'MONGODB-OIDC',
                payload: OIDCgenerateBSON({
                    "n": test.principal,
                })
            }));
            const expectedPayload = OIDCgenerateBSON({issuer: issuer1, clientId: test.clientId});
            assert.eq(reply.payload, expectedPayload);
        }
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

KeyServer.stop();
