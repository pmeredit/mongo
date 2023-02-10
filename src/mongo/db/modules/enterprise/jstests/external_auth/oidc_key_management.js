// Test key refreshing and management for OIDC.
// @tags: [ featureFlagOIDC ]

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
const multipleKeys = assetsDir + '/custom-key-all.json';

const expectedMultipleKeys = [
    {
        "kid": "custom-key-1",
        "kty": "RSA",
        "n":
            "ALtUlNS31SzxwqMzMR9jKOJYDhHj8zZtLUYHi3s1en3wLdILp1Uy8O6Jy0Z66tPyM1u8lke0JK5gS-40yhJ-bvqioW8CnwbLSLPmzGNmZKdfIJ08Si8aEtrRXMxpDyz4Is7JLnpjIIUZ4lmqC3MnoZHd6qhhJb1v1Qy-QGlk4NJy1ZI0aPc_uNEUM7lWhPAJABZsWc6MN8flSWCnY8pJCdIk_cAktA0U17tuvVduuFX_94763nWYikZIMJS_cTQMMVxYNMf1xcNNOVFlUSJHYHClk46QT9nT8FWeFlgvvWhlXfhsp9aNAi3pX-KxIxqF2wABIAKnhlMa3CJW41323Js",
        "e": "AQAB"
    },
    {
        "kid": "custom-key-2",
        "kty": "RSA",
        "n":
            "ANBv7-YFoyL8EQVhig7yF8YJogUTW-qEkE81s_bs2CTsI1oepDFNAeMJ-Krfx1B7yllYAYtScZGo_l60R9Ou4X89LA66bnVRWVFCp1YV1r0UWtn5hJLlAbqKseSmjdwZlL_e420GlUAiyYsiIr6wltC1dFNYyykq62RhfYhM0xpnt0HiN-k71y9A0GO8H-dFU1WgOvEYMvHmDAZtAP6RTkALE3AXlIHNb4mkOc9gwwn-7cGBc08rufYcniKtS0ZHOtD1aE2CTi1MMQMKkqtVxWIdTI3wLJl1t966f9rBHR6qVtTV8Qpq1bquUc2oaHjR4lPTf0Z_hTaELJa5-BBbvJU",
        "e": "AQAB"
    }
];
const expectedSingleKey = [expectedMultipleKeys[0]];

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

// Startup parameters and constants.
const kOIDCConfig = [
    {
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
    },
    {
        issuer: issuerTwo,
        audience: 'jwt@kernel.mongodb.com',
        authNamePrefix: 'issuer2',
        matchPattern: '@10gen.com$',
        clientId: 'deadbeefcafe',
        authorizationClaim: 'mongodb-roles',
        JWKSPollSecs: issuerTwoRefreshIntervalSecs,
        deviceAuthorizationEndpoint: 'https://test.kernel.mongodb.com/oidc/device',
        JWKSUri: issuerTwoJWKSUri,
    }
];
const startupOptions = {
    authenticationMechanisms: 'SCRAM-SHA-256,MONGODB-OIDC',
    oidcIdentityProviders: tojson(kOIDCConfig),
};
const issuerOneKeyOneToken = kOIDCTokens['Token_OIDCAuth_user1'];
const issuerOneKeyTwoToken = kOIDCTokens['Token_OIDCAuth_user1_custom_key_2'];
const issuerTwoKeyOneToken = kOIDCTokens['Token_OIDCAuth_user1@10gen'];
const issuerTwoKeyTwoToken = kOIDCTokens['Token_OIDCAuth_user1@10gen_custom_key_2'];

// Set up the node for the test.
function setup(conn) {
    const adminDB = conn.getDB('admin');
    assert.commandWorked(conn.adminCommand({createUser: 'admin', 'pwd': 'admin', roles: ['root']}));
    assert(adminDB.auth('admin', 'admin'));

    // Create the roles corresponding to user1@mongodb.com and user1@10gen.com's groups.
    assert.commandWorked(conn.adminCommand(
        {createRole: 'issuer1/myReadRole', roles: ['readAnyDatabase'], privileges: []}));
    assert.commandWorked(
        conn.adminCommand({createRole: 'issuer2/myReadRole', roles: ['read'], privileges: []}));

    // Create a user with the hostManager role to run OIDC commands.
    assert.commandWorked(
        conn.adminCommand({createUser: 'oidcAdmin', 'pwd': 'oidcAdmin', roles: ['hostManager']}));

    // Increase logging verbosity.
    assert.commandWorked(adminDB.setLogLevel(3));
}

function compareKeys(actualKeys, expectedKeys) {
    assert.eq(actualKeys.length, expectedKeys.length);
    actualKeys.sort((firstKey, secondKey) => { firstKey.kid.localeCompare(secondKey.kid); });
    expectedKeys.sort((firstKey, secondKey) => { firstKey.kid.localeCompare(secondKey.kid); });
    for (let i = 0; i < expectedKeys.length; i++) {
        assert(bsonWoCompare(expectedKeys[i], actualKeys[i]) === 0);
    }
}

function testAddKey(conn) {
    // Initially, the key server for issuerOne has only custom-key-1. Tokens signed with that should
    // succeed auth but tokens signed with custom-key-2 should fail.
    const externalDB = conn.getDB('$external');
    assert(externalDB.auth({oidcAccessToken: issuerOneKeyOneToken, mechanism: 'MONGODB-OIDC'}));
    assert.commandWorked(conn.adminCommand({listDatabases: 1}));
    externalDB.logout();

    assert(!externalDB.auth({oidcAccessToken: issuerOneKeyTwoToken, mechanism: 'MONGODB-OIDC'}));
    assert.commandFailedWithCode(conn.adminCommand({listDatabases: 1}), ErrorCodes.Unauthorized);

    // Add custom-key-2 to issuerOne's key server endpoint.
    keyMap.issuerOne = multipleKeys;
    rotateKeys(keyMap);

    // Assert that auth with the token signed by custom-key-2 should succeed immediately thanks to
    // the JWKManager's refresh when it cannot initially find the key.
    assert(externalDB.auth({oidcAccessToken: issuerOneKeyTwoToken, mechanism: 'MONGODB-OIDC'}));
    assert.commandWorked(conn.adminCommand({listDatabases: 1}));
}

function testRemoveKey(conn) {
    // Initially, the key server for issuerTwo has both custom-key-1 and custom-key-2.
    // Tokens signed by either token should succeed auth.
    const externalDB = conn.getDB('$external');
    assert(externalDB.auth({oidcAccessToken: issuerTwoKeyOneToken, mechanism: 'MONGODB-OIDC'}));
    assert.commandWorked(conn.adminCommand({listDatabases: 1}));
    externalDB.logout();

    assert(externalDB.auth({oidcAccessToken: issuerTwoKeyTwoToken, mechanism: 'MONGODB-OIDC'}));
    assert.commandWorked(conn.adminCommand({listDatabases: 1}));

    // Remove custom-key-2 from issuerTwo's key server.
    keyMap.issuerTwo = singleKey;
    rotateKeys(keyMap);

    // Assert that the currently authenticated user should start receiving
    // ErrorCodes.ReauthenticationRequired within JWKSPollSecs + 10 (error margin). Once that
    // occurs, reauth with the token signed by custom-key-2 should fail but with custom-key-1 should
    // keep working.
    assert.soon(
        () => {
            try {
                assert.commandFailedWithCode(conn.adminCommand({listDatabases: 1}),
                                             ErrorCodes.ReauthenticationRequired);
                assert(!externalDB.auth(
                    {oidcAccessToken: issuerTwoKeyTwoToken, mechanism: 'MONGODB-OIDC'}));
                assert(externalDB.auth(
                    {oidcAccessToken: issuerTwoKeyOneToken, mechanism: 'MONGODB-OIDC'}));
                assert.commandWorked(conn.adminCommand({listDatabases: 1}));

                return true;
            } catch (e) {
                return false;
            }
        },
        "Tokens signed by removed key not fully invalidated",
        (issuerTwoRefreshIntervalSecs + 10) * 1000);
}

// Assert that key rotation is picked up via implicit refreshes.
function runJWKSetRefreshTest(conn) {
    const addKeyShell = new Mongo(conn.host);
    const removeKeyShell = new Mongo(conn.host);
    testAddKey(addKeyShell);
    testRemoveKey(removeKeyShell);
}

// Assert that oidcListKeys and oidcRefreshKeys function as expected.
function runKeyManagementCommandsTest(conn) {
    const oidcCommandsShell = new Mongo(conn.host);
    const adminDB = oidcCommandsShell.getDB('admin');
    assert(adminDB.auth('oidcAdmin', 'oidcAdmin'));

    // First, check that oidcListKeys without arguments returns all keys.
    let returnedOIDCKeys = assert.commandWorked(adminDB.runCommand({oidcListKeys: 1})).keySets;
    compareKeys(returnedOIDCKeys[issuerOne].keys, expectedMultipleKeys);
    compareKeys(returnedOIDCKeys[issuerTwo].keys, expectedSingleKey);

    // Then, rotate keys and force immediate refresh of all identity providers.
    keyMap.issuerOne = singleKey;
    keyMap.issuerTwo = multipleKeys;
    rotateKeys(keyMap);
    assert.commandWorked(adminDB.runCommand({oidcRefreshKeys: 1}));

    // Now, check that the updated keys are visible via oidcListKeys.
    returnedOIDCKeys = assert.commandWorked(adminDB.runCommand({oidcListKeys: 1})).keySets;
    compareKeys(returnedOIDCKeys[issuerOne].keys, expectedSingleKey);
    compareKeys(returnedOIDCKeys[issuerTwo].keys, expectedMultipleKeys);

    // Check that refreshing and listing keys for just a single identity provider is also possible.
    keyMap.issuerTwo = singleKey;
    rotateKeys(keyMap);
    assert.commandWorked(adminDB.runCommand({oidcRefreshKeys: 1, identityProviders: [issuerTwo]}));
    returnedOIDCKeys =
        assert.commandWorked(adminDB.runCommand({oidcListKeys: 1, identityProviders: [issuerTwo]}))
            .keySets;
    assert.eq(undefined, returnedOIDCKeys[issuerOne]);
    compareKeys(returnedOIDCKeys[issuerTwo].keys, expectedSingleKey);
}

// Assert that key server downtime is logged and updated keys can be force-refreshed as soon as the
// key server is brought back up.
function runKeyServerDowntimeProcedureTest(conn) {
    KeyServer.stop();
    checkLog.containsJson(conn, 7119501);

    KeyServer.start();
    keyMap.issuerOne = multipleKeys;
    rotateKeys(keyMap);

    assert.commandWorked(conn.adminCommand({oidcRefreshKeys: 1, identityProviders: [issuerOne]}));
    const returnedOIDCKeys =
        assert.commandWorked(conn.adminCommand({oidcListKeys: 1, identityProviders: [issuerOne]}))
            .keySets;
    assert.eq(undefined, returnedOIDCKeys[issuerTwo]);
    compareKeys(returnedOIDCKeys[issuerOne].keys, expectedMultipleKeys);
}

// Separate, dedicated mongod that's used to run httpClientRequest against the KeyServer for
// key rotation. This command requires authentication and is unsupported on mongos, so it's easier
// to centralize all the requests via this mongod rather than interleaving it in test logic.
const httpClientRequestMongod = MongoRunner.runMongod();
function rotateKeys(keyMap) {
    const keyRotationRequest = KeyServer.getURL() + '/rotateKeys?map=' + JSON.stringify(keyMap);
    assert.commandWorked(
        httpClientRequestMongod.adminCommand({httpClientRequest: 1, uri: keyRotationRequest}));
}

KeyServer.start();

{
    const mongod = MongoRunner.runMongod({auth: '', setParameter: startupOptions});
    setup(mongod);
    runJWKSetRefreshTest(mongod);
    runKeyManagementCommandsTest(mongod);
    runKeyServerDowntimeProcedureTest(mongod);
    MongoRunner.stopMongod(mongod);
}

// Ensure keys are rotated to the expected startup values.
keyMap.issuerOne = singleKey;
keyMap.issuerTwo = multipleKeys;
rotateKeys(keyMap);

{
    const shardedCluster = new ShardingTest({
        mongos: 1,
        config: 1,
        shards: 1,
        other: {mongosOptions: {setParameter: startupOptions}},
        keyFile: 'jstests/libs/key1',
    });
    setup(shardedCluster.s0);
    runJWKSetRefreshTest(shardedCluster.s0);
    runKeyManagementCommandsTest(shardedCluster.s0);
    runKeyServerDowntimeProcedureTest(shardedCluster.s0);
    shardedCluster.stop();
}

KeyServer.stop();
MongoRunner.stopMongod(httpClientRequestMongod);
})();
