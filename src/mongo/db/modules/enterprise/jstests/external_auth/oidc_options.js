// Test basic option parsing and key loading for OIDC.
// @tags: [ featureFlagOIDC ]

(function() {
'use strict';

load('jstests/ssl/libs/ssl_helpers.js');
load('src/mongo/db/modules/enterprise/jstests/external_auth/lib/oidc_key_server.js');

if (determineSSLProvider() !== 'openssl') {
    print('Skipping test, OIDC is only available with OpenSSL');
    return;
}

const kLoadedKey = 7070202;
const KeyServer = new OIDCKeyServer();
KeyServer.start();
const kJWKSURL = KeyServer.getURL();

function runTest(conn) {
    const expectKeys =
        JSON.parse(cat('src/mongo/db/modules/enterprise/jstests/external_auth/lib/oidc_keys.json'))
            .keys.map((k) => k.kid)
            .sort();
    const loadedKeys = checkLog.getGlobalLog(conn)
                           .map((l) => JSON.parse(l))
                           .filter((l) => l.id === kLoadedKey)
                           .map((l) => l.attr.kid)
                           .sort();
    assert.eq(expectKeys, loadedKeys, "Did not load expected keys");
}

function testOIDCConfig(config) {
    const opts = {
        setParameter: {
            // TODO SERVER-70955 SASL Server Mechanism
            // authenticationMechanisms: 'SCRAM-SHA-256,MONGODB-OIDC',
            logComponentVerbosity: tojson({accessControl: 3}),
            oidcIdentityProviders: tojson(config)
        }
    };
    const standalone = MongoRunner.runMongod(opts);
    standalone.oidcConfig = config;
    runTest(standalone);
    MongoRunner.stopMongod(standalone);
}

const kAllOptionsSetIDP = {
    issuer: 'https://test.kernel.mongodb.com/oidc/issuer',
    audience: 'mongodb-server',
    authNamePrefix: 'myPrefix',
    matchPattern: '@mongodb.com$',
    clientId: 'deadbeefcafe',
    clientSecret: 'hunter2',
    requestScopes: ['email'],
    principalName: 'sub',
    authorizationClaim: 'mongodb-roles',
    logClaims: ['sub'],
    auditClaims: ['iss', 'sub', 'email'],
    JWKSPollSecs: 86400,
    deviceAuthURL: 'https://test.kernel.mongodb.com/oidc/device',
    authURL: 'https://test.kernel.mongodb.com/oidc/auth',
    tokenURL: 'https://test.kernel.mongodb.com/oidc/token',
    JWKS: kJWKSURL,
};

testOIDCConfig([kAllOptionsSetIDP]);

KeyServer.stop();
})();
