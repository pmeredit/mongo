// Assert that OIDC auth attempts are rejected in strict stable API mode.
// @tags: [ requires_fcv_70 ]

import {determineSSLProvider} from "jstests/ssl/libs/ssl_helpers.js";
import {
    OIDCgenerateBSON,
    OIDCKeyServer
} from "src/mongo/db/modules/enterprise/jstests/external_auth/lib/oidc_utils.js";

if (determineSSLProvider() !== 'openssl') {
    print('Skipping test, OIDC is only available with OpenSSL');
    quit();
}

const LIB = 'src/mongo/db/modules/enterprise/jstests/external_auth/lib/';
const keyMap = {
    issuer1: LIB + '/custom-key-1.json',
};

const KeyServer = new OIDCKeyServer(JSON.stringify(keyMap));
KeyServer.start();

const oidcConfig = [{
    issuer: KeyServer.getURL() + '/issuer1',
    audience: 'jwt@kernel.mongodb.com',
    authNamePrefix: 'issuer1',
    matchPattern: '@mongodb.com$',
    clientId: 'deadbeefcafe',
    requestScopes: ['email'],
    principalName: 'sub',
    authorizationClaim: 'mongodb-roles',
    logClaims: ['sub', 'aud', 'mongodb-roles', 'does-not-exist'],
    JWKSPollSecs: 86400,
}];
const params = {
    authenticationMechanisms: 'SCRAM-SHA-256,MONGODB-OIDC',
    logComponentVerbosity: tojson({accessControl: 3}),
    oidcIdentityProviders: tojson(oidcConfig)
};

const standalone = MongoRunner.runMongod({setParameter: params});

const mongo = new Mongo(standalone.host);
const external = mongo.getDB("$external");
assert.commandFailedWithCode(external.runCommand({
    saslStart: 1,
    mechanism: 'MONGODB-OIDC',
    payload: OIDCgenerateBSON({n: "user1@mongodb.com"}),
    apiVersion: "1",
    apiStrict: true
}),
                             ErrorCodes.MechanismUnavailable);

MongoRunner.stopMongod(standalone);
KeyServer.stop();
