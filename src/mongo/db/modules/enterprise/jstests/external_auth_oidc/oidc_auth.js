// Test basic option parsing and key loading for OIDC.
// @tags: [ requires_fcv_70 ]

import {determineSSLProvider} from "jstests/ssl/libs/ssl_helpers.js";
import {
    isOIDCMultipurposeIDPEnabled,
    OIDCgenerateBSON,
    OIDCKeyServer,
    OIDCsignJWT
} from "src/mongo/db/modules/enterprise/jstests/external_auth/lib/oidc_utils.js";
import {OIDCVars} from "src/mongo/db/modules/enterprise/jstests/external_auth/lib/oidc_vars.js";

if (determineSSLProvider() !== 'openssl') {
    print('Skipping test, OIDC is only available with OpenSSL');
    quit();
}

const kAuthFailed = 5286307;
const kAuthSuccess = 5286306;
const kLoadedKey = 7070202;

const LIB = 'src/mongo/db/modules/enterprise/jstests/external_auth/lib/';
const keyMap = {
    issuer1: LIB + '/custom-key-1.json',
    issuer2: LIB + '/custom-key-2.json',
};
const KeyServer = new OIDCKeyServer(JSON.stringify(keyMap));
KeyServer.start();

const kOIDCPayloads = OIDCVars(KeyServer.getURL()).kOIDCPayloads;

function OIDCpayload(key) {
    assert(kOIDCPayloads[key] !== undefined, "Unknown OIDC payload '" + key + "'");
    jsTest.log(kOIDCPayloads[key]);
    return kOIDCPayloads[key];
}

function assertSameRoles(actualRoles, expectRoles) {
    const sortedActualRoles = actualRoles.map((r) => r.db + '.' + r.role).sort();
    const sortedExpectRoles = expectRoles.map((r) => 'admin.' + r).sort();
    assert.eq(sortedActualRoles, sortedExpectRoles);
}

function testAuth(mainConn, config, testCase) {
    const conn = new Mongo(mainConn.host);
    const external = conn.getDB('$external');
    let authnCmd = {saslStart: 1, mechanism: 'MONGODB-OIDC'};

    jsTest.log('Test case: ' + tojson(testCase));
    if (testCase.step1) {
        const reply = assert.commandWorked(external.runCommand(
            {saslStart: 1, mechanism: 'MONGODB-OIDC', payload: testCase.step1}));
        jsTest.log(reply);
        authnCmd = {saslContinue: 1, conversationId: NumberInt(reply.conversationId)};
    }

    const result = assert.commandWorked(
        external.runCommand(Object.extend(authnCmd, {payload: testCase.step2})));
    jsTest.log(result);

    const loggedClaims = checkLog.getGlobalLog(mainConn)
                             .map((l) => JSON.parse(l))
                             .filter((l) => l.id === kAuthSuccess)
                             .pop()
                             .attr.extraInfo.claims;
    assert(!bsonWoCompare(testCase.claims, loggedClaims),
           "Authentication did not log expected claims: " + tojson(testCase.claims) +
               " != " + tojson(loggedClaims));

    const connStatus = assert.commandWorked(conn.adminCommand({connectionStatus: 1}));
    jsTest.log(connStatus);
    assert.eq(connStatus.authInfo.authenticatedUsers.length, 1);
    assert.eq(connStatus.authInfo.authenticatedUsers[0].db, '$external');
    assert.eq(connStatus.authInfo.authenticatedUsers[0].user, testCase.user);
    assertSameRoles(connStatus.authInfo.authenticatedUserRoles, testCase.roles);

    return conn;
}

function assertThrowsAndLogsAuthFailure(conn, func, msg) {
    const failuresBefore = checkLog.getGlobalLog(conn)
                               .map((l) => JSON.parse(l))
                               .filter((l) => l.id === kAuthFailed)
                               .map((l) => l.attr.error)
                               .filter((e) => e === msg);
    assert.throwsWithCode(func, ErrorCodes.AuthenticationFailed);
    const failuresAfter = checkLog.getGlobalLog(conn)
                              .map((l) => JSON.parse(l))
                              .filter((l) => l.id === kAuthFailed)
                              .map((l) => l.attr.error)
                              .filter((e) => e === msg);
    assert.gt(
        failuresAfter.length, failuresBefore.length, "No new failure reported with msg: " + msg);
}

function checkKeysLoaded(conn, testCases) {
    let expectKeys = [];
    testCases.forEach(function(t) {
        expectKeys = expectKeys.concat(JSON.parse(cat(t.expectKeysFrom)).keys.map((k) => k.kid));
    });
    expectKeys.sort();
    const loadedKeys = checkLog.getGlobalLog(conn)
                           .map((l) => JSON.parse(l))
                           .filter((l) => l.id === kLoadedKey)
                           .map((l) => l.attr.kid)
                           .sort();
    assert.eq(expectKeys, loadedKeys, "Did not load expected keys");
}

function runTest(conn, configs, testCases) {
    const admin = conn.getDB('admin');

    assert.commandWorked(admin.runCommand({createUser: 'admin', 'pwd': 'admin', roles: ['root']}));
    assert(admin.auth('admin', 'admin'));
    checkKeysLoaded(conn, testCases);
    testCases.forEach(function(test) {
        const config = configs.filter((c) => c.issuer === test.issuer)[0];
        test.setup(conn, config);
        test.testCases.forEach(function(testCase) {
            if ((typeof testCase) === 'function') {
                testCase(conn, config);
            } else if (testCase.failure === undefined) {
                testAuth(conn, config, testCase);
            } else {
                jsTest.log('Looking for failure message: ' + testCase.failure);
                assertThrowsAndLogsAuthFailure(conn, function() {
                    testAuth(conn, config, testCase);
                }, testCase.failure);
            }
        });
    });
}

function testOIDCConfig(configs, testCases) {
    const params = {
        authenticationMechanisms: 'SCRAM-SHA-256,MONGODB-OIDC',
        logComponentVerbosity: tojson({accessControl: 3}),
        oidcIdentityProviders: tojson(configs)
    };

    jsTest.log('BEGIN standalone');
    const standalone = MongoRunner.runMongod({auth: '', setParameter: params});
    runTest(standalone, configs, testCases);
    MongoRunner.stopMongod(standalone);
    jsTest.log('END standalone');

    jsTest.log('BEGIN sharding');
    const sharding = new ShardingTest({
        mongos: 1,
        config: 1,
        shards: 1,
        other: {mongosOptions: {setParameter: params}},
        keyFile: 'jstests/libs/key1',
    });
    runTest(sharding.s0, configs, testCases);
    sharding.stop();
    jsTest.log('END sharding');
}

const kOIDCConfig = [
    {
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
    },
    {
        issuer: KeyServer.getURL() + '/issuer2',
        audience: 'jwt@kernel.mongodb.com',
        authNamePrefix: 'issuer2',
        matchPattern: '@10gen.com$',
        clientId: 'deadbeefcafe',
        authorizationClaim: 'mongodb-roles',
        JWKSPollSecs: 86400,
    }
];

const kBasicUser1SuccessCase = {
    step1: OIDCpayload('Advertize_OIDCAuth_user1'),
    step2: OIDCpayload('Authenticate_OIDCAuth_user1'),
    user: 'issuer1/user1@mongodb.com',
    roles: ['issuer1/myReadRole', 'readAnyDatabase'],
    claims:
        {sub: 'user1@mongodb.com', aud: ['jwt@kernel.mongodb.com'], "mongodb-roles": ['myReadRole']}
};

const kOIDCTestCases = [
    {
        issuer: "https://test.kernel.mongodb.com/oidc/issuer1",
        expectKeysFrom: LIB + '/custom-key-1.json',
        setup: function(conn) {
            assert.commandWorked(conn.adminCommand(
                {createRole: 'issuer1/myReadRole', roles: ['readAnyDatabase'], privileges: []}));
            assert.commandWorked(conn.adminCommand({
                createRole: 'issuer1/myReadWriteRole',
                roles: ['readWriteAnyDatabase'],
                privileges: []
            }));
        },

        testCases: [
            // Premade tokens from oidc_var.js
            kBasicUser1SuccessCase,
            {
                step1: OIDCpayload('Advertize_OIDCAuth_user2'),
                step2: OIDCpayload('Authenticate_OIDCAuth_user2'),
                user: 'issuer1/user2@mongodb.com',
                roles: ['issuer1/myReadWriteRole', 'readWriteAnyDatabase'],
                claims: {
                    sub: 'user2@mongodb.com',
                    aud: ['jwt@kernel.mongodb.com'],
                    "mongodb-roles": ['myReadWriteRole']
                }
            },

            // One-shot authentication.
            Object.assign({}, kBasicUser1SuccessCase, {step1: null}),

            // Using signature algorithm RS384 and RS512.
            Object.assign({},
                          kBasicUser1SuccessCase,
                          {step2: OIDCpayload('Authenticate_OIDCAuth_user1_RS384')}),
            Object.assign({},
                          kBasicUser1SuccessCase,
                          {step2: OIDCpayload('Authenticate_OIDCAuth_user1_RS512')}),

            // Change of principal name.
            {
                failure:
                    "BadValue: Principal name changed between step1 'user1@mongodb.com' and step2 'user2@mongodb.com'",
                step1: OIDCpayload('Advertize_OIDCAuth_user1'),
                step2: OIDCpayload('Authenticate_OIDCAuth_user2')
            },

            // Expired token.
            {
                failure: "BadValue: Token is expired",
                step1: OIDCpayload('Advertize_OIDCAuth_user1'),
                step2: OIDCpayload('Authenticate_OIDCAuth_user1_expired')
            },

            // Token not valid yet.
            {
                failure: "BadValue: Token not yet valid",
                step1: OIDCpayload('Advertize_OIDCAuth_user1'),
                step2: OIDCpayload('Authenticate_OIDCAuth_user1_not_yet_valid')
            },

            // nbf field omitted (still valid).
            Object.assign({},
                          kBasicUser1SuccessCase,
                          {step2: OIDCpayload('Authenticate_OIDCAuth_user1_no_nbf_field')}),

            // Unknown issuer.
            {
                failure:
                    `BadValue: Token issuer 'https://test.kernel.10gen.com/oidc/issuerX' does not match that inferred from principal name hint '${
                        KeyServer.getURL()}/issuer1'`,
                step1: OIDCpayload('Advertize_OIDCAuth_user1'),
                step2: OIDCpayload('Authenticate_OIDCAuth_user1_wrong_issuer')
            },

            // Issued to another audience.
            function(conn, config) {
                const test = {
                    failure:
                        "BadValue: Token audience 'jwt@kernel.10gen.com' does not match that inferred from principal name hint 'jwt@kernel.mongodb.com'",
                    step1: OIDCpayload('Advertize_OIDCAuth_user1'),
                    step2: OIDCpayload('Authenticate_OIDCAuth_user1_alt_audience')
                };

                if (!isOIDCMultipurposeIDPEnabled()) {
                    test.failure =
                        "BadValue: OIDC token issued for invalid audience. Got: 'jwt@kernel.10gen.com', expected: 'jwt@kernel.mongodb.com'";
                }

                jsTest.log('Looking for failure message: ' + test.failure);
                assertThrowsAndLogsAuthFailure(conn, function() {
                    testAuth(conn, config, test);
                }, test.failure);
            },

            // Issued to multiple audiences.
            {
                failure: "BadValue: OIDC token must contain exactly one audience",
                step1: OIDCpayload('Advertize_OIDCAuth_user1'),
                step2: OIDCpayload('Authenticate_OIDCAuth_user1_multiple_audiences')
            },

            // Issued to empty audience.
            {
                failure: "BadValue: OIDC token must contain exactly one audience",
                step1: OIDCpayload('Advertize_OIDCAuth_user1'),
                step2: OIDCpayload('Authenticate_OIDCAuth_user1_empty_audiences')
            },

            // Dynamic case with a token which isn't valid yet,
            // but who's validity period is within skew tolerance. (60 seconds)
            function(conn, config) {
                jsTest.log('Testing token validity in near (skewable) future.');
                const notBefore = NumberInt(Date.now() / 1000) + 50;
                const token = {
                    iss: KeyServer.getURL() + '/issuer1',
                    sub: 'user1@mongodb.com',
                    nbf: notBefore,
                    exp: 2147483647,
                    aud: ['jwt@kernel.mongodb.com'],
                    nonce: 'gdfhjj324ehj23k4',
                    "mongodb-roles": ['myReadRole'],
                };
                testAuth(conn, config, {
                    step1: OIDCpayload('Advertize_OIDCAuth_user1'),
                    step2: OIDCgenerateBSON({jwt: OIDCsignJWT({alg: "RS256"}, token)}),
                    user: 'issuer1/user1@mongodb.com',
                    roles: ['issuer1/myReadRole', 'readAnyDatabase'],
                    claims: {
                        sub: 'user1@mongodb.com',
                        aud: ['jwt@kernel.mongodb.com'],
                        "mongodb-roles": ['myReadRole']
                    }
                });
            },

            // Dynamic case with a token which expires while we're using it.
            function(conn, config) {
                jsTest.log('Testing token expiration while being used.');
                const notBefore = NumberInt(Date.now() / 1000);
                const expires = notBefore + 30;
                const token = {
                    iss: KeyServer.getURL() + '/issuer1',
                    sub: 'user1@mongodb.com',
                    nbf: notBefore,
                    exp: expires,
                    aud: ['jwt@kernel.mongodb.com'],
                    nonce: 'gdfhjj324ehj23k4',
                    "mongodb-roles": ['myReadRole'],
                };
                const myConn = testAuth(conn, config, {
                    step1: OIDCpayload('Advertize_OIDCAuth_user1'),
                    step2: OIDCgenerateBSON({jwt: OIDCsignJWT({alg: "RS256"}, token)}),
                    user: 'issuer1/user1@mongodb.com',
                    roles: ['issuer1/myReadRole', 'readAnyDatabase'],
                    claims: {
                        sub: 'user1@mongodb.com',
                        aud: ['jwt@kernel.mongodb.com'],
                        "mongodb-roles": ['myReadRole']
                    }
                });

                // Assure listDatabases works while not expired.
                assert.commandWorked(myConn.adminCommand({listDatabases: 1}));

                // Wait until the token has expired and check that it now fails.
                const waitUntil = new Date((expires + 1) * 1000);
                jsTest.log('Sleeping until token has expired at : ' + waitUntil.toISOString());
                sleep(waitUntil - Date.now());
                assert.commandFailedWithCode(myConn.adminCommand({listDatabases: 1}),
                                             ErrorCodes.ReauthenticationRequired);
            },
        ]
    },
    {
        issuer: "https://test.kernel.mongodb.com/oidc/issuer2",
        expectKeysFrom: LIB + '/custom-key-2.json',
        setup: function(conn) {
            assert.commandWorked(conn.adminCommand(
                {createRole: 'issuer2/myReadRole', roles: ['read'], privileges: []}));
            assert.commandWorked(conn.adminCommand(
                {createRole: 'issuer2/myReadWriteRole', roles: ['readWrite'], privileges: []}));
        },
        testCases: [
            // Basic test, make sure second IdentityProvider is select based on hint.
            {
                step1: OIDCpayload('Advertize_OIDCAuth_user1@10gen'),
                step2: OIDCpayload('Authenticate_OIDCAuth_user1@10gen_custom_key_2'),
                user: 'issuer2/user1@10gen.com',
                roles: ['issuer2/myReadRole', 'read'],
                claims: {iss: KeyServer.getURL() + '/issuer2', sub: 'user1@10gen.com'}
            },

            // Try to auth with a token from a different IdentityProvider.
            {
                failure: `BadValue: Token issuer '${
                    KeyServer
                        .getURL()}/issuer1' does not match that inferred from principal name hint '${
                    KeyServer.getURL()}/issuer2'`,
                step1: OIDCpayload('Advertize_OIDCAuth_user1@10gen'),
                step2: OIDCpayload('Authenticate_OIDCAuth_user1')
            },
        ],
    },
];

testOIDCConfig(kOIDCConfig, kOIDCTestCases);

KeyServer.stop();
