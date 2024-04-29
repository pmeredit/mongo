// Tests MONGODB-OIDC mechanism integration between server and shell. Specifically covers auth via
// MongoURIs, the db.auth() shell wrapper method, and from the shell's command line.
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
};
const KeyServer = new OIDCKeyServer(JSON.stringify(keyMap));
KeyServer.start();

const kOIDCTokens = OIDCVars(KeyServer.getURL()).kOIDCTokens;

const kOIDCConfig = [{
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
const startupOptions = {
    authenticationMechanisms: 'SCRAM-SHA-256,MONGODB-OIDC',
    logComponentVerbosity: tojson({accessControl: 3}),
    oidcIdentityProviders: tojson(kOIDCConfig),
    JWKSMinimumQuiescePeriodSecs: 0,
};
const expectedRoles = ['issuer1/myReadRole', 'readAnyDatabase'];
const verifyAuthFn =
    `const connStatus = assert.commandWorked(db.adminCommand({connectionStatus: 1}));
        assert.eq(connStatus.authInfo.authenticatedUsers.length, 1);
        assert.eq(connStatus.authInfo.authenticatedUsers[0].db, '$external');
        assert.eq(connStatus.authInfo.authenticatedUsers[0].user, 'issuer1/user1@mongodb.com');
        const expectedRoles = ['issuer1/myReadRole', 'readAnyDatabase'];
        const sortedActualRoles =
            connStatus.authInfo.authenticatedUserRoles.map((r) => r.db + '.' + r.role).sort();
        const sortedExpectRoles = expectedRoles.map((r) => 'admin.' + r).sort();
        assert.eq(sortedActualRoles, sortedExpectRoles);`;

function assertThrowsMessage(conn, func, msg) {
    const admin = conn.getDB('admin');
    const external = conn.getDB('$external');

    try {
        func();
        assert(false, "Expected to fail with message: " + msg);
    } catch (e) {
        // Returned message will only be authentication failed.
        // Need to look in the logs instead.
    }

    external.logout();
    assert(admin.auth('admin', 'pwd'));
    const kAuthFailed = 5286307;
    jsTest.log('Checking log for msg', msg);
    const logs =
        checkLog.getGlobalLog(conn).map((l) => JSON.parse(l)).filter((l) => l.id == kAuthFailed);
    admin.logout();

    assert.gt(logs.length, 0, 'No auth failures found');
    const lastFailure = logs[logs.length - 1];
    assert.eq(lastFailure.attr.error, msg);
}

function runAuthWithObj(conn, authObject) {
    const external = conn.getDB('$external');

    jsTest.log('Running auth directly with object: ' + tojson(authObject));
    assert(external.auth(authObject));

    const connStatus = assert.commandWorked(conn.adminCommand({connectionStatus: 1}));
    assert.eq(connStatus.authInfo.authenticatedUsers.length, 1);
    assert.eq(connStatus.authInfo.authenticatedUsers[0].db, '$external');
    assert.eq(connStatus.authInfo.authenticatedUsers[0].user, 'issuer1/user1@mongodb.com');
    const sortedActualRoles =
        connStatus.authInfo.authenticatedUserRoles.map((r) => r.db + '.' + r.role).sort();
    const sortedExpectRoles = expectedRoles.map((r) => 'admin.' + r).sort();
    assert.eq(sortedActualRoles, sortedExpectRoles);

    external.logout();
}

function runAuthWithUri(conn, authObject) {
    let uriPrefix = 'mongodb://localhost:';
    if (authObject.user) {
        uriPrefix = 'mongodb://' + encodeURIComponent(authObject.user) + '@localhost:';
    }
    const uri = uriPrefix + conn.port + '/test?authSource=$external&' +
        'authMechanism=MONGODB-OIDC&authMechanismProperties=OIDC_ACCESS_TOKEN:' +
        authObject.oidcAccessToken;
    jsTest.log('Running auth via Mongo URI: ', uri);
    let smoke = runMongoProgram('mongo', uri, '--eval', verifyAuthFn);
    assert.eq(smoke, 0, "Could not auth using OIDC access token in URI");
}

function runAuthFromCLI(conn, authObject) {
    jsTest.log('Running auth via command line argument');
    let smoke = 1;
    if (authObject.user) {
        smoke = runMongoProgram("mongo",
                                "--host",
                                "localhost",
                                "--port",
                                conn.port,
                                '--authenticationMechanism',
                                'MONGODB-OIDC',
                                '--authenticationDatabase',
                                '$external',
                                '-u',
                                authObject.user,
                                '--oidcAccessToken',
                                authObject.oidcAccessToken,
                                "--eval",
                                verifyAuthFn);
    } else {
        smoke = runMongoProgram("mongo",
                                "--host",
                                "localhost",
                                "--port",
                                conn.port,
                                '--authenticationMechanism',
                                'MONGODB-OIDC',
                                '--authenticationDatabase',
                                '$external',
                                '--oidcAccessToken',
                                authObject.oidcAccessToken,
                                "--eval",
                                verifyAuthFn);
    }
    assert.eq(smoke, 0, "Could not auth using OIDC Access token in command line");
}

function runPositiveTest(conn, authObject) {
    // Assert that db.auth({user: <>, mechanism: 'MONGODB-OIDC', oidcAccessToken: <>}) works.
    runAuthWithObj(conn, authObject);

    // Assert that authenticating via MONGODB-OIDC through a MongoURI works.
    runAuthWithUri(conn, authObject);

    // Assert that authenticating via MONGODB-OIDC through a new Mongo shell object works.
    runAuthFromCLI(conn, authObject);
}

// Given an authObject that represents an auth attempt expected to fail, this function asserts that
// auth fails via direct object, MongoURI, and a new Mongo shell.
function runNegativeTest(conn, authObject, errMsg) {
    assertThrowsMessage(conn, () => runAuthWithObj(conn, authObject), errMsg);
    assertThrowsMessage(conn, () => runAuthWithUri(conn, authObject), errMsg);
    assertThrowsMessage(conn, () => runAuthFromCLI(conn, authObject), errMsg);
}

function runTest(conn) {
    // Create admin user that can check logs and create roles.
    assert.commandWorked(conn.adminCommand({createUser: 'admin', pwd: 'pwd', roles: ['root']}));

    // Create the role that the token claim will map to.
    const adminDB = conn.getDB('admin');
    assert(adminDB.auth('admin', 'pwd'));
    assert.commandWorked(conn.adminCommand(
        {createRole: 'issuer1/myReadRole', roles: ['readAnyDatabase'], privileges: []}));
    adminDB.logout();

    // Auth directly from shell, passing in just the token. Positive test case.
    const validAccessToken = kOIDCTokens['Token_OIDCAuth_user1'];
    const validTokenOnlyAuthObj = {oidcAccessToken: validAccessToken, mechanism: 'MONGODB-OIDC'};
    runPositiveTest(conn, validTokenOnlyAuthObj);

    // Auth directly from shell, passing in both the token and the username. Positive test case.
    const validTokenWithHintAuthObj = {
        user: 'user1@mongodb.com',
        oidcAccessToken: validAccessToken,
        mechanism: 'MONGODB-OIDC'
    };
    runPositiveTest(conn, validTokenWithHintAuthObj);

    // Auth directly from shell, passing a username that does not match the token's sub claim.
    // Negative test case.
    const mismatchTokenWithHintAuthObj = {
        user: 'user2@mongodb.com',
        oidcAccessToken: validAccessToken,
        mechanism: 'MONGODB-OIDC'
    };
    runNegativeTest(
        conn,
        mismatchTokenWithHintAuthObj,
        "BadValue: Principal name changed between step1 'user2@mongodb.com' and step2 'user1@mongodb.com'");

    // Auth directly from shell with expired token. Negative test case.
    const expiredAccessToken = kOIDCTokens['Token_OIDCAuth_user1_expired'];
    const expiredAuthObj = {oidcAccessToken: expiredAccessToken, mechanism: 'MONGODB-OIDC'};
    runNegativeTest(conn, expiredAuthObj, 'BadValue: Token is expired');

    // Auth directly from shell with incorrectly signed token. Negative test case.
    const wronglySignedToken = validAccessToken + 'HELLOWORLD';
    const wronglySignedAuthObj = {oidcAccessToken: wronglySignedToken, mechanism: 'MONGODB-OIDC'};
    runNegativeTest(conn, wronglySignedAuthObj, 'InvalidSignature: OpenSSL: Signature is invalid');
}

{
    const standalone = MongoRunner.runMongod({auth: '', setParameter: startupOptions});
    runTest(standalone);
    MongoRunner.stopMongod(standalone);
}

{
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
