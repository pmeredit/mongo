/*
 * Tests a non-PIT replica set restore with OIDC external auth mechanism.
 *
 * @tags: [
 *     requires_persistence,
 *     requires_wiredtiger
 * ]
 */

import {MagicRestoreUtils} from "jstests/libs/backup_utils.js";
import {ReplSetTest} from "jstests/libs/replsettest.js";
import {determineSSLProvider} from "jstests/ssl/libs/ssl_helpers.js";
import {
    OIDCgenerateBSON,
    OIDCKeyServer,
    OIDCsignJWT
} from "src/mongo/db/modules/enterprise/jstests/external_auth/lib/oidc_utils.js";
import {OIDCVars} from "src/mongo/db/modules/enterprise/jstests/external_auth/lib/oidc_vars.js";

// TODO SERVER-86034: Run on Windows machines once named pipe related failures are resolved.
if (_isWindows()) {
    jsTestLog("Temporarily skipping test for Windows variants. See SERVER-86034.");
    quit();
}

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

function checkKeysLoaded(conn, testCases) {
    let expectKeys = [];
    testCases.forEach(function(t) {
        expectKeys = expectKeys.concat(JSON.parse(cat(t.expectKeysFrom)).keys.map((k) => k.kid));
    });
    expectKeys.sort();
    const kLoadedKey = 7070202;
    const loadedKeys = checkLog.getGlobalLog(conn)
                           .map((l) => JSON.parse(l))
                           .filter((l) => l.id === kLoadedKey)
                           .map((l) => l.attr.kid)
                           .sort();
    assert.eq(expectKeys, loadedKeys, "Did not load expected keys");
}

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

            // nbf field omitted (still valid).
            Object.assign({},
                          kBasicUser1SuccessCase,
                          {step2: OIDCpayload('Authenticate_OIDCAuth_user1_no_nbf_field')}),

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
        ],
    },
];

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

    const kAuthSuccess = 5286306;
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

function testOIDC(insertHigherTermOplogEntry) {
    if (determineSSLProvider() !== 'openssl') {
        print('Skipping test, OIDC is only available with OpenSSL');
        quit();
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

    const config = {
        setParameter: {
            authenticationMechanisms: 'SCRAM-SHA-256,MONGODB-OIDC',
            logComponentVerbosity: tojson({accessControl: 3}),
            oidcIdentityProviders: tojson(kOIDCConfig)
        }
    };

    // Do initial auth tests.
    const rst = new ReplSetTest({nodes: {n0: config}});
    rst.startSet();
    rst.initiate();

    let primary = rst.getPrimary();
    let admin = primary.getDB("admin");

    assert.commandWorked(admin.runCommand({createUser: 'admin', 'pwd': 'admin', roles: ['root']}));
    assert(admin.auth('admin', 'admin'));
    checkKeysLoaded(primary, kOIDCTestCases);
    kOIDCTestCases.forEach(function(test) {
        const config = kOIDCConfig.filter((c) => c.issuer === test.issuer)[0];
        test.setup(primary, config);
        test.testCases.forEach(function(testCase) {
            if ((typeof testCase) === 'function') {
                testCase(primary, config);
            } else {
                testAuth(primary, config, testCase);
            }
        });
    });
    admin.logout();

    // Perform the magic restore.

    const dbName = "db";
    const coll = "coll";

    const db = primary.getDB(dbName);
    // Insert some data to restore. This data will be reflected in the restored node.
    ['a', 'b', 'c'].forEach(
        key => { assert.commandWorked(db.getCollection(coll).insert({[key]: 1})); });
    const expectedDocs = db.getCollection(coll).find().toArray();

    const magicRestoreUtils = new MagicRestoreUtils({
        backupSource: primary,
        pipeDir: MongoRunner.dataDir,
        isPit: false,
        insertHigherTermOplogEntry: insertHigherTermOplogEntry
    });
    magicRestoreUtils.takeCheckpointAndOpenBackup();

    // These documents will be truncated by magic restore, since they were written after the backup
    // cursor was opened.
    ['e', 'f', 'g'].forEach(
        key => { assert.commandWorked(db.getCollection(coll).insert({[key]: 1})); });
    assert.eq(db.getCollection(coll).find().toArray().length, 6);

    magicRestoreUtils.assertOplogCountForNamespace(primary, dbName + "." + coll, 6, "i");
    const {entriesAfterBackup} = magicRestoreUtils.getEntriesAfterBackup(primary);
    assert.eq(entriesAfterBackup.length, 3);

    magicRestoreUtils.copyFilesAndCloseBackup();

    // Update the roles collection after taking backup. This should get truncated by magic restore.
    assert.commandWorked(
        admin.runCommand({updateRole: "issuer1/myReadWriteRole", privileges: [], roles: []}));

    const expectedConfig = assert.commandWorked(primary.adminCommand({replSetGetConfig: 1})).config;
    rst.stopSet(/*signal=*/ null, /*forRestart=*/ true);

    let restoreConfiguration = {
        "nodeType": "replicaSet",
        "replicaSetConfig": expectedConfig,
        "maxCheckpointTs": magicRestoreUtils.getCheckpointTimestamp(),
    };
    restoreConfiguration =
        magicRestoreUtils.appendRestoreToHigherTermThanIfNeeded(restoreConfiguration);

    magicRestoreUtils.writeObjsAndRunMagicRestore(
        restoreConfiguration, [], {"replSet": jsTestName()});

    // Restart the original replica set. We need to skip stepup on restart because that will call
    // serverStatus which requires auth.
    rst.startSet({dbpath: magicRestoreUtils.getBackupDbPath()},
                 true /* restart */,
                 true /* skipStepUpOnRestart */);

    primary = rst.getPrimary();
    admin = primary.getDB("admin");
    admin.auth("siteRootAdmin", "secret");

    const restoredDocs = primary.getDB(dbName).getCollection(coll).find().toArray();
    // The later 3 writes were truncated during magic restore.
    assert.eq(restoredDocs.length, 3);
    assert.eq(restoredDocs, expectedDocs);

    magicRestoreUtils.postRestoreChecks({
        node: primary,
        expectedConfig: expectedConfig,
        dbName: dbName,
        collName: coll,
        expectedOplogCountForNs: 3,
        opFilter: "i",
        expectedNumDocsSnapshot: 3,
    });

    // Run the auth tests again.
    assert(admin.auth('admin', 'admin'));
    checkKeysLoaded(primary, kOIDCTestCases);
    kOIDCTestCases.forEach(function(test) {
        const config = kOIDCConfig.filter((c) => c.issuer === test.issuer)[0];
        test.testCases.forEach(function(testCase) {
            if ((typeof testCase) === 'function') {
                testCase(primary, config);
            } else {
                testAuth(primary, config, testCase);
            }
        });
    });

    rst.stopSet();
}

for (const insertHigherTermOplogEntry of [false, true]) {
    testOIDC(insertHigherTermOplogEntry);
}

KeyServer.stop();
