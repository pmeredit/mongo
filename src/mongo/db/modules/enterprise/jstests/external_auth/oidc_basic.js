// Basic OIDC SASL interaction stub test.

(function() {
'use strict';

if (!TestData.setParameters.featureFlagOIDCSpike) {
    jsTest.log('Skipping test because featureFlagOIDCSpike is not enabled');
    return;
}

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
    const kAuthFailed = 20249;
    const logs =
        checkLog.getGlobalLog(conn).map((l) => JSON.parse(l)).filter((l) => l.id == kAuthFailed);
    admin.logout();

    assert.gt(logs.length, 0, 'No auth failures found');
    const lastFailure = logs[logs.length - 1];
    assert.eq(lastFailure.attr.error, msg);
}

function runAuthSteps(conn, clientStep1, clientStep2) {
    const admin = conn.getDB('admin');
    const external = conn.getDB('$external');

    jsTest.log('Running auth steps: ' + clientStep1 + ' ' + clientStep2);

    const clientStep1Payload = new BinData(0, clientStep1);
    const step1Reply = assert.commandWorked(external.runCommand(
        {saslStart: 1, mechanism: 'MONGODB-OIDC', payload: clientStep1Payload}));
    jsTest.log("Step1: " + tojson(step1Reply));

    if (clientStep2 === null) {
        assert.eq(step1Reply.done, true);
        assert.eq(step1Reply.payload.base64(), '');
    } else {
        // BSON("idp" << "https://localhost:1234" <<
        //      "clientId" << "deadbeefcafe" <<
        //      "clientSecret" << "hunter2")
        const serverStep1PayloadExpect = 'WgAAAAJpZHAAFwAAAGh0dHBzOi8vbG9jYWxob3N0' +
            'OjEyMzQAAmNsaWVudElkAA0AAABkZWFkYmVlZmNh' +
            'ZmUAAmNsaWVudFNlY3JldAAIAAAAaHVudGVyMgAA';
        assert.eq(step1Reply.payload.base64(), serverStep1PayloadExpect);

        const clientStep2Payload = new BinData(0, clientStep2);
        const step2Reply = assert.commandWorked(external.runCommand(
            {saslContinue: 1, conversationId: NumberInt(1), payload: clientStep2Payload}));
        jsTest.log(step2Reply);

        assert.eq(step2Reply.done, true);
        assert.eq(step2Reply.payload.base64(), '');
    }

    const authInfo = assert.commandWorked(admin.runCommand({connectionStatus: 1})).authInfo;
    jsTest.log("AuthInfo: " + tojson(authInfo));
    assert.eq(authInfo.authenticatedUsers.length, 1);
    assert.eq(authInfo.authenticatedUsers[0].user, 'user1');
    assert.eq(authInfo.authenticatedUserRoles.length, 1);
    assert.eq(authInfo.authenticatedUserRoles[0].role, 'readWriteAnyDatabase');

    external.logout();
}

function checkKeysLoaded(conn, keyNames) {
    const kKeyLoadedId = 6766000;
    const keys = checkLog.getGlobalLog(conn)
                     .map((l) => JSON.parse(l))
                     .filter((l) => l.id === kKeyLoadedId)
                     .map((l) => l.attr.kid);
    jsTest.log('Loaded keys: ' + tojson(keys));
    keyNames.forEach(function(key) {
        assert(keys.includes(key), "Loaded keys does not include: " + key);
    });
}

function runTest(conn) {
    const admin = conn.getDB('admin');
    const external = conn.getDB('$external');
    assert.commandWorked(admin.runCommand({createUser: 'admin', pwd: 'pwd', roles: ['root']}));
    assert(admin.auth('admin', 'pwd'));

    checkKeysLoaded(
        conn,
        ['rfc-7517-appendix-b', 'NIST-CAVP-DS-RSA2VS-0UhWwyvtfIdxPvR9zCWYJB5_AM0LE2qc6RGOcI0cQjw']);

    external.createUser({
        user: 'user1',
        mechanisms: ['MONGODB-OIDC'],
        roles: [{role: 'readWriteAnyDatabase', db: 'admin'}]
    });
    external.createUser({user: 'user2', mechanisms: ['MONGODB-OIDC'], roles: []});
    admin.logout();

    const kEmptyObject = 'BQAAAAA=';                      // BSONObj()
    const kAdvertUser1 = 'EgAAAAJuAAYAAAB1c2VyMQAA';      // BSON('n' << 'user1')
    const kAdvertUser2 = 'EgAAAAJuAAYAAAB1c2VyMgAA';      // BSON('n' << 'user2')
    const kAuthAsUser1 = 'FAAAAAJqd3QABgAAAHVzZXIxAAA=';  // BSON('jwt' << 'user1')

    // Basic flow, no advertized principal, just auth in step 2.
    runAuthSteps(conn, kEmptyObject, kAuthAsUser1);

    // Advertize principal, then auth in second step.
    runAuthSteps(conn, kAdvertUser1, kAuthAsUser1);

    // Quick flow, auth in first step.
    runAuthSteps(conn, kAuthAsUser1, null);

    // Negative: Advertize different user than authenticate as.
    const kChangedUserError =
        "BadValue: Principal name changed between step1 'user2' and step2 'user1'";
    assertThrowsMessage(
        conn, () => runAuthSteps(conn, kAdvertUser2, kAuthAsUser1), kChangedUserError);
}

{
    const kEntPath = 'src/mongo/db/modules/enterprise';
    const opts = {
        auth: '',
        setParameter: {
            authenticationMechanisms: 'SCRAM-SHA-256,MONGODB-OIDC',
            logComponentVerbosity: '{"accessControl":5}',
            oidcAuthURL: 'https://localhost:1234',
            oidcClientId: 'deadbeefcafe',
            oidcClientSecret: 'hunter2',
            oidcKeySetFile: kEntPath + '/jstests/external_auth/lib/oidc_keys.json',
        },
    };
    const standalone = MongoRunner.runMongod(opts);
    runTest(standalone);
    MongoRunner.stopMongod(standalone);
}
})();
