// Tests server OIDC token compatibilty with Google Cloud Platform ID tokens, using a remote
// VM on Google Compute Engine to obtain a machine identity token non-interactively
// @tags: [ requires_fcv_70 ]
import {arrayEq} from 'jstests/aggregation/extras/utils.js';
import {getPython3Binary} from "jstests/libs/python.js";
import {ShardingTest} from "jstests/libs/shardingtest.js";
import {determineSSLProvider} from 'jstests/ssl/libs/ssl_helpers.js';
import {
    OIDCGenerateBSONtoFile
} from 'src/mongo/db/modules/enterprise/jstests/external_auth/lib/oidc_utils.js';

(function() {
'use strict';

if (determineSSLProvider() !== 'openssl') {
    print('Skipping test, OIDC is only available with OpenSSL');
    return;
}

const tokenFileLocation = '/home/ubuntu/gcp_token.json';
const configFileLocation = '/home/ubuntu/gce_vm_config.json';
const remoteInfoFileLocation = '/home/ubuntu/gce_vm_info.json';
const sshKeyFileLocation = '/home/ubuntu/gcp_ssh_key';
const bsonTokenLocation = '/home/ubuntu/gcp_token_bson';

// Helper function to retrieve the configured audience from the config file.
function readConfigFile() {
    try {
        return JSON.parse(cat(configFileLocation));
    } catch (e) {
        jsTestLog("Failed to read/parse gcp_vm_config.json.");
        throw e;
    }
}

const kGCPConfig = [{
    issuer: 'https://accounts.google.com',
    audience: readConfigFile()["audience"],
    authNamePrefix: 'issuerGCP',
    supportsHumanFlows: false,
    useAuthorizationClaim: false,
    logClaims: ['sub', 'aud', 'groups'],
    JWKSPollSecs: 86400,
}];

const kStartupOptions = {
    authenticationMechanisms: 'SCRAM-SHA-256,MONGODB-OIDC',
    logComponentVerbosity: tojson({accessControl: 3}),
    oidcIdentityProviders: tojson(kGCPConfig),
};

function runAdminCommand(conn, db, command) {
    const adminDB = conn.getDB('admin');
    assert(adminDB.auth('admin', 'pwd'));
    assert.commandWorked(conn.getDB(db).runCommand(command));
    adminDB.logout();
}

function runTest(conn) {
    // Create the user that the GCP principal will derive privileges from. This is necessary since
    // GCP ID tokens cannot be configured to contain claims representing authorization rights.
    assert.commandWorked(conn.adminCommand({createUser: 'admin', pwd: 'pwd', roles: ['root']}));
    runAdminCommand(conn, conn.getDB('$external'), {
        createUser: 'issuerGCP/104022079631559363852',
        roles: [{role: 'readWriteAnyDatabase', db: 'admin'}]
    });

    // Launch the python script to retrieve the GCP Compute Engine VM's ID token.
    const exitCode = runNonMongoProgram(
        getPython3Binary(),
        'src/mongo/db/modules/enterprise/jstests/external_auth_oidc_gcp/lib/get_id_token.py',
        '--config_file=' + configFileLocation,
        '--output_file=' + tokenFileLocation,
        '--ssh_key_file=' + sshKeyFileLocation,
        '--vm_info_file=' + remoteInfoFileLocation);
    assert.eq(exitCode, 0);

    // Read the token from the file that the script wrote it to.
    const token = JSON.parse(cat(tokenFileLocation));
    removeFile(tokenFileLocation);

    assert(token, "Empty or invalid token response");
    assert(!token.error, "Error returned while obtaining token: " + JSON.stringify(token));

    const bsonTokenResult = OIDCGenerateBSONtoFile({"jwt": token.access_token}, bsonTokenLocation);

    // Expect result code 0,fail if anything else
    assert(!bsonTokenResult,
           "Converting token to BSON format failed with error code " + bsonTokenResult);

    const bsonPayload = cat(bsonTokenLocation);
    removeFile(bsonTokenLocation);

    const authnCmd = {saslStart: 1, mechanism: 'MONGODB-OIDC'};
    assert.commandWorked(
        conn.getDB("$external").runCommand(Object.extend(authnCmd, {payload: bsonPayload})));

    const connStatus = assert.commandWorked(conn.adminCommand({connectionStatus: 1}));
    jsTest.log("Connection Status:" + JSON.stringify(connStatus));
    assert.eq(connStatus.authInfo.authenticatedUsers.length, 1);
    assert.eq(connStatus.authInfo.authenticatedUsers[0].db, '$external');

    let authenticatedRoles = connStatus.authInfo.authenticatedUserRoles;
    authenticatedRoles = authenticatedRoles.map(({role, db}) => role + "." + db);
    authenticatedRoles.sort();

    let expectedRoles = [{role: 'readWriteAnyDatabase', db: 'admin'}];
    expectedRoles = expectedRoles.map(({role, db}) => role + "." + db);
    expectedRoles.sort();

    jsTest.log("Expected Roles: " + expectedRoles);
    jsTest.log("Authenticated Roles:" + authenticatedRoles);
    assert(arrayEq(expectedRoles, authenticatedRoles), "Expected roles != Authenticated Roles");
}

{
    const shardedCluster = new ShardingTest({
        mongos: 1,
        config: 1,
        shards: 1,
        other: {mongosOptions: {setParameter: kStartupOptions}},
        keyFile: 'jstests/libs/key1',
    });
    runTest(shardedCluster.s0);
    shardedCluster.stop();
}
})();
