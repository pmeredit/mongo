// Tests server OIDC token compatibilty with Azure Managed Identity tokens, using a remote
// host on Azure Container App service to obtain a machine identity token non-interactively
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

const kAzureConfig = [{
    issuer: 'https://sts.windows.net/c96563a8-841b-4ef9-af16-33548de0c958/',
    audience: '3e39bf20-432b-440b-a104-9f070e3b87b9',
    authNamePrefix: 'issuerAzure',
    matchPattern: '@outlook.com$',
    clientId: '3e39bf20-432b-440b-a104-9f070e3b87b9',
    requestScopes: ['api://3e39bf20-432b-440b-a104-9f070e3b87b9'],
    authorizationClaim: 'groups',
    logClaims: ['sub', 'aud', 'groups'],
    JWKSPollSecs: 86400,
}];

const kAzureStartupOptions = {
    authenticationMechanisms: 'SCRAM-SHA-256,MONGODB-OIDC',
    logComponentVerbosity: tojson({accessControl: 3}),
    oidcIdentityProviders: tojson(kAzureConfig),
};

function runTest(conn) {
    // Create the roles that the Azure groups will map to.
    assert.commandWorked(conn.adminCommand({createUser: 'admin', pwd: 'pwd', roles: ['root']}));

    const tokenFileLocation = '/home/ubuntu/azure_token.json';
    const configFileLocation = '/home/ubuntu/azure_e2e_config.json';
    const hostnameFileLocation = '/home/ubuntu/azure_remote_host';
    const keyfileLocation = '/home/ubuntu/azure_remote_key';
    const bsonTokenLocation = '/home/ubuntu/bson_azure_token';

    const adminDB = conn.getDB('admin');
    assert(adminDB.auth('admin', 'pwd'));

    const test02Db = conn.getDB('test02');

    assert(test02Db.coll.insert({val: 'test02-read-write'}));

    const testRoles = [{role: 'readWrite', db: 'test01'}, {role: 'read', db: 'test02'}];

    assert.commandWorked(conn.adminCommand({
        createRole: 'issuerAzure/fcee9e02-51be-4765-83b0-f0ccc6d60f85',
        roles: testRoles,
        privileges: []
    }));

    adminDB.logout();
    let authnCmd = {saslStart: 1, mechanism: 'MONGODB-OIDC'};

    let code = runNonMongoProgram(
        getPython3Binary(),
        'src/mongo/db/modules/enterprise/jstests/external_auth_oidc_azure/lib/get_token.py',
        '--config_file=' + configFileLocation,
        '--hostname_file=' + hostnameFileLocation,
        '--output_file=' + tokenFileLocation,
        '--key_file=' + keyfileLocation);

    const token = JSON.parse(cat(tokenFileLocation));
    removeFile(tokenFileLocation);

    assert(token, "Empty or invalid token response");
    assert(!token.error, "Error returned while obtaining token: " + JSON.stringify(token));

    const bsonTokenResult = OIDCGenerateBSONtoFile({"jwt": token.access_token}, bsonTokenLocation);

    // Expect result code 0,fail if anything else
    assert(!bsonTokenResult,
           "Converting token to BSON format failed with error code " + bsonTokenResult);

    let bsonPayload = cat(bsonTokenLocation);
    removeFile(bsonTokenLocation);

    const result = assert.commandWorked(
        conn.getDB("$external").runCommand(Object.extend(authnCmd, {payload: bsonPayload})));

    const connStatus = assert.commandWorked(conn.adminCommand({connectionStatus: 1}));

    jsTest.log("Connection Status:" + JSON.stringify(connStatus));
    assert.eq(connStatus.authInfo.authenticatedUsers.length, 1);
    assert.eq(connStatus.authInfo.authenticatedUsers[0].db, '$external');

    let authenticatedRoles = connStatus.authInfo.authenticatedUserRoles;
    authenticatedRoles = authenticatedRoles.map(({role, db}) => role + "." + db);
    authenticatedRoles.sort();

    let configuredRoles = testRoles;
    configuredRoles.push({role: 'issuerAzure/fcee9e02-51be-4765-83b0-f0ccc6d60f85', db: 'admin'});
    configuredRoles = configuredRoles.map(({role, db}) => role + "." + db);
    configuredRoles.sort();

    jsTest.log("Configured Roles: " + configuredRoles);
    jsTest.log("Authenticated Roles:" + authenticatedRoles);
    assert(arrayEq(configuredRoles, authenticatedRoles), "Configured roles != Authenticated Roles");
}
{
    const shardedCluster = new ShardingTest({
        mongos: 1,
        config: 1,
        shards: 1,
        other: {mongosOptions: {setParameter: kAzureStartupOptions}},
        keyFile: 'jstests/libs/key1',
    });
    runTest(shardedCluster.s0);
    shardedCluster.stop();
}
})();
