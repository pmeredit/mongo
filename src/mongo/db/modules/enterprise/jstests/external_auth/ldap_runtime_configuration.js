/**
 * Tests that LDAP servers, timeouts, bindDNs, and bindPasswords can be changed at runtime while
 * connections are in-progress without causing crashes.
 */

import {configureFailPoint} from "jstests/libs/fail_point_util.js";
import {funWithArgs} from "jstests/libs/parallel_shell_helpers.js";
import {ShardingTest} from "jstests/libs/shardingtest.js";
import {
    defaultUserDNSuffix,
    LDAPTestConfigGenerator,
    setupTest
} from "src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldap_authz_lib.js";

function authAsLDAPUser(shouldSucceed, user) {
    const externalDB = db.getMongo().getDB('$external');
    assert.eq(externalDB.auth({
        user: user,
        pwd: 'Secret123',
        mechanism: 'PLAIN',
        digestPassword: false,
    }),
              shouldSucceed);
}

// Conn should already be authenticated as the admin user.
function runAdminCommand(conn, command) {
    const adminDB = conn.getDB('admin');
    const res = assert.commandWorked(adminDB.runCommand(command));
    return res;
}

function launchCluster(
    useConnectionPooling, configGenerator, delay, disableCacheInvalidation = false) {
    TestData.skipCheckingIndexesConsistentAcrossCluster = true;
    TestData.skipCheckOrphans = true;
    TestData.skipCheckDBHashes = true;
    TestData.skipCheckShardFilteringMetadata = true;

    // These tests should only use the mock server, not ldaptest.10gen.cc.
    configGenerator.ldapServers = [];
    configGenerator.startMockupServer(delay);

    configGenerator.authorizationManagerCacheSize = 0;
    configGenerator.ldapValidateLDAPServerConfig = false;
    configGenerator.ldapUseConnectionPool = useConnectionPooling;
    configGenerator.ldapUserToDNMapping =
        [{match: "(.+)", substitution: "cn={0}," + defaultUserDNSuffix}];
    configGenerator.ldapAuthzQueryTemplate = "{USER}?memberOf";
    const shardingConfig = configGenerator.generateShardingConfig();
    shardingConfig.other.writeConcernMajorityJournalDefault = false;

    // runChangeLDAPQueryBindTest() will fail if the UserCacheInvalidation job runs while the lookup
    // call to the config server from mongos is being made. This is because the call will be
    // invalidated, and a reattempt will use the new invalid LDAP parameters. We set the interval to
    // the maximum value to prevent this job from occurring during the test.
    if (disableCacheInvalidation) {
        shardingConfig.other.mongosOptions.setParameter.userCacheInvalidationIntervalSecs = 86400;
    }

    // Launch sharded cluster with the above LDAP config.
    const st = new ShardingTest(shardingConfig);
    const mongos = st.s0;
    setupTest(mongos);

    return st;
}

/**
 * - Server initially has a valid LDAP configuration.
 * - A client attempts authenticating as an LDAP user, and the server stalls while binding to the
 * LDAP server.
 * - A different client changes LDAP configuration to an invalid value that should result in LDAP
 * auth failures.
 * - The ongoing authentication attempt should continue using the old configuration and eventually
 * succeed.
 * - Subsequent authentication attempts should fail due to the now-invalid LDAP configuration.
 * - Changing the LDAP configuration back to the original value should allow auth to start working
 * normally again.
 */
function runTest(fpConn, mainConn, parameterName, badParameterValue) {
    // Admin operations will be run on the failpoint connection, so authenticate as the admin.
    const adminDB = fpConn.getDB('admin');
    assert(adminDB.auth("siteRootAdmin", "secret"));

    // Configure fail point to force the bind operation itself to hang as long as the fail point
    // is active on the fpConn.
    let bindTimeoutFp = configureFailPoint(fpConn, 'ldapBindTimeoutHangIndefinitely');

    // Retrieve the current value of the parameter that will be changed.
    const oldParamValue = runAdminCommand(fpConn, {getParameter: 1, [parameterName]: 1});

    // Run a parallel shell to auth as ldapz_ldap1 and wait for the failpoint to get hit.
    const waitForSuccessfulAuth = startParallelShell(
        funWithArgs(authAsLDAPUser, true /* shouldSucceed */, 'ldapz_ldap1'), mainConn.port);
    bindTimeoutFp.wait();
    sleep(3000);

    // Change the configured parameter to a value that will cause auth failures.
    jsTestLog('Changing parameter ' + parameterName + ' from ' + oldParamValue[parameterName] +
              ' to ' + badParameterValue);
    runAdminCommand(fpConn, {setParameter: 1, [parameterName]: badParameterValue});

    // Turn the failpoint off. Despite the new parameter value, the existing auth attempt
    // should succeed since it started before the config update.
    bindTimeoutFp.off();
    waitForSuccessfulAuth();

    // Now, launch another auth attempt. Since the parameter value has been updated this should
    // fail.
    startParallelShell(funWithArgs(authAsLDAPUser, false /* shouldSucceed */, 'ldapz_admin'),
                       mainConn.port)();

    // Switch the parameter's value back to one that permits successful auth.
    jsTestLog('Changing parameter ' + parameterName + ' from ' + badParameterValue + ' to ' +
              oldParamValue[parameterName]);
    runAdminCommand(fpConn, {setParameter: 1, [parameterName]: oldParamValue[parameterName]});
    startParallelShell(funWithArgs(authAsLDAPUser, true /* shouldSucceed */, 'ldapz_admin'),
                       mainConn.port)();
}

function runChangeLDAPServerTest(useConnectionPooling) {
    const configGenerator = new LDAPTestConfigGenerator();
    const st = launchCluster(useConnectionPooling, configGenerator, 0);
    const mongos = st.s0;
    const isReplicaSetEndpointActive = st.isReplicaSetEndpointActive();

    runTest(mongos, mongos, 'ldapServers', 'localhost:12345');

    // TODO (SERVER-83433): Add back the test coverage for running db hash check and validation
    // on replica set that is fsync locked and has replica set endpoint enabled.
    st.stop({
        skipCheckDBHashes: isReplicaSetEndpointActive,
        skipValidation: isReplicaSetEndpointActive
    });
    configGenerator.stopMockupServer();
}

function runChangeLDAPQueryBindTest(useConnectionPooling) {
    const configGenerator = new LDAPTestConfigGenerator();
    const st = launchCluster(
        useConnectionPooling, configGenerator, 0, true /** disableCacheInvalidation */);
    const mongos = st.s0;
    const configPrimary = st.configRS.getPrimary();
    const isReplicaSetEndpointActive = st.isReplicaSetEndpointActive();

    runTest(configPrimary, mongos, 'ldapQueryUser', 'cn=ldapz_ldap2,' + defaultUserDNSuffix);

    // TODO (SERVER-83433): Add back the test coverage for running db hash check and validation
    // on replica set that is fsync locked and has replica set endpoint enabled.
    st.stop({
        skipCheckDBHashes: isReplicaSetEndpointActive,
        skipValidation: isReplicaSetEndpointActive
    });
    configGenerator.stopMockupServer();
}

function runChangeLDAPTimeoutTest(useConnectionPooling) {
    const configGenerator = new LDAPTestConfigGenerator();
    const st = launchCluster(useConnectionPooling, configGenerator, 5);
    const mongos = st.s0;
    const isReplicaSetEndpointActive = st.isReplicaSetEndpointActive();

    runTest(mongos, mongos, 'ldapTimeoutMS', 50);

    // TODO (SERVER-83433): Add back the test coverage for running db hash check and validation
    // on replica set that is fsync locked and has replica set endpoint enabled.
    st.stop({
        skipCheckDBHashes: isReplicaSetEndpointActive,
        skipValidation: isReplicaSetEndpointActive
    });
    configGenerator.stopMockupServer();
}

runChangeLDAPServerTest(true);
runChangeLDAPServerTest(false);
runChangeLDAPQueryBindTest(true);
runChangeLDAPQueryBindTest(false);

// Since timeout enforcement is deferred to the system library when connection pooling is off,
// it's only tested with the connection pool enabled.
runChangeLDAPTimeoutTest(true);
