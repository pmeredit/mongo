/**
 * Tests that LDAP servers, timeouts, bindDNs, and bindPasswords can be changed at runtime while
 * connections are in-progress without causing crashes.
 */

import {configureFailPoint} from "jstests/libs/fail_point_util.js";
import {funWithArgs} from "jstests/libs/parallel_shell_helpers.js";
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

function runAdminCommand(conn, command) {
    const adminDB = conn.getDB('admin');
    assert(adminDB.auth("siteRootAdmin", "secret"));
    assert.commandWorked(adminDB.runCommand(command));
    adminDB.logout();
}

function launchCluster(useConnectionPooling) {
    TestData.skipCheckingIndexesConsistentAcrossCluster = true;
    TestData.skipCheckOrphans = true;
    TestData.skipCheckDBHashes = true;
    TestData.skipCheckShardFilteringMetadata = true;

    const configGenerator = new LDAPTestConfigGenerator();
    configGenerator.authorizationManagerCacheSize = 0;
    configGenerator.ldapValidateLDAPServerConfig = false;
    configGenerator.ldapUseConnectionPool = useConnectionPooling;
    configGenerator.ldapUserToDNMapping =
        [{match: "(.+)", substitution: "cn={0}," + defaultUserDNSuffix}];
    configGenerator.ldapAuthzQueryTemplate = "{USER}?memberOf";
    const shardingConfig = configGenerator.generateShardingConfig();
    shardingConfig.other.writeConcernMajorityJournalDefault = false;

    // Launch sharded cluster with the above LDAP config.
    const st = new ShardingTest(shardingConfig);
    const mongos = st.s0;
    setupTest(mongos);

    return st;
}

function runTest(fpConn, mainConn, badParam, goodParam) {
    // Configure fail point to force the bind operation itself to hang as long as the fail point
    // is active on the fpConn.
    const bindTimeoutFp = configureFailPoint(fpConn, 'ldapBindTimeoutHangIndefinitely');

    // Run a parallel shell to auth as ldapz_ldap1 and wait for the failpoint to get hit.
    const waitForSuccessfulAuth = startParallelShell(
        funWithArgs(authAsLDAPUser, true /* shouldSucceed */, 'ldapz_ldap1'), mainConn.port);
    bindTimeoutFp.wait();

    // Change the configured parameter to a value that will cause auth failures.
    runAdminCommand(fpConn, badParam);

    // Subsequent auth attempts will use the new parameter, resulting in failure.
    const waitForFailedAuth = startParallelShell(
        funWithArgs(authAsLDAPUser, false /* shouldSucceed */, 'ldapz_admin'), mainConn.port);
    bindTimeoutFp.wait();
    sleep(3000);

    // Turn off the failpoint so that auth succeeds before the timeout.
    bindTimeoutFp.off();
    waitForSuccessfulAuth();
    waitForFailedAuth();

    // Switch the parameter's value back to one that permits successful auth.
    runAdminCommand(fpConn, goodParam);
    startParallelShell(funWithArgs(authAsLDAPUser, true /* shouldSucceed */, 'ldapz_admin'),
                       mainConn.port)();
}

function runChangeLDAPServerTest(useConnectionPooling) {
    const st = launchCluster(useConnectionPooling);
    const mongos = st.s0;
    const isReplicaSetEndpointActive = st.isReplicaSetEndpointActive();

    runTest(mongos,
            mongos,
            {setParameter: 1, ldapServers: "localhost:20441"},
            {setParameter: 1, ldapServers: "ldaptest.10gen.cc"});

    // TODO (SERVER-83433): Add back the test coverage for running db hash check and validation
    // on replica set that is fsync locked and has replica set endpoint enabled.
    st.stop({
        skipCheckDBHashes: isReplicaSetEndpointActive,
        skipValidation: isReplicaSetEndpointActive
    });
}

function runChangeLDAPQueryBindTest(useConnectionPooling) {
    const st = launchCluster(useConnectionPooling);
    const mongos = st.s0;
    const configPrimary = st.configRS.getPrimary();
    const isReplicaSetEndpointActive = st.isReplicaSetEndpointActive();

    runTest(configPrimary,
            mongos,
            {setParameter: 1, ldapQueryUser: 'cn=ldapz_ldap2,' + defaultUserDNSuffix},
            {setParameter: 1, ldapQueryPassword: 'Secret123'});

    // TODO (SERVER-83433): Add back the test coverage for running db hash check and validation
    // on replica set that is fsync locked and has replica set endpoint enabled.
    st.stop({
        skipCheckDBHashes: isReplicaSetEndpointActive,
        skipValidation: isReplicaSetEndpointActive
    });
}

function runChangeLDAPTimeoutTest(useConnectionPooling) {
    const st = launchCluster(useConnectionPooling);
    const mongos = st.s0;
    const isReplicaSetEndpointActive = st.isReplicaSetEndpointActive();

    runTest(mongos,
            mongos,
            {setParameter: 1, ldapTimeoutMS: 1},
            {setParameter: 1, ldapTimeoutMS: 10000});

    // TODO (SERVER-83433): Add back the test coverage for running db hash check and validation
    // on replica set that is fsync locked and has replica set endpoint enabled.
    st.stop({
        skipCheckDBHashes: isReplicaSetEndpointActive,
        skipValidation: isReplicaSetEndpointActive
    });
}

runChangeLDAPServerTest(true);
runChangeLDAPServerTest(false);
runChangeLDAPQueryBindTest(true);
runChangeLDAPQueryBindTest(false);

// Since timeout enforcement is deferred to the system library when connection pooling is off,
// it's only tested with the connection pool enabled.
runChangeLDAPTimeoutTest(true);
