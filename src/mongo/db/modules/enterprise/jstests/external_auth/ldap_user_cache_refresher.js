/**
 * Tests that the LDAPUserCachePoller refreshes entries at the configured interval and
 * invalidates entries when refreshes fail and the configured staleness interval
 * passes.
 */

import {funWithArgs} from "jstests/libs/parallel_shell_helpers.js";
import {
    defaultRole,
    defaultUserDNSuffix,
    setupTest
} from "src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldap_authz_lib.js";
import {
    LDAPWriteClient,
    MockLDAPServer,
    setupConfigGenerator
} from "src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldap_mock_server_utils.js";

// Consts used in tests.
const ldapUserOne = {
    userName: 'ldapz_ldap1',
    pwd: 'Secret123',
};
const ldapUserTwo = {
    userName: 'ldapz_ldap2',
    pwd: 'Secret123',
};
const kLdapUserCacheRefreshInterval = 10;
const kLdapUserCacheStalenessInterval = 30;
const kMongosCacheInvalidationInterval = 45;
const kErrorMS = 3000;
const kMongodRefreshSleepMS = ((kLdapUserCacheRefreshInterval) * 1000) + kErrorMS;
const kMongodStalenessSleepMS =
    ((kLdapUserCacheRefreshInterval + kLdapUserCacheStalenessInterval) * 1000) + kErrorMS;
const kShardedRefreshSleepMS =
    ((kLdapUserCacheRefreshInterval + kMongosCacheInvalidationInterval) * 1000) + kErrorMS;
const kShardedStalenessSleepMS = ((kLdapUserCacheRefreshInterval + kLdapUserCacheStalenessInterval +
                                   kMongosCacheInvalidationInterval) *
                                  1000) +
    kErrorMS;

// Function ran by client shells in parallel while testing refresh.
const clientRefreshCallback = function({userName, pwd}, expectedResult) {
    const externalDB = db.getMongo().getDB('$external');
    assert(externalDB.auth({
        user: userName,
        pwd: pwd,
        mechanism: 'PLAIN',
        digestPassword: false,
    }));

    const testDB = db.getMongo().getDB('test');
    if (expectedResult === 'success') {
        assert.writeOK(testDB.test.insert({a: 1}), "Cannot write despite having privileges");
    } else if (expectedResult === 'failure') {
        assert.writeError(testDB.test.insert({a: 1}),
                          "Refresh did not occur, can still write after LDAP server role update");
    } else {
        assert.commandWorkedOrFailedWithCode(db.runCommand({insert: "test", documents: [{a: 1}]}),
                                             ErrorCodes.Unauthorized);
    }
};

// This helper function auths to the MongoDB server and then shuts down the LDAP server. It verifies
// that:
// 1. Already logged-in connections remain authorized to perform what they were able to do before
//    cache invalidation.
// 2. Other connections cannot login while the LDAP server is down.
// 3. A log message is emitted, explaining that we couldn't re-acquire user rights via LDAP, and we
// will fallback to stale information."
function testStalenessInterval({userName, pwd}, conn, mockLDAPServer, stalenessInterval) {
    const externalDB = conn.getDB('$external');
    assert(externalDB.auth({
        user: userName,
        pwd: pwd,
        mechanism: 'PLAIN',
        digestPassword: false,
    }));

    mockLDAPServer.stop();

    const testDB = conn.getDB('test');
    assert.writeOK(testDB.test.insert({a: 1}), "User cannot write before LDAP server outage");
    sleep(stalenessInterval);
    assert.writeOK(testDB.test.insert({a: 1}), "User cannot write after LDAP server outage");
    externalDB.logout();

    const adminDB = conn.getDB('admin');
    assert(adminDB.auth("siteRootAdmin", "secret"));

    // Verify we are unable to re-acquire user authorization rights via LDAP and we will fallback to
    // stale information.
    checkLog.containsJson(adminDB, 7785501);
    adminDB.logout();

    const parallelConn = new Mongo(conn.host);
    assert(!parallelConn.getDB('$external').auth({
        user: userName,
        pwd: pwd,
        mechanism: 'PLAIN',
        digestPassword: false,
    }));
}

// Runs the core of the test, which involves verifying that cached role information is used within
// the staleness interval and that cached role information gets updated after the refresh interval.
function runCommonTest(conn, mockLDAPServer, refreshInterval, stalenessInterval) {
    // Launch two clients to auth as ldapz_ldap1 and ldapz_ldap2, respectively. They should be able
    // to insert to the test collection on the test database.
    let awaitClient =
        startParallelShell(funWithArgs(clientRefreshCallback, ldapUserOne, 'success'), conn.port);
    awaitClient();
    awaitClient =
        startParallelShell(funWithArgs(clientRefreshCallback, ldapUserTwo, 'success'), conn.port);
    awaitClient();

    // Launch the mock LDAP write client to modify ldapz_ldap1 such that it loses its testWriter
    // role.
    const ldapWriteClient = new LDAPWriteClient(mockLDAPServer.getPort());
    ldapWriteClient.executeWriteOp(defaultRole, "cn=ldapz_ldap1," + defaultUserDNSuffix, "delete");

    // Immediately after the update, ldapz_ldap1's ability to write to the collection will be
    // non-deterministic. Most of the time, it should still be able to write to the collection, but
    // in the small chance that the refresh job happened to fire immediately after the update to the
    // LDAP server, ldapz_ldap1 will fail to write. To account for this, the client simply asserts
    // that it can auth normally and asserts that the write either succeeds or fails with the
    // Unauthorized error code.
    awaitClient =
        startParallelShell(funWithArgs(clientRefreshCallback, ldapUserOne, 'maybe'), conn.port);
    awaitClient();

    // After waiting for the refresh interval, the mongod should now have acquired the updated set
    // of roles for ldapz_ldap1 and ldapz_ldap2. Subsequently, ldapz_ldap1 should now fail to write
    // to the test collection while ldapz_ldap2 still succeeds.
    sleep(refreshInterval);
    awaitClient =
        startParallelShell(funWithArgs(clientRefreshCallback, ldapUserOne, 'failure'), conn.port);
    awaitClient();
    awaitClient =
        startParallelShell(funWithArgs(clientRefreshCallback, ldapUserTwo, 'success'), conn.port);
    awaitClient();

    // Abruptly shut down the mock LDAP server to simulate an outage. Verify that cached roles are
    // not used any longer than the staleness interval.
    testStalenessInterval(ldapUserTwo, conn, mockLDAPServer, stalenessInterval);
}

function runMongodTest() {
    // Start a mock LDAP server.
    const mockLDAPServer = new MockLDAPServer(
        'src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldap_mock_server_dit.ldif');
    mockLDAPServer.start();

    // Configure mongod to talk to the mock LDAP server.
    const ldapConfig = setupConfigGenerator(mockLDAPServer.getHostAndPort());
    const mongodConfig = ldapConfig.generateMongodConfig();
    mongodConfig.setParameter.ldapUserCacheRefreshInterval = kLdapUserCacheRefreshInterval;
    mongodConfig.setParameter.ldapUserCacheStalenessInterval = kLdapUserCacheStalenessInterval;

    // Launch and set up mongod.
    const mongod = MongoRunner.runMongod(mongodConfig);
    setupTest(mongod);

    runCommonTest(mongod, mockLDAPServer, kMongodRefreshSleepMS, kMongodStalenessSleepMS);

    // Shut the mongod down. The mock LDAP server should have already been shut down as part of the
    // staleness test.
    MongoRunner.stopMongod(mongod);
}

function runShardedTest() {
    // Start a mock LDAP server.
    const mockLDAPServer = new MockLDAPServer(
        'src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldap_mock_server_dit.ldif');
    mockLDAPServer.start();

    // Then, configure mongod to talk to the mock LDAP server.
    const ldapConfig = setupConfigGenerator(mockLDAPServer.getHostAndPort());

    // Launch and set up the ShardingTest.
    TestData.skipCheckingIndexesConsistentAcrossCluster = true;
    TestData.skipCheckOrphans = true;
    TestData.skipCheckDBHashes = true;
    TestData.skipCheckShardFilteringMetadata = true;

    const shardedConfig = ldapConfig.generateShardingConfig();
    shardedConfig.other.writeConcernMajorityJournalDefault = false;
    shardedConfig.other.configOptions.setParameter.ldapUserCacheRefreshInterval =
        kLdapUserCacheRefreshInterval;
    shardedConfig.other.configOptions.setParameter.ldapUserCacheStalenessInterval =
        kLdapUserCacheStalenessInterval;
    shardedConfig.other.mongosOptions.setParameter.userCacheInvalidationIntervalSecs =
        kMongosCacheInvalidationInterval;

    const shardedCluster = new ShardingTest(shardedConfig);
    const mongos = shardedCluster.s0;
    const isReplicaSetEndpointActive = shardedCluster.isReplicaSetEndpointActive();

    setupTest(mongos);
    runCommonTest(mongos, mockLDAPServer, kShardedRefreshSleepMS, kShardedStalenessSleepMS);

    // Shut the cluster down. The mock LDAP server should have already been shut down as part of
    // the staleness test.
    // TODO (SERVER-83433): Add back the test coverage for running db hash check and validation
    // on replica set that is fsync locked and has replica set endpoint enabled.
    shardedCluster.stop({
        skipCheckDBHashes: isReplicaSetEndpointActive,
        skipValidation: isReplicaSetEndpointActive
    });
}

function runShardedCacheOverflowTest() {
    // Start a mock LDAP server.
    const mockLDAPServer = new MockLDAPServer(
        'src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldap_mock_server_dit.ldif');
    mockLDAPServer.start();

    // Then, configure mongod to talk to the mock LDAP server.
    const ldapConfig = setupConfigGenerator(mockLDAPServer.getHostAndPort(), 1);

    // Launch and set up the ShardingTest.
    TestData.skipCheckingIndexesConsistentAcrossCluster = true;
    TestData.skipCheckOrphans = true;
    TestData.skipCheckDBHashes = true;

    const shardedConfig = ldapConfig.generateShardingConfig();
    shardedConfig.other.writeConcernMajorityJournalDefault = false;
    shardedConfig.other.configOptions.setParameter.ldapUserCacheRefreshInterval =
        kLdapUserCacheRefreshInterval;
    shardedConfig.other.configOptions.setParameter.ldapUserCacheStalenessInterval =
        kLdapUserCacheStalenessInterval;
    shardedConfig.other.mongosOptions.setParameter.userCacheInvalidationIntervalSecs =
        kMongosCacheInvalidationInterval;

    const shardedCluster = new ShardingTest(shardedConfig);
    const mongos = shardedCluster.s0;
    const isReplicaSetEndpointActive = shardedCluster.isReplicaSetEndpointActive();

    setupTest(mongos);
    // Auth as ldapz_ldap1 and maintain an idle connection.
    const externalDB = mongos.getDB('$external');
    assert(externalDB.auth({
        user: ldapUserOne.userName,
        pwd: ldapUserOne.pwd,
        mechanism: 'PLAIN',
        digestPassword: false,
    }));
    const testDB = mongos.getDB('test');

    // Launch another client to auth as ldapz_ldap2. This should
    // knock ldapz_ldap1 out of the config server's cache.
    let awaitClientTwo =
        startParallelShell(funWithArgs(clientRefreshCallback, ldapUserTwo, 'success'), mongos.port);
    awaitClientTwo();

    // Launch the mock LDAP write client to modify ldapz_ldap1 such that it loses its testWriter
    // role.
    const ldapWriteClient = new LDAPWriteClient(mockLDAPServer.getPort());
    ldapWriteClient.executeWriteOp(defaultRole, "cn=ldapz_ldap1," + defaultUserDNSuffix, "delete");

    // Sleep for the mongos refresh interval, and then verify that the open connection cannot write.
    sleep(kShardedRefreshSleepMS);
    assert.writeError(testDB.test.insert({a: 1}),
                      "Refresh did not occur, can still write after LDAP server role update");

    // Shut everything down.
    // TODO (SERVER-83433): Add back the test coverage for running db hash check and validation
    // on replica set that is fsync locked and has replica set endpoint enabled.
    shardedCluster.stop({
        skipCheckDBHashes: isReplicaSetEndpointActive,
        skipValidation: isReplicaSetEndpointActive
    });
    mockLDAPServer.stop();
}

runMongodTest();
runShardedTest();
runShardedCacheOverflowTest();