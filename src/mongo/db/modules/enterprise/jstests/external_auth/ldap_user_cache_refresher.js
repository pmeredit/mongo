/**
 * Tests that the LDAPUserCachePoller refreshes entries at the configured interval and
 * invalidates entries when refreshes fail and the configured staleness interval
 * passes.
 */

(function() {
'use strict';

load("jstests/libs/parallel_shell_helpers.js");
load("src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldap_authz_lib.js");

// Consts used in tests.
const ldapUserOne = {
    userName: 'ldapz_ldap1',
    pwd: 'Secret123',
};
const ldapUserTwo = {
    userName: 'ldapz_ldap2',
    pwd: 'Secret123',
};
const mockServerPath =
    "src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldapmockserver.py";
const mockWriteClientPath =
    "src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldapclient.py";
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
// that it's able to continue using its cached privileges to insert immediately after the LDAP
// server goes down, but should eventually find itself unable to perform anything after the cache
// gets invalidated due to failed refreshes.
function testStalenessInterval({userName, pwd}, conn, ldapServerPid, stalenessInterval) {
    const externalDB = conn.getDB('$external');
    assert(externalDB.auth({
        user: userName,
        pwd: pwd,
        mechanism: 'PLAIN',
        digestPassword: false,
    }));

    jsTest.log("Shutting down LDAP server");
    stopMongoProgramByPid(ldapServerPid);

    const testDB = conn.getDB('test');
    assert.writeOK(testDB.test.insert({a: 1}), "Cannot write immediately after LDAP server outage");

    sleep(stalenessInterval);
    assert.writeError(testDB.test.insert({a: 1}),
                      "can write after LDAP server outage + staleness interval");
}

// Instantiates the LDAP config generator with the common params needed for both mongod and mongos.
function setupConfigGenerator(mockServerHostAndPort, authzManagerCacheSize = 100) {
    let ldapConfig = new LDAPTestConfigGenerator();
    ldapConfig.ldapServers = [mockServerHostAndPort];
    ldapConfig.ldapUserToDNMapping = [
        {match: "(ldapz_ldap1)", substitution: "cn={0}," + defaultUserDNSuffix},
        {match: "(ldapz_ldap2)", substitution: "cn={0}," + defaultUserDNSuffix},
    ];
    ldapConfig.ldapAuthzQueryTemplate = "{USER}?memberOf";
    ldapConfig.authorizationManagerCacheSize = authzManagerCacheSize;

    return ldapConfig;
}

// Starts the mock LDAP server at a randomized port, asserts that it has started and is listening
// for new connections, and returns an object with the PID and the hostname:port it is running on.
function startMockLDAPServer() {
    const mockServerPort = allocatePort();
    const mockServerHostAndPort = `localhost:${mockServerPort}`;

    clearRawMongoProgramOutput();

    const ldapServerPid =
        startMongoProgramNoConnect("python", mockServerPath, "--port", mockServerPort);
    assert(checkProgram(ldapServerPid));
    assert.soon(() => rawMongoProgramOutput().search("LDAPServerFactory starting on " +
                                                     mockServerPort) !== -1);

    return {
        ldapServerPid: ldapServerPid,
        mockServerHostAndPort: mockServerHostAndPort,
        mockServerPort: mockServerPort,
    };
}

// Runs the core of the test, which involves verifying that cached role information is used within
// the staleness interval and that cached role information gets updated after the refresh interval.
function runCommonTest(conn, ldapServerPid, mockServerPort, refreshInterval, stalenessInterval) {
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
    const ldapWriteClientPid = startMongoProgramNoConnect("python",
                                                          mockWriteClientPath,
                                                          "--targetPort",
                                                          mockServerPort,
                                                          "--group",
                                                          defaultRole,
                                                          "--user",
                                                          "cn=ldapz_ldap1," + defaultUserDNSuffix,
                                                          "--modifyAction",
                                                          "delete");
    assert.eq(waitProgram(ldapWriteClientPid), 0);

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
    testStalenessInterval(ldapUserTwo, conn, ldapServerPid, stalenessInterval);
}

function runMongodTest() {
    // First, launch the mock LDAP server.
    const mockServerInfo = startMockLDAPServer();

    // Then, configure mongod to talk to the mock LDAP server.
    const ldapConfig = setupConfigGenerator(mockServerInfo.mockServerHostAndPort);
    const mongodConfig = ldapConfig.generateMongodConfig();
    mongodConfig.setParameter.ldapUserCacheRefreshInterval = kLdapUserCacheRefreshInterval;
    mongodConfig.setParameter.ldapUserCacheStalenessInterval = kLdapUserCacheStalenessInterval;

    // Launch and set up mongod.
    const mongod = MongoRunner.runMongod(mongodConfig);
    setupTest(mongod);

    runCommonTest(mongod,
                  mockServerInfo.ldapServerPid,
                  mockServerInfo.mockServerPort,
                  kMongodRefreshSleepMS,
                  kMongodStalenessSleepMS);

    // Shut down the mongod.
    MongoRunner.stopMongod(mongod);
}

function runShardedTest() {
    // First, launch the mock LDAP server.
    const mockServerInfo = startMockLDAPServer();

    // Then, configure mongod to talk to the mock LDAP server.
    const ldapConfig = setupConfigGenerator(mockServerInfo.mockServerHostAndPort);

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
    setupTest(mongos);
    runCommonTest(mongos,
                  mockServerInfo.ldapServerPid,
                  mockServerInfo.mockServerPort,
                  kShardedRefreshSleepMS,
                  kShardedStalenessSleepMS);

    // Shut down the sharded cluster.
    shardedCluster.stop();
}

function runShardedCacheOverflowTest() {
    // First, launch the mock LDAP server.
    const mockServerInfo = startMockLDAPServer();

    // Then, configure mongod to talk to the mock LDAP server.
    const ldapConfig = setupConfigGenerator(mockServerInfo.mockServerHostAndPort, 1);

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
    const ldapWriteClientPid = startMongoProgramNoConnect("python",
                                                          mockWriteClientPath,
                                                          "--targetPort",
                                                          mockServerInfo.mockServerPort,
                                                          "--group",
                                                          defaultRole,
                                                          "--user",
                                                          "cn=ldapz_ldap1," + defaultUserDNSuffix,
                                                          "--modifyAction",
                                                          "delete");
    assert.eq(waitProgram(ldapWriteClientPid), 0);

    // Sleep for the mongos refresh interval, and then verify that the open connection cannot write.
    sleep(kShardedRefreshSleepMS);
    assert.writeError(testDB.test.insert({a: 1}),
                      "Refresh did not occur, can still write after LDAP server role update");

    // Shut everything down.
    externalDB.logout();
    shardedCluster.stop();
    stopMongoProgramByPid(mockServerInfo.ldapServerPid);
}

runMongodTest();
runShardedTest();
runShardedCacheOverflowTest();
})();
