/**
 * Tests that the server chases LDAP referrals seamlessly.
 */

import {
    authAndVerify,
    setupTest
} from "src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldap_authz_lib.js";
import {
    MockLDAPServer,
    setupConfigGenerator
} from "src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldap_mock_server_utils.js";

function setupLDAPServers(shouldUseConnectionPool) {
    // Start the LDAP server that will be referred to by another LDAP server. Mongod will not be
    // aware of this LDAP server.
    const backgroundLDAPServer = new MockLDAPServer(
        'src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldap_mock_server_dit.ldif');
    backgroundLDAPServer.start();

    // Start the LDAP server that mongod will connect to. It will refer all search queries to
    // backgroundLDAPServer.
    const referringLDAPServer = new MockLDAPServer(
        'src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldap_mock_server_dit.ldif',
        backgroundLDAPServer.getHostAndPort());
    referringLDAPServer.start();

    // Generate the config generator to communicate with the referring LDAP server.
    const ldapConfig =
        setupConfigGenerator(referringLDAPServer.getHostAndPort(), 100, shouldUseConnectionPool);

    return {
        backgroundServer: backgroundLDAPServer,
        referringServer: referringLDAPServer,
        configGenerator: ldapConfig,
    };
}

function runTest(connection) {
    // Authenticate to mongod as an LDAP user. This should trigger a referral when retrieving the
    // user's roles via a search query.
    const userOptions = {
        user: 'ldapz_ldap1',
        pwd: 'Secret123',
        mechanism: 'PLAIN',
        digestPassword: false,
    };
    authAndVerify({conn: connection, options: {authOptions: userOptions, user: 'ldapz_ldap1'}});
}

function runMongodTest(shouldUseConnectionPool) {
    const ldapServerInfo = setupLDAPServers(shouldUseConnectionPool);

    // Configure mongod to talk to the referring LDAP server.
    const mongodConfig = ldapServerInfo.configGenerator.generateMongodConfig();
    const mongod = MongoRunner.runMongod(mongodConfig);
    setupTest(mongod);
    runTest(mongod);

    // Shut everything down.
    MongoRunner.stopMongod(mongod);
    ldapServerInfo.referringServer.stop();
    ldapServerInfo.backgroundServer.stop();
}

function runShardedTest(shouldUseConnectionPool) {
    const ldapServerInfo = setupLDAPServers(shouldUseConnectionPool);

    // Configure the sharded cluster to talk to the referring LDAP server and skip unnecessary
    // checks.
    TestData.skipCheckingIndexesConsistentAcrossCluster = true;
    TestData.skipCheckOrphans = true;
    TestData.skipCheckDBHashes = true;
    TestData.skipCheckShardFilteringMetadata = true;

    const shardedConfig = ldapServerInfo.configGenerator.generateShardingConfig();
    shardedConfig.other.writeConcernMajorityJournalDefault = false;

    const shardedCluster = new ShardingTest(shardedConfig);
    const isReplicaSetEndpointActive = shardedCluster.isReplicaSetEndpointActive();

    setupTest(shardedCluster.s0);
    runTest(shardedCluster.s0);

    // Shut everything down.
    // TODO (SERVER-83433): Add back the test coverage for running db hash check and validation
    // on replica set that is fsync locked and has replica set endpoint enabled.
    shardedCluster.stop({
        skipCheckDBHashes: isReplicaSetEndpointActive,
        skipValidation: isReplicaSetEndpointActive
    });
    ldapServerInfo.referringServer.stop();
    ldapServerInfo.backgroundServer.stop();
}

runMongodTest(true /* shouldUseConnectionPool */);
runMongodTest(false /* shouldUseConnectionPool */);
runShardedTest(true /* shouldUseConnectionPool */);
runShardedTest(false /* shouldUseConnectionPool */);
