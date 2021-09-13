/**
 * Tests that the LDAPUserCachePoller iterates at the configured interval and
 * invalidates entries when refreshes fail and the configured staleness interval
 * passes.
 */

(function() {
'use strict';

load("jstests/libs/parallel_shell_helpers.js");
load("src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldap_authz_lib.js");

// Function ran by client shells in parallel.
const clientCallback = function({userName, pwd}) {
    const externalDB = db.getMongo().getDB('$external');
    assert(externalDB.auth({
        user: userName,
        pwd: pwd,
        mechanism: 'PLAIN',
        digestPassword: false,
    }));
    externalDB.logout();
};

// Consts used in tests.
const ldapUser = {
    userName: 'ldapz_ldap2',
    pwd: 'Secret123',
};
const kLDAPSearchLogId = 24033;
const kCacheRefreshLogId = 5914800;
const kMongodCacheInvalidationLogId = 5914802;
const kMongosCacheInvalidationLogId = 20263;
const kUserCacheInvalidationIntervalMS = 30000;
const kContextRegex = new RegExp('AuthorizationManager.*|conn.*', 'i');
const kAtLeastComparator = (actual, expected) => {
    return actual >= expected;
};
const kExactComparator = (actual, expected) => {
    return actual === expected;
};

function runMongodTest() {
    let ldapConfig = new LDAPTestConfigGenerator();
    // Set up mongod to refresh entries every 5 seconds and have a max staleness interval of 15
    // seconds.
    ldapConfig.ldapAuthzQueryTemplate = "{USER}?memberOf";
    ldapConfig.ldapUserToDNMapping = [
        {match: "(ldapz_ldap2)", substitution: "cn={0}," + defaultUserDNSuffix},
    ];
    const mongodConfig = ldapConfig.generateMongodConfig();
    mongodConfig.setParameter.ldapShouldRefreshUserCacheEntries = true;
    mongodConfig.setParameter.ldapUserCacheRefreshInterval = 5;
    mongodConfig.setParameter.ldapUserCacheStalenessInterval = 15;

    // Launch and set up mongod.
    const mongod = MongoRunner.runMongod(mongodConfig);
    setupTest(mongod);

    // Ensure that the refresh and invalidation logs are visible.
    const adminDB = mongod.getDB('admin');
    assert(adminDB.auth('siteRootAdmin', 'secret'));
    assert.commandWorked(adminDB.setLogLevel(2));

    // Log in as an LDAP user to ensure it gets loaded into the user cache. Check that this
    // triggered a LDAP search operation.
    let awaitClient = startParallelShell(funWithArgs(clientCallback, ldapUser), mongod.port);
    awaitClient();
    checkLog.containsRelaxedJson(
        adminDB, kLDAPSearchLogId, {}, 1, 300000, kExactComparator, kContextRegex);

    // Launch a client to auth again as the LDAP user. This time, the auth operation should not
    // require a search operation. Additionally, the server should have refreshed its cache at least
    // three times but should not have invalidated anything.
    awaitClient = startParallelShell(funWithArgs(clientCallback, ldapUser), mongod.port);
    awaitClient();
    checkLog.containsRelaxedJson(adminDB, kCacheRefreshLogId, {}, 3, 300000, kAtLeastComparator);
    checkLog.containsRelaxedJson(adminDB, kMongodCacheInvalidationLogId, {}, 0);
    checkLog.containsRelaxedJson(
        adminDB, kLDAPSearchLogId, {}, 1, 300000, kExactComparator, kContextRegex);

    // Set the failToRefreshLDAPUsers failpoint, which will trick the server into thinking that it
    // is unable to refresh the cached LDAP user. Then, check that logging in requires a search
    // operation and that the server logged the invalidation of its cache.
    assert.commandWorked(adminDB.runCommand({
        configureFailPoint: 'failToRefreshLDAPUsers',
        mode: 'alwaysOn',
    }));

    checkLog.containsRelaxedJson(adminDB, kMongodCacheInvalidationLogId, {}, 1);
    awaitClient = startParallelShell(funWithArgs(clientCallback, ldapUser), mongod.port);
    awaitClient();
    checkLog.containsRelaxedJson(
        adminDB, kLDAPSearchLogId, {}, 2, 300000, kExactComparator, kContextRegex);

    // Shut down the mongod.
    MongoRunner.stopMongod(mongod);
}

function runShardedTest() {
    let ldapConfig = new LDAPTestConfigGenerator();
    ldapConfig.ldapAuthzQueryTemplate = "{USER}?memberOf";
    ldapConfig.ldapUserToDNMapping = [
        {match: "(ldapz_ldap2)", substitution: "cn={0}," + defaultUserDNSuffix},
    ];

    // Launch and set up the ShardingTest.
    TestData.skipCheckingIndexesConsistentAcrossCluster = true;
    TestData.skipCheckOrphans = true;
    TestData.skipCheckDBHashes = true;

    const shardedConfig = ldapConfig.generateShardingConfig();
    shardedConfig.other.writeConcernMajorityJournalDefault = false;
    shardedConfig.other.configOptions.setParameter.ldapShouldRefreshUserCacheEntries = true;
    shardedConfig.other.configOptions.setParameter.ldapUserCacheRefreshInterval = 5;
    shardedConfig.other.configOptions.setParameter.ldapUserCacheStalenessInterval = 15;
    shardedConfig.other.mongosOptions.setParameter.userCacheInvalidationIntervalSecs = 30;

    const shardedCluster = new ShardingTest(shardedConfig);
    setupTest(shardedCluster.s0);
    // Sleep for the userCacheInvalidationInterval to ensure that the user cache invalidations that
    // occur due to user/role creation in setupTest() propagate to the mongos.
    sleep(kUserCacheInvalidationIntervalMS);

    const mongos = shardedCluster.s0;
    const configPrimary = shardedCluster.c0;

    // Ensure that the refresh and invalidation logs are visible on the config server only.
    const mongosAdminDB = mongos.getDB('admin');
    assert(mongosAdminDB.auth('siteRootAdmin', 'secret'));
    assert.commandWorked(mongosAdminDB.setLogLevel(2));
    const configAdminDB = configPrimary.getDB('admin');
    assert(configAdminDB.auth('siteRootAdmin', 'secret'));
    assert.commandWorked(configAdminDB.setLogLevel(2));

    // Check that mongo user cache has been invalidated in response to siteRootAdmin user/role
    // creation during setup.
    checkLog.containsRelaxedJson(
        mongosAdminDB, kMongosCacheInvalidationLogId, {}, 1, 300000, kExactComparator);

    // Log in as an LDAP user to ensure it gets loaded into the user cache. Check that this
    // triggered a LDAP search operation on the config server.
    let awaitClient = startParallelShell(funWithArgs(clientCallback, ldapUser), mongos.port);
    awaitClient();
    checkLog.containsRelaxedJson(
        configAdminDB, kLDAPSearchLogId, {}, 1, 300000, kExactComparator, kContextRegex);

    // Launch a client to auth again as the LDAP user. Assert that the config server performed
    // refreshes in the meantime and did not invalidate its cache and that mongos DID NOT invalidate
    // its cache in response to the config server refreshes because the refreshes should not have
    // resulted in changed user information.
    awaitClient = startParallelShell(funWithArgs(clientCallback, ldapUser), mongos.port);
    awaitClient();
    checkLog.containsRelaxedJson(
        configAdminDB, kCacheRefreshLogId, {}, 3, 300000, kAtLeastComparator);
    checkLog.containsRelaxedJson(
        configAdminDB, kMongodCacheInvalidationLogId, {}, 0, 300000, kExactComparator);
    checkLog.containsRelaxedJson(
        mongosAdminDB, kMongosCacheInvalidationLogId, {}, 1, 300000, kExactComparator);

    // Set the failToRefreshLDAPUsers failpoint on the config server, which will trick it into
    // thinking that it is unable to refresh the cached LDAP user. Then, assert that the config
    // server invalidated its user cache and that mongos also invalidates its user cache in response
    // after userCacheInvalidationIntervalSecs. Since the LDAP server is still up, auth should occur
    // normally even after the invalidation.
    assert.commandWorked(configAdminDB.runCommand({
        configureFailPoint: 'failToRefreshLDAPUsers',
        mode: 'alwaysOn',
    }));
    checkLog.containsRelaxedJson(
        configAdminDB, kMongodCacheInvalidationLogId, {}, 1, 300000, kExactComparator);
    sleep(kUserCacheInvalidationIntervalMS);
    checkLog.containsRelaxedJson(
        mongosAdminDB, kMongosCacheInvalidationLogId, {}, 2, 300000, kExactComparator);
    awaitClient = startParallelShell(funWithArgs(clientCallback, ldapUser), mongos.port);
    awaitClient();

    shardedCluster.stop();
}

runMongodTest();
runShardedTest();
})();
