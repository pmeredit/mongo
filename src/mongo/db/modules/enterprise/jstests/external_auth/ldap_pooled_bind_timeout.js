/**
 * Tests that binds that occur on timed out pooled LDAP connections occur without incident but
 * while resulting in failed authentication.
 * @tags: [requires_ldap_pool]
 */

import {configureFailPoint} from "jstests/libs/fail_point_util.js";
import {ShardingTest} from "jstests/libs/shardingtest.js";
import {
    defaultUserDNSuffix,
    LDAPTestConfigGenerator,
    setupTest
} from "src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldap_authz_lib.js";

function assertPostTimeoutLogExists(adminDB) {
    checkLog.containsJson(adminDB, 24060, {});
}

function runTest() {
    TestData.skipCheckingIndexesConsistentAcrossCluster = true;
    TestData.skipCheckOrphans = true;
    TestData.skipCheckDBHashes = true;
    TestData.skipCheckShardFilteringMetadata = true;

    const configGenerator = new LDAPTestConfigGenerator();
    configGenerator.startMockupServer();
    configGenerator.ldapValidateLDAPServerConfig = false;
    configGenerator.ldapUserToDNMapping =
        [{match: "(.+)", substitution: "cn={0}," + defaultUserDNSuffix}];
    configGenerator.ldapAuthzQueryTemplate = "{USER}?memberOf";
    const shardingConfig = configGenerator.generateShardingConfig();
    shardingConfig.other.writeConcernMajorityJournalDefault = false;

    // Launch sharded cluster with the above LDAP config.
    const st = new ShardingTest(shardingConfig);
    const mongos = st.s0;
    setupTest(mongos);

    // Set the log level to 5 using an admin-authenticated connection.
    const siteRootAdminConn = new Mongo(mongos.host);
    const adminDB = siteRootAdminConn.getDB('admin');
    assert(adminDB.auth("siteRootAdmin", "secret"));
    assert.commandWorked(adminDB.setLogLevel(5));

    // Configure fail point to force the bind operation itself to hang as long as the fail point
    // is on.
    const bindTimeoutFp = configureFailPoint(mongos, 'ldapBindTimeoutHangIndefinitely');

    // Auth as ldapz_ldap1. This should fail due to timeout.
    const externalDB = mongos.getDB('$external');
    assert(!externalDB.auth({
        user: 'ldapz_ldap1',
        pwd: 'Secret123',
        mechanism: 'PLAIN',
        digestPassword: false,
    }));

    // Now, turn the failpoint off and check that the server successfully emits a log indicating
    // that the bind completed normally after the timeout, with the result thrown away.
    bindTimeoutFp.off();
    assertPostTimeoutLogExists(adminDB);

    // TODO (SERVER-83433): Add back the test coverage for running db hash check and validation
    // on replica set that is fsync locked and has replica set endpoint enabled.
    const isReplicaSetEndpointActive = st.isReplicaSetEndpointActive();
    st.stop({
        skipCheckDBHashes: isReplicaSetEndpointActive,
        skipValidation: isReplicaSetEndpointActive
    });
    configGenerator.stopMockupServer();
}

runTest();
