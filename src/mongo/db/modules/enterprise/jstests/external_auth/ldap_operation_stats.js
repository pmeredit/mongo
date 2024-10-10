/**
 * Test that user acquisition stats, tracked in curOp,
 * for LDAP Operations are appropriately updated through
 * the UserAcquisitionStatsHandle used in the LDAP related code.
 * @tags: [requires_ldap_pool]
 */

import {configureFailPoint} from "jstests/libs/fail_point_util.js";
import {determineSSLProvider} from "jstests/ssl/libs/ssl_helpers.js";
import {
    defaultPwd
} from "src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldap_authz_lib.js";
import {
    MockLDAPServer,
    setupConfigGenerator
} from "src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldap_mock_server_utils.js";

// The values of these metrics vary on Windows due to differences in how it chases referrals, so
// this test is only run on Linux.
if (determineSSLProvider() !== 'openssl') {
    print('Skipping test, ldap_operation_stats.js is only run on Linux');
    quit();
}

// Users and authorization options for each user
const user1 = "ldapz_ldap1";
const user2 = "ldapz_ldap2";
const userOneAuthOptions = {
    user: user1,
    pwd: defaultPwd,
    mechanism: "PLAIN",
    digestPassword: false
};
const userTwoAuthOptions = {
    user: user2,
    pwd: defaultPwd,
    mechanism: "PLAIN",
    digestPassword: false
};

// Expected logs.
const waitTimeRegex = new RegExp('^[1-9][0-9]*$', 'i');
const logID = 51803;
const expectedUserOneUsersInfoCommandLog = {
    usersInfo: {
        user: user1,
        db: "$external",
    },
};
const expectedUserTwoUsersInfoCommandLog = {
    usersInfo: {
        user: user2,
        db: "$external",
    },
};
let expectedUserOneUsersInfoStats = {
    LDAPOperations: {
        LDAPNumberOfSuccessfulReferrals: 1,
        LDAPNumberOfFailedReferrals: 0,
        LDAPNumberOfReferrals: 1,
        bindStats: {numOp: 1, opDurationMicros: waitTimeRegex},
        searchStats: {numOp: 1, opDurationMicros: waitTimeRegex}
    },
};
let expectedUserTwoUsersInfoStats = {
    LDAPOperations: {
        LDAPNumberOfSuccessfulReferrals: 0,
        LDAPNumberOfFailedReferrals: 4,
        LDAPNumberOfReferrals: 4,
        bindStats: {numOp: 4, opDurationMicros: waitTimeRegex},
        searchStats: {numOp: 4, opDurationMicros: waitTimeRegex}
    },
};

function hasCommandLogEntry(conn, id, command, attributes, count) {
    let expectedLog = {command: command};
    if (Object.keys(attributes).length > 0) {
        expectedLog = Object.assign({}, expectedLog, attributes);
    }
    checkLog.containsRelaxedJson(conn, id, expectedLog, count);
}

function checkCumulativeLDAPMetrics(ldapOperations,
                                    expectedNumSuccessfulReferrals,
                                    expectedNumFailedReferrals,
                                    expectedNumReferrals,
                                    expectedNumBinds,
                                    expectedNumSearches) {
    assert.eq(expectedNumSuccessfulReferrals, ldapOperations.LDAPNumberOfSuccessfulReferrals);
    assert.eq(expectedNumFailedReferrals, ldapOperations.LDAPNumberOfFailedReferrals);
    assert.eq(expectedNumReferrals, ldapOperations.LDAPNumberOfReferrals);
    assert.eq(expectedNumBinds, ldapOperations.bindStats.numOp);
    assert.gt(ldapOperations.bindStats.opDurationMicros, 0);
    assert.eq(expectedNumSearches, ldapOperations.searchStats.numOp);
    assert.gt(ldapOperations.searchStats.opDurationMicros, 0);
}

function runTest(conn) {
    // Create a separate admin-authenticated connection to run admin operations.
    assert.commandWorked(
        conn.getDB('admin').runCommand({createUser: 'admin', pwd: 'pwd', roles: ['root']}));
    const adminConn = new Mongo(conn.host);
    const adminDB = adminConn.getDB('admin');
    assert(adminDB.auth('admin', 'pwd'));

    // Set logging level to 1 so that we log all operations that aren't slow
    assert.commandWorked(adminDB.runCommand({setParameter: 1, logLevel: 1}));

    // using $external for LDAP, authorize users
    const externalDB = conn.getDB("$external");
    assert(externalDB.auth(userOneAuthOptions));
    externalDB.logout();

    // Assert that the LDAP metrics are visible in the slow query log for saslStart and usersInfo
    // and added to the cumulative serverStatus metrics.
    hasCommandLogEntry(
        adminDB, logID, expectedUserOneUsersInfoCommandLog, expectedUserOneUsersInfoStats, 1);
    let ldapOperations = adminDB.serverStatus().ldapOperations;
    print("Cumulative LDAP operations:" + JSON.stringify(ldapOperations));

    // We expect 1 successful referral, 0 failed referrals, 1 total referral, 2 binds, and 1 search
    // operation so far. All of this should come from the userOne auth.
    checkCumulativeLDAPMetrics(ldapOperations, 1, 0, 1, 2, 1);

    // Set a failpoint that should cause mongod to automatically fail when binding to the referred
    // server. This should be reflected in LDAPNumberOfFailedReferrals.
    const ldapReferralFp = configureFailPoint(m, 'ldapReferralFail');
    assert(!externalDB.auth(userTwoAuthOptions));
    ldapReferralFp.off();

    // The cumulative LDAP operation stats should reflect the sum of the previous stats and the new
    // values from the failed referrals (including retries).
    hasCommandLogEntry(
        adminDB, logID, expectedUserTwoUsersInfoCommandLog, expectedUserTwoUsersInfoStats, 1);
    ldapOperations = adminDB.serverStatus().ldapOperations;
    print("Cumulative LDAP operations:" + JSON.stringify(ldapOperations));

    // We expect 1 successful referral, 4 failed referrals, 5 total referrals, 7 binds, and 5 search
    // operations so far. The 4 failed referrals come from 1 failure + 3 retries, the 5 additional
    // binds come from 1 successful bind against the configured LDAP server + 1 failure + 3 retry
    // binds against the referred LDAP server, and the 4 additional searches come from 1 failure + 3
    // retries.
    checkCumulativeLDAPMetrics(ldapOperations, 1, 4, 5, 7, 5);
    adminDB.logout();
}

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

const configGenerator = setupConfigGenerator(referringLDAPServer.getHostAndPort());
const config = MongoRunner.mongodOptions(configGenerator.generateMongodConfig());

// Set the refresh interval to a high number so intervening refreshes don't throw the stats off.
config.setParameter.ldapUserCacheRefreshInterval = 600;
const m = MongoRunner.runMongod(config);

jsTest.log('Starting ldap_operation_stats.js Standalone');
runTest(m);
jsTest.log('SUCCESS ldap_operation_stats.js Standalone');

MongoRunner.stopMongod(m);
referringLDAPServer.stop();
backgroundLDAPServer.stop();
