/**
 * Test that user acquisition stats, tracked in curOp,
 * for LDAP Operations are appropriately updated through
 * the UserAcquisitionStatsHandle used in the LDAP related code.
 * @tags: [requires_ldap_pool]
 */

(function() {
'use strict';
load("src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldap_authz_lib.js");

function hasCommandLogEntry(conn, id, command, attributes, count) {
    let expectedLog = {command: command};
    if (Object.keys(attributes).length > 0) {
        expectedLog = Object.assign({}, expectedLog, attributes);
    }
    checkLog.containsRelaxedJson(conn, id, expectedLog, count);
}

function hasNoCommandLogEntry(conn, id, command, attributes) {
    let expectedLog = {command: command};
    if (Object.keys(attributes).length > 0) {
        expectedLog = Object.assign({}, expectedLog, attributes);
    }
    checkLog.containsRelaxedJson(conn, id, expectedLog, 0);
}

function runTest(conn, mode) {
    // Set logging level to 1 so that we log all operations that aren't slow
    const adminDB = conn.getDB('admin');
    assert.commandWorked(adminDB.runCommand({createUser: 'admin', pwd: 'pwd', roles: ['root']}));
    assert(adminDB.auth('admin', 'pwd'));
    assert.commandWorked(adminDB.runCommand({setParameter: 1, logLevel: 1}));
    adminDB.logout();

    // Users and authorization options for each user
    var user1 = "ldapz_ldap1";
    var user2 = "ldapz_ldap2";
    var userOneAuthOptions =
        {user: user1, pwd: defaultPwd, mechanism: "PLAIN", digestPassword: false};
    var userTwoAuthOptions =
        {user: user2, pwd: defaultPwd, mechanism: "PLAIN", digestPassword: false};

    // using $external for LDAP, authorize users
    const externalDB = conn.getDB("$external");
    assert(externalDB.auth(userOneAuthOptions));
    externalDB.logout();

    const waitTimeRegex = new RegExp('^[1-9][0-9]*$', 'i');
    const logID = 51803;
    let expectedSaslStartCommandLog = {saslStart: 1};
    let expectedCommandUserOneAuth = {
        LDAPOperations: {
            LDAPNumberOfReferrals: 0,
            bindStats: {numOp: 1, opDurationMicros: waitTimeRegex},
            searchStats: {numOp: 0, opDurationMicros: 0},
            unbindStats: {numOp: 0, opDurationMicros: 0}
        },
    };
    assert(adminDB.auth('admin', 'pwd'));
    hasCommandLogEntry(adminDB, logID, expectedSaslStartCommandLog, expectedCommandUserOneAuth, 1);
    let ldapOperations = adminDB.serverStatus().ldapOperations;
    print("Cumulative LDAP operations:" + JSON.stringify(ldapOperations));
    assert.eq(1, ldapOperations.bindStats.numOp);
    assert.gt(ldapOperations.bindStats.opDurationMicros, 0);
    adminDB.logout();

    externalDB.auth(userTwoAuthOptions);
    externalDB.logout();
    let expectedCommandUserTwoAuth = {
        LDAPOperations: {
            LDAPNumberOfReferrals: 0,
            bindStats: {numOp: 1, opDurationMicros: waitTimeRegex},
            searchStats: {numOp: 1, opDurationMicros: waitTimeRegex},
            unbindStats: {numOp: 0, opDurationMicros: 0}
        },
    };
    assert(adminDB.auth('admin', 'pwd'));
    hasCommandLogEntry(adminDB, logID, expectedSaslStartCommandLog, expectedCommandUserTwoAuth, 1);
    ldapOperations = adminDB.serverStatus().ldapOperations;
    print("Cumulative LDAP operations:" + JSON.stringify(ldapOperations));
    assert.eq(2, ldapOperations.bindStats.numOp);
    assert.eq(1, ldapOperations.searchStats.numOp);
    assert.gt(ldapOperations.searchStats.opDurationMicros, 0);
    adminDB.logout();
}
// Standalone
var configGenerator = new LDAPTestConfigGenerator();
configGenerator.ldapAuthzQueryTemplate = "{USER}?memberOf";
configGenerator.ldapUserToDNMapping = [
    {match: "(ldapz_ldap1)", substitution: "cn={0}," + defaultUserDNSuffix},
    {match: "(ldapz_ldap2)", ldapQuery: defaultUserDNSuffix + "??one?(cn={0})"}
];
var config = MongoRunner.mongodOptions(configGenerator.generateMongodConfig());
var m = MongoRunner.runMongod(config);
jsTest.log('Starting ldap_operation_stats.js Standalone');
runTest(m, 'Standalone');
jsTest.log('SUCCESS ldap_operation_stats.js Standalone');
MongoRunner.stopMongod(m);
})();
