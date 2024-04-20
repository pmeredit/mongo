/**
 * Test that the appropriate logs are emitted when making TLS connections to
 * LDAP servers.
 * @tags: [requires_ldap_pool]
 */

import {isRHEL8} from "jstests/libs/os_helpers.js";
import {
    defaultPwd,
    defaultUserDNSuffix,
    LDAPTestConfigGenerator,
    setupTest
} from "src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldap_authz_lib.js";

if (!isRHEL8()) {
    jsTestLog(
        'Not running on RHEL 8.0 - skipping test since TLS with ldaptest.10gen.cc only works on RHEL 8.0');
    quit();
}

function launchCluster(transportSecurity, ldapEnableOpenLDAPLogging) {
    TestData.skipCheckingIndexesConsistentAcrossCluster = true;
    TestData.skipCheckOrphans = true;
    TestData.skipCheckDBHashes = true;
    TestData.skipCheckShardFilteringMetadata = true;

    const configGenerator = new LDAPTestConfigGenerator();
    configGenerator.ldapAuthzQueryTemplate = "{USER}?memberOf";
    configGenerator.ldapUserToDNMapping = [
        {match: "(ldapz_ldap1)", substitution: "cn={0}," + defaultUserDNSuffix},
        {match: "(ldapz_ldap2)", ldapQuery: defaultUserDNSuffix + "??one?(cn={0})"}
    ];
    configGenerator.ldapTransportSecurity = transportSecurity;

    const ldapConfig = configGenerator.generateShardingConfig();
    ldapConfig.other.writeConcernMajorityJournalDefault = false;
    ldapConfig.other.configOptions.setParameter.ldapEnableOpenLDAPLogging =
        ldapEnableOpenLDAPLogging;
    ldapConfig.other.mongosOptions.setParameter.ldapEnableOpenLDAPLogging =
        ldapEnableOpenLDAPLogging;

    const st = new ShardingTest(ldapConfig);
    setupTest(st.s0);
    return st;
}

function runTest(conn, useTLS, enableOpenLDAPLogging) {
    // Set logging level to 1 so that we log all operations that aren't slow
    const adminDB = conn.getDB('admin');
    assert(adminDB.auth('siteRootAdmin', 'secret'));
    assert.commandWorked(adminDB.runCommand({setParameter: 1, logLevel: 1}));
    adminDB.logout();

    const externalDB = conn.getDB("$external");
    assert(externalDB.auth({
        user: 'ldapz_ldap1',
        pwd: defaultPwd,
        mechanism: 'PLAIN',
        digestPassword: false,
    }));
    externalDB.logout();

    assert(adminDB.auth('siteRootAdmin', 'secret'));
    if (useTLS) {
        checkLog.containsJson(adminDB, 7997802, {});
    } else {
        assert(checkLog.checkContainsWithCountJson(adminDB,
                                                   7997802,
                                                   {},
                                                   /*expectedCount=*/ 0));
    }

    if (enableOpenLDAPLogging) {
        checkLog.containsJson(adminDB, 7997801, {});
    } else {
        assert(checkLog.checkContainsWithCountJson(adminDB,
                                                   7997801,
                                                   {},
                                                   /*expectedCount=*/ 0));
    }
}

{
    const st = launchCluster('tls', true /*enableOpenLDAPLogging*/);
    const isReplicaSetEndpointActive = st.isReplicaSetEndpointActive();
    runTest(st.s0, true /*useTLS*/, true /*enableOpenLDAPLogging*/);
    // TODO (SERVER-83433): Add back the test coverage for running db hash check and validation
    // on replica set that is fsync locked and has replica set endpoint enabled.
    st.stop({
        skipCheckDBHashes: isReplicaSetEndpointActive,
        skipValidation: isReplicaSetEndpointActive
    });
}

{
    const st = launchCluster('none', true /*enableOpenLDAPLogging*/);
    const isReplicaSetEndpointActive = st.isReplicaSetEndpointActive();
    runTest(st.s0, false /*useTLS*/, true /*enableOpenLDAPLogging*/);
    // TODO (SERVER-83433): Add back the test coverage for running db hash check and validation
    // on replica set that is fsync locked and has replica set endpoint enabled.
    st.stop({
        skipCheckDBHashes: isReplicaSetEndpointActive,
        skipValidation: isReplicaSetEndpointActive
    });
}

{
    const st = launchCluster('tls', false /*enableOpenLDAPLogging*/);
    const isReplicaSetEndpointActive = st.isReplicaSetEndpointActive();
    runTest(st.s0, true /*useTLS*/, false /*enableOpenLDAPLogging*/);
    // TODO (SERVER-83433): Add back the test coverage for running db hash check and validation
    // on replica set that is fsync locked and has replica set endpoint enabled.
    st.stop({
        skipCheckDBHashes: isReplicaSetEndpointActive,
        skipValidation: isReplicaSetEndpointActive
    });
}

{
    const st = launchCluster('none', false /*enableOpenLDAPLogging*/);
    const isReplicaSetEndpointActive = st.isReplicaSetEndpointActive();
    runTest(st.s0, false /*useTLS*/, false /*enableOpenLDAPLogging*/);
    // TODO (SERVER-83433): Add back the test coverage for running db hash check and validation
    // on replica set that is fsync locked and has replica set endpoint enabled.
    st.stop({
        skipCheckDBHashes: isReplicaSetEndpointActive,
        skipValidation: isReplicaSetEndpointActive
    });
}
