// @tags: [requires_ldap_pool]

(function() {
'use strict';

load("src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldap_timeout_lib.js");

// Const test options.
const kConnTestOptions = {
    failPoint: kConnectionFailPoint,
    isPooled: true,
    maxPoolSize: kMaxPoolSize,
    ldapConnectionPoolHostRefreshIntervalMillis:
        kDefaultLdapConnectionPoolHostRefreshIntervalMillis,
    ldapTimeoutMS: kLdapTimeoutMS,
    ldapRetryCount: 0,
    slowDelaySecs: kSlowDelaySecs,
    slowResponses: kSlowResponses,
    totalRequests: kTotalRequests,
};

const kBindTestOptions = {
    failPoint: kBindFailPoint,
    isPooled: true,
    maxPoolSize: kMaxPoolSize,
    ldapConnectionPoolHostRefreshIntervalMillis:
        kDefaultLdapConnectionPoolHostRefreshIntervalMillis,
    ldapTimeoutMS: kLdapTimeoutMS,
    ldapRetryCount: 0,
    slowDelaySecs: kSlowDelaySecs,
    slowResponses: kSlowResponses,
    totalRequests: kTotalRequests,
};

const kSearchAuthzTestOptions = {
    failPoint: kSearchFailPoint,
    isPooled: true,
    maxPoolSize: kMaxPoolSize,
    ldapConnectionPoolHostRefreshIntervalMillis:
        kDefaultLdapConnectionPoolHostRefreshIntervalMillis,
    ldapTimeoutMS: kLdapTimeoutMS,
    ldapRetryCount: 0,
    slowDelaySecs: kSlowDelaySecs,
    slowResponses: kSlowResponses,
    totalRequests: kTotalRequests,
};

const kSearchLivenessCheckTestOptions = {
    failPoint: kLivenessCheckFailPoint,
    isPooled: true,
    maxPoolSize: kMaxPoolSize,
    ldapConnectionPoolHostRefreshIntervalMillis: 3000,
    ldapTimeoutMS: kLdapTimeoutMS,
    ldapRetryCount: 0,
    slowDelaySecs: kSlowDelaySecs,
    slowResponses: kSlowResponses,
    totalRequests: kTotalRequests,
};

function pooledTimeoutTestCallback({conn, shardingTest, options}) {
    const failPoint = options.failPoint;
    const slowResponses = options.slowResponses;
    const slowDelaySecs = options.slowDelaySecs;

    // All clients are expected to get a response from the server within the timeout the LDAP
    // connection pool is enforcing plus an error margin to account for network latency.
    // For the search failpoint, the behavior is a bit more complex thanks to lack of
    // parallelization with usersInfo. The server can only process one usersInfo request at a time.
    // Therefore, the slowest usersInfo requests will be delayed by the usersInfoTimeout * the
    // number of slow responses plus the error margin. That timeout should only be attainable if
    // each of the delayed usersInfo commands timed itself out in roughly usersInfoTimeoutMS.
    let expectedPoolTimeoutMS = options.ldapTimeoutMS + kLdapTimeoutErrorDeltaMS;
    if (failPoint === kSearchFailPoint) {
        expectedPoolTimeoutMS =
            (kUsersInfoTimeoutMS + kUsersInfoTimeoutErrorDeltaMS) * slowResponses;
    }

    jsTest.log('Starting pooled timeout test for failpoint ' + failPoint);

    // First, set the log level to D1 so that all commands run on the server get logged.
    setLogLevel(conn);

    if (failPoint === kLivenessCheckFailPoint) {
        // Launch clients to authenticate as ldapz_ldap1 to warm up the connection pool and prevent
        // the failpoint from triggering during the smoke test.
        runClients(
            conn, options, {userName: 'ldapz_ldap1', pwd: defaultPwd}, expectedPoolTimeoutMS);
        setLdapFailPoint(kDisableNativeLDAPTimeoutFailPoint, 'alwaysOn', 3600, conn, shardingTest);
        setLdapFailPoint(failPoint, {'times': slowResponses}, slowDelaySecs, conn, shardingTest);
        const refreshInterval = options.ldapConnectionPoolHostRefreshIntervalMillis;
        sleep(refreshInterval + kLdapTimeoutErrorDeltaMS);

        // Launch another set of clients to authenticate as ldapz_ldap1. Even if the delays during
        // refresh caused the previous connection pool to expire, this should not impact a new set
        // of clients trying to authenticate.
        runClients(
            conn, options, {userName: 'ldapz_ldap1', pwd: defaultPwd}, expectedPoolTimeoutMS);
    } else {
        // Set the failpoint first because we do not need to worry about the failpoint triggering
        // during connection pool setup.
        setLdapFailPoint(kDisableNativeLDAPTimeoutFailPoint, 'alwaysOn', 3600, conn, shardingTest);
        setLdapFailPoint(failPoint, {'times': slowResponses}, slowDelaySecs, conn, shardingTest);
        if (failPoint === kSearchFailPoint) {
            // Launch clients to authenticate as ldapz_ldap2.
            runClients(
                conn, options, {userName: 'ldapz_ldap2', pwd: defaultPwd}, expectedPoolTimeoutMS);
        } else {
            // Launch clients to authenticate as ldapz_ldap1.
            runClients(
                conn, options, {userName: 'ldapz_ldap1', pwd: defaultPwd}, expectedPoolTimeoutMS);
        }
    }

    // Check that the server did not crash/close the connection with the test due to any of the
    // delays.
    assert(conn);

    jsTest.log('SUCCESS - pooled timeout test for failpoint ' + failPoint);
}

// Hang during connection establishment.
runTimeoutTest(pooledTimeoutTestCallback, kConnTestOptions);
// Hang during bind.
runTimeoutTest(pooledTimeoutTestCallback, kBindTestOptions);
// Hang during authorization search.
runTimeoutTest(pooledTimeoutTestCallback, kSearchAuthzTestOptions);
// Hang during liveness check search.
runTimeoutTest(pooledTimeoutTestCallback, kSearchLivenessCheckTestOptions);
})();
