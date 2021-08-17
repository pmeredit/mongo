(function() {
'use strict';

load("src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldap_timeout_lib.js");

// Const test options.
const kExpectedBindFailLog = {
    "mechanism": "PLAIN",
    "user": "ldapz_ldap1",
    "db": "$external",
    "error": {
        "code": new RegExp('96|18', 'i'),
        "codeName": new RegExp('OperationFailed|AuthenticationFailed', 'i'),
        "errmsg": new RegExp('Failed to acquire LDAP group membership|Authentication failed.', 'i'),
    },
};
const kExpectedSearchFailLog = {
    "mechanism": "PLAIN",
    "user": "ldapz_ldap2",
    "db": "$external",
    "error": {
        "code": new RegExp('96|18'),
        "codeName": new RegExp('OperationFailed|AuthenticationFailed', 'i'),
        "errmsg": new RegExp('Failed to acquire LDAP group membership|Authentication failed.', 'i'),
    },
};
const kExpectedLivenessFailLog = {
    "error": {
        "code": 96,
        "codeName": "OperationFailed",
        "errmsg": "Operation timed out",
    },
};

const kConnTestOptions = {
    failPoint: kConnectionFailPoint,
    isPooled: true,
    ldapConnectionPoolHostRefreshIntervalMillis: 60000,
    ldapTimeoutMS: kLdapTimeoutMS,
    maxPoolSize: 2,
    expectedUnderTimeoutLogId: 20250,
    expectedUnderTimeoutAttrs: {
        "mechanism": "PLAIN",
        "speculative": false,
        "principalName": "ldapz_ldap1",
        "authenticationDatabase": "$external",
    },
    expectedOverTimeoutLogId: 5286307,
    expectedOverTimeoutAttrs: kExpectedBindFailLog,
    slowDelaySecs: kSlowDelaySecs,
    slowResponses: kSlowResponses,
    totalRequests: kTotalRequests,
};

const kBindTestOptions = {
    failPoint: kBindFailPoint,
    isPooled: true,
    ldapConnectionPoolHostRefreshIntervalMillis: 60000,
    ldapTimeoutMS: 10000,
    maxPoolSize: 2,
    expectedUnderTimeoutLogId: 20250,
    expectedUnderTimeoutAttrs: {
        "mechanism": "PLAIN",
        "speculative": false,
        "principalName": "ldapz_ldap1",
        "authenticationDatabase": "$external",
    },
    expectedOverTimeoutLogId: 5286307,
    expectedOverTimeoutAttrs: kExpectedBindFailLog,
    slowDelaySecs: kSlowDelaySecs,
    slowResponses: kSlowResponses,
    totalRequests: kTotalRequests,
};

const kSearchAuthzTestOptions = {
    failPoint: kSearchFailPoint,
    isPooled: true,
    ldapConnectionPoolHostRefreshIntervalMillis: 60000,
    ldapTimeoutMS: 10000,
    maxPoolSize: 2,
    expectedUnderTimeoutLogId: 20250,
    expectedUnderTimeoutAttrs: {
        "mechanism": "PLAIN",
        "speculative": false,
        "principalName": "ldapz_ldap2",
        "authenticationDatabase": "$external",
    },
    expectedOverTimeoutLogId: 5286307,
    expectedOverTimeoutAttrs: kExpectedSearchFailLog,
    slowDelaySecs: kSlowDelaySecs,
    slowResponses: kSlowResponses,
    totalRequests: kTotalRequests,
};

const kSearchLivenessCheckTestOptions = {
    failPoint: kLivenessCheckFailPoint,
    isPooled: true,
    ldapConnectionPoolHostRefreshIntervalMillis: 3000,
    ldapTimeoutMS: 10000,
    maxPoolSize: 20,
    expectedUnderTimeoutLogId: 24062,
    expectedUnderTimeoutAttrs: {
        "refreshTimeElapsedMillis": kUnderTimeoutRegex,
    },
    expectedOverTimeoutLogId: 5885201,
    expectedOverTimeoutAttrs: kExpectedLivenessFailLog,
    slowDelaySecs: kSlowDelaySecs,
    slowResponses: 1,
    totalRequests: 1,
};

function pooledTimeoutTestCallback({conn, shardingTest, options}) {
    const failPoint = options.failPoint;
    const expectedUnderTimeoutLogId = options.expectedUnderTimeoutLogId;
    const expectedUnderTimeoutAttrs = options.expectedUnderTimeoutAttrs;
    const expectedOverTimeoutLogId = options.expectedOverTimeoutLogId;
    const expectedOverTimeoutAttrs = options.expectedOverTimeoutAttrs;
    const slowResponses = options.slowResponses;
    const slowDelaySecs = options.slowDelaySecs;
    const totalRequests = options.totalRequests;
    const maxPoolSize = options.maxPoolSize;

    jsTest.log('Starting pooled timeout test for failpoint ' + failPoint);

    // First, set the log level to D1 so that all commands run on the server get logged.
    setLogLevel(conn);

    if (failPoint === kLivenessCheckFailPoint) {
        // Launch clients to authenticate as ldapz_ldap1 to warm up the connection pool, and then
        // set the failpoint and sleep as long as needed to get to witness the failed connection
        // refreshes.
        runClients(conn, options, {userName: 'ldapz_ldap1', pwd: defaultPwd});
        setLdapFailPoint(failPoint, {'times': slowResponses}, slowDelaySecs, conn, shardingTest);
        const refreshInterval = options.ldapConnectionPoolHostRefreshIntervalMillis;
        sleep(refreshInterval);
    } else {
        // Set the failpoint first because we do not need to worry about the failpoint triggering
        // during connection pool setup.
        setLdapFailPoint(failPoint, {'times': slowResponses}, slowDelaySecs, conn, shardingTest);
        if (failPoint === kSearchFailPoint) {
            // Launch clients to authenticate as ldapz_ldap2, which actually has LDAP roles and so
            // should trigger failures when retrieving roles.
            runClients(conn, options, {userName: 'ldapz_ldap2', pwd: defaultPwd});
        } else {
            // Launch clients to authenticate as ldapz_ldap1, which is authn-only and so should
            // trigger failures during connection establishment or during bind.
            runClients(conn, options, {userName: 'ldapz_ldap1', pwd: defaultPwd});
        }
    }

    // Determine the number of under timeout and over timeout logs expected based on the failPoint
    // and the size of the connection pool. By default, the comparators are set to be slightly
    // relaxed and accept an error of up to 1 log to account for unexpected flakiness in the LDAP
    // server.
    let countUnderTimeout;
    let underComparator = (actual, expected) => {
        return Math.abs(actual - expected) <= 1;
    };
    let countOverTimeout;
    let overComparator = underComparator;

    // When the LDAP server hangs for all connections in the pool, none of the clients will
    // successfully auth within the timeout.
    if (maxPoolSize <= slowResponses) {
        countUnderTimeout = 0;
        countOverTimeout = totalRequests;
    } else {
        switch (failPoint) {
            case kConnectionFailPoint:
            case kBindFailPoint:
                // Due to the connection pool, the exact number of auth attempts that succeed will
                // vary based on how healthy connections are provided. During connection
                // establishment, the worst case is that all auth attempts that hit failpoints will
                // time out. The best case is when the pool identifies the unhealthy connections and
                // checks out ones that don't hit the fail point, resulting in more successes.
                // Therefore, we simply assert that at least totalRequests - slowResponses auth
                // attempts succeeded while at most slowResponses timed out.
                countUnderTimeout = totalRequests - slowResponses;
                underComparator = (actual, expected) => {
                    return actual >= expected;
                };
                countOverTimeout = slowResponses;
                overComparator = (actual, expected) => {
                    return actual <= expected;
                };
                break;
            case kSearchFailPoint:
                // During authorization role checks, cache misses will result in search operations
                // on the LDAP server. Each lookup request will join an already-in-progress one if
                // available, which will be the one that is hung on a fail point. Therefore, they
                // should all time out.
                countUnderTimeout = 0;
                countOverTimeout = totalRequests;
                break;
            case kLivenessCheckFailPoint:
                // The connection pool uses liveness checks to refresh its connections. The number
                // of times connections fail to refresh should be equal to 'slowResponses,' while
                // the number of times connections successfully refresh should be at least
                // 'totalRequests - slowResponses'.
                countUnderTimeout = totalRequests - slowResponses;
                underComparator = (actual, expected) => {
                    return actual >= expected;
                };
                countOverTimeout = slowResponses;
        }
    }
    // Assert that the logs reflect the expected number of successes and failures.
    checkTimeoutLogs(conn,
                     expectedUnderTimeoutLogId,
                     expectedUnderTimeoutAttrs,
                     countUnderTimeout,
                     expectedOverTimeoutLogId,
                     expectedOverTimeoutAttrs,
                     countOverTimeout,
                     underComparator,
                     overComparator);

    jsTest.log('SUCCESS - pooled timeout test for failpoint ' + failPoint);
}

// Hang during connection establishment with default max of 2 connections in pool.
runTimeoutTest(pooledTimeoutTestCallback, kConnTestOptions);
// Hang during connection establishment with max of 20 connections in pool.
kConnTestOptions.maxPoolSize = 20;
runTimeoutTest(pooledTimeoutTestCallback, kConnTestOptions);
// Hang during bind with default max of 2 connections in pool.
runTimeoutTest(pooledTimeoutTestCallback, kBindTestOptions);
// Hang during bind with max of 20 connections in pool.
kBindTestOptions.maxPoolSize = 20;
runTimeoutTest(pooledTimeoutTestCallback, kBindTestOptions);
// Hang during authorization search with default max of 2 connections in pool.
runTimeoutTest(pooledTimeoutTestCallback, kSearchAuthzTestOptions);
// Hang during authorization search with max of 20 connections in pool.
kSearchAuthzTestOptions.maxPoolSize = 20;
runTimeoutTest(pooledTimeoutTestCallback, kSearchAuthzTestOptions);
// Hang during liveness check search.
runTimeoutTest(pooledTimeoutTestCallback, kSearchLivenessCheckTestOptions);
})();
