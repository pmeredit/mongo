// Excluded from AL2 Atlas Enterprise build variant since it depends on libldap_r, which
// is not installed on that variant.
// @tags: [incompatible_with_atlas_environment]

import {
    defaultPwd
} from "src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldap_authz_lib.js";
import {
    checkTimeoutLogs,
    kBindFailPoint,
    kConnectionFailPoint,
    kDisableNativeLDAPTimeoutFailPoint,
    kLdapTimeoutErrorDeltaMS,
    kLdapTimeoutMS,
    kOverTimeoutRegex,
    kSearchFailPoint,
    kSlowDelaySecs,
    kSlowResponses,
    kTotalRequests,
    kUnderTimeoutRegex,
    runClients,
    runTimeoutTest,
    setLdapFailPoint,
    setLogLevel
} from "src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldap_timeout_lib.js";

// Const test options.
const kExpectedConnBindCommandAttrs = {
    "saslStart": 1,
    "mechanism": "PLAIN",
    "options": {"skipEmptyExchange": true},
    "payload": "xxx",
    "$db": "$external",
};

const kExpectedSearchCommandAttrs = {
    "usersInfo": "ldapz_ldap1",
    "showPrivileges": true,
};

// No connections are expected to time out when kConnectionFailPoint or kBindFailPoint are set.
// If the failpoint is hit, auth will succeed after the delay elapses, otherwise it will succeed
// normally without hitting the delay.
const kConnBindTestOptions = {
    failPoint: kConnectionFailPoint,
    isPooled: false,
    ldapTimeoutMS: kLdapTimeoutMS,
    ldapRetryCount: 0,
    expectedUnderTimeoutLogId: 51803,
    expectedUnderTimeoutAttrs: {
        "ns": "$external.$cmd",
        "command": kExpectedConnBindCommandAttrs,
        "durationMillis": kUnderTimeoutRegex,
    },
    expectedOverTimeoutLogId: 51803,
    expectedOverTimeoutAttrs: {
        "ns": "$external.$cmd",
        "command": kExpectedConnBindCommandAttrs,
        "durationMillis": kOverTimeoutRegex,

    },
    slowDelaySecs: kSlowDelaySecs,
    slowResponses: kSlowResponses,
    totalRequests: kTotalRequests,
};

// Since usersInfo is bound by maxTimeMS and only one usersInfo can be processed at a time, all
// usersInfo invocations should result in a timeout with an error message.
const kSearchTestOptions = {
    failPoint: kSearchFailPoint,
    isPooled: false,
    ldapTimeoutMS: kLdapTimeoutMS,
    ldapRetryCount: 0,
    expectedUnderTimeoutLogId: 51803,
    expectedUnderTimeoutAttrs: {
        "errMsg": "Command usersInfo requires authentication",
    },
    expectedOverTimeoutLogId: 51803,
    expectedOverTimeoutAttrs: {
        "command": kExpectedSearchCommandAttrs,
        "errMsg": new RegExp('.*', 'i'),
    },
    slowDelaySecs: kSlowDelaySecs,
    slowResponses: kSlowResponses,
    totalRequests: kTotalRequests,
};

// poollessTimeoutTestCallback launches as many shells as specified by totalRequests in parallel.
// It also sets a specified failpoint that will stall as many requests as specified by slowRequests
// for slowDelaySecs secs each. It checks the logs to verify that all of the requests were
// successful and took roughly the expected amount of time to complete.
function poollessTimeoutTestCallback({conn, shardingTest, options}) {
    const failPoint = options.failPoint;
    const expectedUnderTimeoutLogId = options.expectedUnderTimeoutLogId;
    let expectedUnderTimeoutAttrs = options.expectedUnderTimeoutAttrs;
    const expectedOverTimeoutLogId = options.expectedOverTimeoutLogId;
    let expectedOverTimeoutAttrs = options.expectedOverTimeoutAttrs;
    const slowDelaySecs = options.slowDelaySecs;
    const slowResponses = options.slowResponses;
    const totalRequests = options.totalRequests;

    // All clients are expected to get a response from the server either after the full delay or
    // after a timeout is enforced.
    // First, kDisableNativeLDAPTimeoutFailPoint is set so that the system LDAP library timeout is
    // set to some arbitrarily large value. Then, the following behavior occurs depending on the
    // failpoint set:
    //  - kConnectionFailPoint: runClients() launches shells that try to auth as ldapz_ldap1. The
    //    first 'slowResponses' clients will be delayed by slowDelaySecs to get a connection to the
    //    LDAP server, which will cause auth to take at least slowDelaySecs to complete. The
    //    remaining clients will auth in less than slowDelaySecs.
    //  - kBindFailPoint: runClients() launches shells that try to auth as ldapz_ldap1. The first
    //    'slowResponses' clients will be delayed by slowDelaySecs when binding to the LDAP server
    //    which will cause auth to take at least slowDelaySecs to complete. The remaining clients
    //    will auth in less than slowDelaySecs.
    //  - kSearchFailPoint: runClients() launches shells that execute usersInfo for ldapz_ldap1. The
    //    first 'slowResponses' clients will be delayed by slowDelaySecs when searching for LDAP
    //    group membership on the LDAP server for ldapz_ldap1. Since the server can only process one
    //    usersInfo command at a time, the delays will propagate and multiply for the last clients
    //    served. However, usersInfo has a built-in maxTimeMS timeout which should be less than
    //    slowDelaySecs, so all usersInfo invocations should either fail due to timeout or due to
    //    admin auth failure, resulting in an Unauthorized failure.
    let expectedPoollessTimeoutMS = (options.slowDelaySecs * 1000) + kLdapTimeoutErrorDeltaMS;
    if (failPoint === kSearchFailPoint) {
        expectedPoollessTimeoutMS =
            ((options.slowDelaySecs * 1000) + kLdapTimeoutErrorDeltaMS) * slowResponses;
    }

    jsTest.log('Starting poolless timeout test for failpoint ' + failPoint);

    // First, set the log level to D1 so that all commands run on the server get logged.
    setLogLevel(conn);

    // Set the fail point and run the clients.
    setLdapFailPoint(kDisableNativeLDAPTimeoutFailPoint, 'alwaysOn', 3600, conn, shardingTest);
    setLdapFailPoint(failPoint, {'times': slowResponses}, slowDelaySecs, conn, shardingTest);
    runClients(
        conn, options, {userName: 'ldapz_ldap1', pwd: defaultPwd}, expectedPoollessTimeoutMS);

    let countUnderTimeout = totalRequests - slowResponses;
    let countOverTimeout = slowResponses;
    const relaxedComparator = (actual, expected) => { return Math.abs(actual - expected) <= 1; };
    let expectedUnderComparator = relaxedComparator;
    let expectedOverComparator = relaxedComparator;

    if (failPoint === kSearchFailPoint) {
        countUnderTimeout = 9;
        countOverTimeout = 1;
        expectedUnderComparator = (actual, expected) => { return actual <= expected; };
        expectedOverComparator = (actual, expected) => { return actual >= expected; };
    }

    checkTimeoutLogs(conn,
                     expectedUnderTimeoutLogId,
                     expectedUnderTimeoutAttrs,
                     countUnderTimeout,
                     expectedOverTimeoutLogId,
                     expectedOverTimeoutAttrs,
                     countOverTimeout,
                     expectedUnderComparator,
                     expectedOverComparator);

    jsTest.log('SUCCESS - poolless timeout test for failpoint ' + failPoint);
}

// These tests simulate an environment where the server is contacting a slow LDAP server without
// using a connection pool, meaning that it is entirely reliant on the system LDAP library to
// enforce timeouts. In the event that the LDAP library doesn't properly enforce the timeouts, the
// server should be able to process requests normally (albeit slowly).
runTimeoutTest(poollessTimeoutTestCallback, kConnBindTestOptions);
kConnBindTestOptions.failPoint = kBindFailPoint;
runTimeoutTest(poollessTimeoutTestCallback, kConnBindTestOptions);
runTimeoutTest(poollessTimeoutTestCallback, kSearchTestOptions);
