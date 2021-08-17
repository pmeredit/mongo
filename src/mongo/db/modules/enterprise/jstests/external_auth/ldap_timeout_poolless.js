(function() {
'use strict';

load("src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldap_timeout_lib.js");

// Const test options.
const kExpectedCommandAttrs = {
    "saslStart": 1,
    "mechanism": "PLAIN",
    "options": {"skipEmptyExchange": true},
    "payload": "xxx",
    "$db": "$external",
};

const kTestOptions = {
    failPoint: kConnectionFailPoint,
    isPooled: false,
    ldapTimeoutMS: kLdapTimeoutMS,
    expectedUnderTimeoutLogId: 51803,
    expectedUnderTimeoutAttrs: {
        "ns": "$external.$cmd",
        "command": kExpectedCommandAttrs,
        "durationMillis": kUnderTimeoutRegex,
    },
    expectedOverTimeoutLogId: 51803,
    expectedOverTimeoutAttrs: {
        "ns": "$external.$cmd",
        "command": kExpectedCommandAttrs,
        "durationMillis": kOverTimeoutRegex,

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
    const expectedUnderTimeoutAttrs = options.expectedUnderTimeoutAttrs;
    const expectedOverTimeoutLogId = options.expectedOverTimeoutLogId;
    const expectedOverTimeoutAttrs = options.expectedOverTimeoutAttrs;
    const slowDelaySecs = options.slowDelaySecs;
    const slowResponses = options.slowResponses;
    const totalRequests = options.totalRequests;

    jsTest.log('Starting poolless timeout test for failpoint ' + failPoint);

    // First, set the log level to D1 so that all commands run on the server get logged.
    setLogLevel(conn);

    // Set the fail point and run the clients.
    setLdapFailPoint(failPoint, {'times': slowResponses}, slowDelaySecs, conn, shardingTest);
    runClients(conn, options, {userName: 'ldapz_ldap1', pwd: defaultPwd});

    // If the delay occurred when searching for the LDAP user's group membership, it is expected
    // that all of the auth attempts took at least the delay to complete. This must occur since all
    // of the LDAP search operations would have used the same backing lookup call after the cache
    // miss. Since connection pooling is turned off, behavior is deterministic for hangs during bind
    // and connection establishment. Each auth attempt results in a fresh connection to the LDAP
    // server. The first five hang and succeed after the hang (60 seconds), while the second five
    // succeed at normal speeds (less than 60 seconds).
    // Sometimes, an LDAP operation may fail due to genuine network issues or flakiness in the LDAP
    // server rather than the actual failpoints. To mitigate those impacts, the test will accept
    // up to 1 log more or less than expected.
    let countUnderTimeout = totalRequests - slowResponses;
    let countOverTimeout = slowResponses;
    const relaxedComparator = (actual, expected) => {
        return Math.abs(actual - expected) <= 1;
    };

    if (failPoint === 'ldapSearchTimeoutHang') {
        countUnderTimeout = 0;
        countOverTimeout = totalRequests;
    }

    checkTimeoutLogs(conn,
                     expectedUnderTimeoutLogId,
                     expectedUnderTimeoutAttrs,
                     countUnderTimeout,
                     expectedOverTimeoutLogId,
                     expectedOverTimeoutAttrs,
                     countOverTimeout,
                     relaxedComparator,
                     relaxedComparator);

    jsTest.log('SUCCESS - poolless timeout test for failpoint ' + failPoint);
}

// These tests simulate an environment where the server is contacting a slow LDAP server without
// using a connection pool, meaning that it is entirely reliant on the system LDAP library to
// enforce timeouts. In the event that the LDAP library doesn't properly enforce the timeouts, the
// server should be able to process requests normally (albeit slowly).
runTimeoutTest(poollessTimeoutTestCallback, kTestOptions);
kTestOptions.failPoint = kBindFailPoint;
runTimeoutTest(poollessTimeoutTestCallback, kTestOptions);
kTestOptions.failPoint = kSearchFailPoint;
runTimeoutTest(poollessTimeoutTestCallback, kTestOptions);
})();
