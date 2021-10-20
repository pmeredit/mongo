// Library functions used by ldap timeout tests.

load("jstests/libs/parallel_shell_helpers.js");
load("src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldap_authz_lib.js");

// Const options.
const kSlowResponses = 5;
const kTotalRequests = 10;
const kSlowDelaySecs = 30;
const kLdapTimeoutMS = 10000;
const kUnderTimeoutRegex = new RegExp('^[0-9]{1,4}$|^[1-2][0-9]{4}$', 'i');
const kOverTimeoutRegex = new RegExp('^[3-9][0-9]{4}$|^[1-9][0-9]{5,}$', 'i');
const kDisableNativeLDAPTimeoutFailPoint = 'disableNativeLDAPTimeout';
const kConnectionFailPoint = 'ldapConnectionTimeoutHang';
const kBindFailPoint = 'ldapBindTimeoutHang';
const kSearchFailPoint = 'ldapSearchTimeoutHang';
const kLivenessCheckFailPoint = 'ldapLivenessCheckTimeoutHang';

function setLdapFailPoint(fp, mode, delay, db, shardingTest) {
    if (shardingTest && (fp === kSearchFailPoint || fp === kDisableNativeLDAPTimeoutFailPoint)) {
        shardingTest.configRS.nodes.forEach((node) => {
            assert.commandWorked(node.adminCommand({
                configureFailPoint: fp,
                mode: mode,
                data: {
                    delay: delay,
                },
            }));
        });
    } else {
        assert.commandWorked(db.adminCommand({
            configureFailPoint: fp,
            mode: mode,
            data: {
                delay: delay,
            },
        }));
    }
}

function setLogLevel(conn) {
    // Timeout tests require that all queries be logged, hence we need at least D1 logging.
    const adminDB = conn.getDB('admin');
    assert(adminDB.auth('siteRootAdmin', 'secret'));
    assert.commandWorked(adminDB.setLogLevel(1));
    adminDB.logout();
}

function runClients(conn, options, user) {
    const totalRequests = options.totalRequests;

    const awaitLdapConnectionHangs = [];
    for (let i = 0; i < totalRequests; i++) {
        awaitLdapConnectionHangs.push(
            startParallelShell(funWithArgs(clientCallback, user), conn.port));
    }

    // Wait for all of the clients to complete.
    awaitLdapConnectionHangs.forEach((awaitHang) => awaitHang());

    // Checking for logs requires admin auth.
    assert(conn.getDB('admin').auth('siteRootAdmin', 'secret'));
}

function runTimeoutTest(timeoutCallback, timeoutCallbackOptions) {
    // First, set up the LDAP config so that the replica set recognizes user ldapz_ldap1 for
    // authentication and authorization.
    let configGenerator = new LDAPTestConfigGenerator();
    configGenerator.ldapAuthzQueryTemplate = "{USER}?memberOf";
    configGenerator.ldapUserToDNMapping = [
        {match: "(ldapz_ldap1)", substitution: "cn={0}," + defaultUserDNSuffix},
        {match: "(ldapz_ldap2)", substitution: "cn={0}," + defaultUserDNSuffix},
    ];
    configGenerator.ldapTimeoutMS = timeoutCallbackOptions.ldapTimeoutMS;
    configGenerator.ldapUseConnectionPool = timeoutCallbackOptions.isPooled;
    if (configGenerator.ldapUseConnectionPool) {
        configGenerator.ldapConnectionPoolMaximumConnectionsPerHost =
            timeoutCallbackOptions.maxPoolSize;
        configGenerator.ldapConnectionPoolHostRefreshIntervalMillis =
            timeoutCallbackOptions.ldapConnectionPoolHostRefreshIntervalMillis;
    }
    configGenerator.authorizationManagerCacheSize = 0;
    configGenerator.ldapValidateLDAPServerConfig = false;

    runTests(timeoutCallback, configGenerator, timeoutCallbackOptions);
}

// Authenticate as a user. This will trigger a bind operation to complete LDAP proxy auth and
// possibly a search operation to get LDAP roles if LDAP authz is enabled for the user.
const clientCallback = function({userName, pwd}) {
    const externalDB = db.getMongo().getDB('$external');
    return externalDB.auth({
        user: userName,
        pwd: pwd,
        mechanism: 'PLAIN',
        digestPassword: false,
    });
};

// Checks that there are 'numUnder' logs representing operations that took less than the timeout and
// 'numUnder' logs representing operations that took longer than the timeout.
function checkTimeoutLogs(conn,
                          expectedUnderTimeoutLogId,
                          expectedUnderTimeoutAttrs,
                          numUnder,
                          expectedOverTimeoutLogId,
                          expectedOverTimeoutAttrs,
                          numOver,
                          comparatorUnder,
                          comparatorOver) {
    checkLog.containsRelaxedJson(conn,
                                 expectedUnderTimeoutLogId,
                                 expectedUnderTimeoutAttrs,
                                 numUnder,
                                 300000,
                                 comparatorUnder);
    checkLog.containsRelaxedJson(
        conn, expectedOverTimeoutLogId, expectedOverTimeoutAttrs, numOver, 300000, comparatorOver);
}
