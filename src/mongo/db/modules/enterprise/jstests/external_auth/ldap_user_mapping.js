// Test userToDNMapping via the LDAP proxy
import {getPython3Binary} from "jstests/libs/python.js";
import {
    adminUserDN,
    authAndVerify,
    defaultPwd,
    defaultUserDNSuffix,
    LDAPTestConfigGenerator,
    runTests
} from "src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldap_authz_lib.js";

if (_isWindows()) {
    jsTest.log('Skipping test on WinLDAP');
    quit();
}

const kAuthenticationFailed = 5286307;

const ENTERPRISE = 'src/mongo/db/modules/enterprise';

const proxy = (function() {
    const script = ENTERPRISE + '/jstests/external_auth/lib/ldapproxy.py';
    const port = allocatePort();
    const pid = startMongoProgramNoConnect(getPython3Binary(),
                                           script,
                                           '--port',
                                           port,
                                           '--targetHost',
                                           'ldaptest.10gen.cc',
                                           '--targetPort',
                                           '389',
                                           '--delay',
                                           '0');
    assert(pid);

    // Wait for the proxy to start accepting connections.
    assert.soon(function() {
        return 0 ===
            runNonMongoProgram(getPython3Binary(),
                               script,
                               '--testClient',
                               '--targetHost',
                               '127.0.0.1',
                               '--targetPort',
                               port);
    });

    return {server: `127.0.0.1:${port}`, pid: pid};
})();

const authOptions = {
    user: adminUserDN,
    pwd: defaultPwd,
    mechanism: "PLAIN",
    digestPassword: false
};
const configGenerator = new LDAPTestConfigGenerator();
configGenerator.startMockupServer();
configGenerator.ldapServers = [proxy.server];
configGenerator.ldapAuthzQueryTemplate = "{USER}?memberOf?";

function createMapping(testCase = null) {
    const cases = [
        // Success based soft failure.
        "Does not exist",
        // Requested test case
        testCase,
        // Successful match to ensure if testCase isn't a hard failure, the mapping will succeed.
        "\\u25A0 \\u25A0",
    ];
    return cases.filter((desc) => (desc !== null)).map(function(desc) {
        return {match: '.*', ldapQuery: `${defaultUserDNSuffix}??one?(description=${desc})`};
    });
}

function assertTestsSucceed() {
    jsTest.log('Starting test expecting success: ' + tojson(configGenerator.ldapUserToDNMapping));
    configGenerator.useLogFiles = false;
    runTests(authAndVerify, configGenerator, {authOptions: authOptions, user: adminUserDN});
}

function assertTestsFail() {
    // For the test which fails, the mongod is no longer running,
    // and even if it were we can't auth to request the log properly.
    // Capture the name of the log file and parse it the hard way.
    let logFile;
    function captureLogFile(ctx) {
        logFile = ctx.mongodConfig.logFile;
        authAndVerify(ctx);
    }

    jsTest.log('Starting test expecting failure: ' + tojson(configGenerator.ldapUserToDNMapping));
    configGenerator.useLogFiles = true;
    try {
        assert.throws(() => runTests(captureLogFile,
                                     configGenerator,
                                     {authOptions: authOptions, user: adminUserDN}));
    } finally {
        print(cat(logFile));
    }

    jsTest.log('Checking for specific log message in logfile');
    const errors = cat(logFile)
                       .split("\n")
                       .filter((line) => line !== '')
                       .map((line) => JSON.parse(line))
                       .filter((line) => line.id === kAuthenticationFailed);
    printjson(errors);
    // Expect to find at least one instance of an error.
    assert.gt(errors.length, 0);
    errors.forEach(function(e) {
        // Error may appear more than once, but all results should be the same.
        assert.eq(e.msg, 'Failed to authenticate');

        const result = e.attr.error;
        assert(result.includes('aborting transformation'));
        assert(!result.includes('\u25A0 \u25A0'));
    });
}

// Control case. Everything is as it should be.
configGenerator.ldapUserToDNMapping = createMapping();
assertTestsSucceed();

// Tests queries for which the proxy will generate error codes.
// These codes should result in "soft" errors which translate to success.
const successCodes = [
    0x10,  // LDAP_NO_SUCH_ATTRIBUTE
    0x20,  // LDAP_NO_SUCH_OBJECT
];
successCodes.forEach(function(code) {
    configGenerator.ldapUserToDNMapping = createMapping('FailureCode:' + code);
    assertTestsSucceed();
});

// Check for hard failure when requsting other result codes.
// These codes will result in an assertion being thrown,
const failureCodes = [
    0x02,  // LDAP_PROTOCOL_ERROR
    0x32,  // LDAP_INSUFFICIENT_ACCESS
    0x34,  // LDAP_UNAVAILABLE
];
failureCodes.forEach(function(code) {
    configGenerator.ldapUserToDNMapping = createMapping('FailureCode:' + code);
    assertTestsFail();
});

// Same tests with OperationFailured errors made non-fatal.
configGenerator.ldapAbortOnNameMappingFailure = false;
failureCodes.forEach(function(code) {
    configGenerator.ldapUserToDNMapping = createMapping('FailureCode:' + code);
    assertTestsSucceed();
});

stopMongoProgramByPid(proxy.pid);

configGenerator.stopMockupServer();
