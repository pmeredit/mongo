/**
 * Test that FIPS mode works on enterprise builds of mongod and mongos if FIPS is available on the
 * OS.
 * @tags: [
 *   # FIPS tests are TSAN incompatible, as TSAN runs into false positives on these tests -- see
 *   # BF-26624 for example.
 *   tsan_incompatible
 * ]
 */
import {isSUSE15SP1} from "jstests/libs/os_helpers.js";
import {ShardingTest} from "jstests/libs/shardingtest.js";
import {isMacOS, isOpenSSL3orGreater, supportsFIPS} from "jstests/ssl/libs/ssl_helpers.js";

// Disable test on SLES 15 SP1 because of buggy FIPS support
// SLES 15 SP2 FIPS works
if (isSUSE15SP1()) {
    quit();
}

// Global consts.
const SERVER_CERT = "jstests/libs/server.pem";
const CA_FILE = "jstests/libs/ca.pem";

const fipsOptions = {
    sslMode: "requireSSL",
    sslPEMKeyFile: SERVER_CERT,
    sslCAFile: CA_FILE,
    sslFIPSMode: "",
};

let expectSupportsFIPS = supportsFIPS() || isMacOS();

// Examine the failure logs of mongod/s startup and validate that we observe the expected error
// messages logged due to FIPS mode enabled but FIPS being unsupported.
function validateFailure() {
    const mongoOutput = rawMongoProgramOutput(".*");

    let regexTest =
        /this version of mongodb was not compiled with FIPS support|FIPS_mode_set:fips mode not supported/;

    if (_isWindows()) {
        regexTest = /FIPS modes is not enabled on the operating system/;
    }

    if (isOpenSSL3orGreater()) {
        regexTest = /Failed to load OpenSSL 3 FIPS provider/;
    }

    let res = regexTest.test(mongoOutput);
    clearRawMongoProgramOutput();

    return res;
}

// Examine the startup logs of mongod/s startup and validate that we observe the expected message
// logged when FIPS mode is enabled and FIPS is supported
function validateSuccess(conn) {
    let regexTest = /FIPS 140-2 mode activated/;

    if (isMacOS()) {
        regexTest = / due to FIPS mode/;
    }

    if (isOpenSSL3orGreater()) {
        regexTest = /FIPS 140 mode activated/;
    }

    return checkLog.checkContainsOnce(conn, regexTest);
}

// Launches a standalone mongod with FIPS mode enabled and checks that it logs as FIPS messages
// as expected
function runMongodTest(fipsOptions) {
    jsTest.log('Starting test for standalone mongod');

    let isFipsEnabledMessage = false;
    let isFipsUnsupportedMessage = false;

    try {
        const conn = MongoRunner.runMongod(fipsOptions);
        isFipsEnabledMessage = validateSuccess(conn);
        MongoRunner.stopMongod(conn);
    } catch (e) {
        jsTest.log("Exception thrown starting mongod, validating failure log: " +
                   JSON.stringify(e));
        isFipsUnsupportedMessage = validateFailure();
    }

    assert((isFipsEnabledMessage && expectSupportsFIPS) ||
           (isFipsUnsupportedMessage && !expectSupportsFIPS));
}

// Launches a sharded cluster with FIPS mode enabled on the mongos, config replica set, and shard
// replica set and checks that mongos logs FIPS messages as expected
function runShardedTest(fipsOptions) {
    jsTest.log('Starting test for sharded cluster');
    const options = {
        mongos: [fipsOptions],
        config: [fipsOptions],
        rs: {nodes: [fipsOptions]},
        shards: 1,
        useHostname: false,
    };

    let isFipsEnabledMessage = false;
    let isFipsUnsupportedMessage = false;

    try {
        const st = new ShardingTest(options);
        const mongos = st.s0;
        isFipsEnabledMessage = validateSuccess(mongos);
        st.stop();
    } catch (e) {
        jsTest.log("Exception thrown starting sharded cluster, validating failure log: " +
                   JSON.stringify(e));
        isFipsUnsupportedMessage = validateFailure();
    }

    assert((isFipsEnabledMessage && expectSupportsFIPS) ||
           (isFipsUnsupportedMessage && !expectSupportsFIPS));
}

runMongodTest(fipsOptions);
runShardedTest(fipsOptions);
