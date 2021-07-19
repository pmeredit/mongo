(function() {

// Test that FIPS mode works on enterprise builds of mongod and mongos if FIPS is available on the
// OS.

// Global consts.
const SERVER_CERT = "jstests/libs/server.pem";
const CLIENT_CERT = "jstests/libs/client.pem";
const CA_FILE = "jstests/libs/ca.pem";

const sslOptions = {
    sslMode: "requireSSL",
    sslPEMKeyFile: SERVER_CERT,
    sslCAFile: CA_FILE,
};

const fipsOptions = {
    sslMode: "requireSSL",
    sslPEMKeyFile: SERVER_CERT,
    sslCAFile: CA_FILE,
    sslFIPSMode: "",
};

// Checks that servers that fail to start only do so if FIPS is not enabled on the operating system.
function validateFailure() {
    const mongoOutput = rawMongoProgramOutput();
    jsTest.log('Server failed to start, checking for FIPS support');
    assert(mongoOutput.match(/this version of mongodb was not compiled with FIPS support/) ||
           mongoOutput.match(/FIPS modes is not enabled on the operating system/) ||
           mongoOutput.match(/FIPS_mode_set:fips mode not supported/));
    clearRawMongoProgramOutput();
}

// Check that servers are still running and can be authenticated to.
function validateSuccess(conn) {
    assert(conn);
    // verify that auth works, SERVER-18051
    conn.getDB("admin").createUser({user: "root", pwd: "root", roles: ["root"]});
    assert(conn.getDB("admin").auth("root", "root"), "auth failed");
}

// Launches a standalone mongod with sslFIPSMode set and checks that it launches normally if FIPS
// mode is available on the operating system.
function runMongodTest(fipsOptions) {
    jsTest.log('Starting test for standalone mongod');
    try {
        const conn = MongoRunner.runMongod(fipsOptions);
        validateSuccess(conn);
        MongoRunner.stopMongod(conn);
    } catch (e) {
        validateFailure();
    }
    jsTest.log('SUCCESS - standalone mongod');
}

// Launches a sharded cluster with sslFIPSMode set on the mongos, config replica set, and shard
// replica set and checks that it launches normally if FIPS mode is available on the operating
// system.
function runShardedTest(fipsOptions) {
    jsTest.log('Starting test for sharded cluster');
    const options = {
        mongos: [fipsOptions],
        config: [fipsOptions],
        rs: {nodes: [fipsOptions]},
        shards: 1,
        useHostname: false,
    };
    try {
        const st = new ShardingTest(options);
        validateSuccess(st.s0);
        st.stop();
    } catch (e) {
        validateFailure();
    }
    jsTest.log('SUCCESS - sharded cluster');
}

// Launches a mongo shell with sslFIPSMode and checks that it is able to successfully connect to a
// running mongod if FIPS mode is available on the operating system.
function runShellTest(sslOptions) {
    jsTest.log('Starting test for mongo shell');
    const md = MongoRunner.runMongod(sslOptions);
    conn = runMongoProgram('mongo',
                           '--port',
                           md.port,
                           '--ssl',
                           '--sslFIPSMode',
                           '--sslPEMKeyFile',
                           CLIENT_CERT,
                           '--sslCAFile',
                           CA_FILE,
                           '--eval',
                           ';');
    if (conn !== 0) {
        validateFailure();
    }
    MongoRunner.stopMongod(md);
    jsTest.log('SUCCESS - mongo shell');
}

runMongodTest(fipsOptions);
runShardedTest(fipsOptions);
runShellTest(sslOptions);
})();
