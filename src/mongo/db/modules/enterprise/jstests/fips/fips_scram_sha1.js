/**
 * Test that FIPS mode implicitly disables SCRAM-SHA-1 authentication mechanism and that it can
 * still be explicitly enabled.
 * @tags: [
 *   # FIPS tests are TSAN incompatible, as TSAN runs into false positives on these tests -- see
 *   # BF-26624 for example.
 *   tsan_incompatible
 * ]
 */
import {isSUSE15SP1} from "jstests/libs/os_helpers.js";
import {ShardingTest} from "jstests/libs/shardingtest.js";
import {supportsFIPS} from "jstests/ssl/libs/ssl_helpers.js";

// Disable test on SLES 15 SP1 because of buggy FIPS support
// SLES 15 SP2 FIPS works
if (isSUSE15SP1()) {
    quit();
}

const SERVER_CERT = "jstests/libs/server.pem";
const CLIENT_CERT = "jstests/libs/client.pem";
const CA_FILE = "jstests/libs/ca.pem";

function shellConnect(port, mech) {
    let uri = 'mongodb://root:root@localhost:' + port + '/admin';
    if (mech !== undefined) {
        uri += '?authMechanism=' + mech;
    }
    return runMongoProgram('mongo',
                           uri,
                           '--tls',
                           '--tlsCertificateKeyFile',
                           CLIENT_CERT,
                           '--tlsCAFile',
                           CA_FILE,
                           '--eval',
                           ';');
}

function runTest(conn, opts) {
    const admin = conn.getDB('admin');
    conn.getDB('admin').createUser({user: 'root', pwd: 'root', roles: ['root']});
    assert(conn.getDB('admin').auth('root', 'root'));

    // Tri-state value.
    // true/false: SCRAM-SHA-1 explicitly (not)set in setParameter.
    // undefined: setParameter not present.
    const setparam = (function(sp) {
        if (sp.authenticationMechanisms === undefined) {
            return undefined;
        }

        return sp.authenticationMechanisms.includes('SCRAM-SHA-1');
    })(opts.setParameter || {});

    // Directly ask the server if it supports SCRAM-SHA-1.
    const sha1Enabled = (function() {
        const mechs =
            assert.commandWorked(admin.runCommand({getParameter: 1, authenticationMechanisms: 1}))
                .authenticationMechanisms;
        jsTest.log('authenticationMechanisms: ' + tojson(mechs));
        return mechs.includes('SCRAM-SHA-1');
    })();

    // If there's no explicit authMechs list, and fips mode is enabled,
    // then we should get a D1 log message about implicit disable.
    const implicitlyDisabled = (setparam === undefined) && (opts.tlsFIPSMode !== undefined);
    const kSHA1DisabledMsg = 5952800;
    assert.eq(checkLog.checkContainsOnceJson(conn, kSHA1DisabledMsg, {}), implicitlyDisabled);

    // If there is an explicit authMechs list without SHA1,
    // or SHA1 got implicitly disabled,
    // then we should not expect SHA1 support.
    assert.eq(sha1Enabled, !((setparam === false) || implicitlyDisabled));

    // Validate connecting fresh clients and using negotiation vs explicit auth methods.
    assert.eq(shellConnect(conn.port), 0);
    assert.eq(shellConnect(conn.port, 'SCRAM-SHA-1'), sha1Enabled ? 0 : 1);
    assert.eq(shellConnect(conn.port, 'SCRAM-SHA-256'), 0);
}

const baseOpts = {
    tlsMode: "requireTLS",
    tlsCertificateKeyFile: SERVER_CERT,
    tlsCAFile: CA_FILE,
};

const fipsOpts = Object.extend({
    tlsFIPSMode: '',
},
                               baseOpts);

const overrideNonFipsOpts = Object.extend({
    setParameter: {
        authenticationMechanisms: 'SCRAM-SHA-1,SCRAM-SHA-256',
    },
},
                                          baseOpts);

const overrideFipsOpts = Object.extend({
    setParameter: {
        authenticationMechanisms: 'SCRAM-SHA-1,SCRAM-SHA-256',
    },
},
                                       fipsOpts);

function testWithOpts(test) {
    // Test non-fips mode.
    test(baseOpts);

    // Test fips mode with default mechs.
    test(fipsOpts);

    // Override non-fips.
    test(overrideNonFipsOpts);

    // Test fips mode with override mechs.
    test(overrideFipsOpts);
}

// Skip test if FIPS is not supported
if (!supportsFIPS()) {
    quit();
}

testWithOpts(function(opts) {
    // We don't include auth in baseOpts because mongos enabled auth via keyFile, not auth.
    const myopts = Object.extend({auth: ''}, opts);
    jsTest.log('Standlone: ' + tojson(myopts));
    const conn = MongoRunner.runMongod(myopts);
    runTest(conn, myopts);
    MongoRunner.stopMongod(conn);
    jsTest.log('SUCCESS - standalone mongod');
});

testWithOpts(function(opts) {
    jsTest.log('Sharding: ' + tojson(opts));
    const st = new ShardingTest({
        mongos: [opts],
        config: [opts],
        rs: {nodes: [opts]},
        shards: 1,
        useHostname: false,
        other: {keyFile: 'jstests/libs/key1'},
    });
    runTest(st.s0, opts);
    st.stop();
    jsTest.log('SUCCESS - sharding');
});
