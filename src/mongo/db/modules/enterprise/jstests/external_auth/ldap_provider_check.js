// Excluded from AL2 Atlas Enterprise build variant since it depends on libldap_r, which
// is not installed on that variant.
// @tags: [incompatible_with_atlas_environment]

import {findMatchingLogLines} from "jstests/libs/log.js";
import {
    LDAPTestConfigGenerator,
    setupTest
} from "src/mongo/db/modules/enterprise/jstests/external_auth/lib/ldap_authz_lib.js";

function getLogLine(globalLog, id) {
    const fieldMatcher = {id: id};
    const lines = [...findMatchingLogLines(globalLog.log, fieldMatcher)];
    assert(lines.length <= 1);
    return lines.length == 0 ? null : JSON.parse(lines[0]);
}

function getOSRelease() {
    try {
        const RE_ID = /^ID="?([^"\n]+)"?$/m;
        const RE_VERSION_ID = /^VERSION_ID="?([^"\n]+)"?$/m;

        const osRelease = cat("/etc/os-release");
        const id = osRelease.match(RE_ID)[1];
        const version_id = Number(osRelease.match(RE_VERSION_ID)[1]);
        return {id: id, ver: version_id};
    } catch (e) {
        return {id: "unknown"};
    }
}

function runTest(libldap, callback) {
    opts.env.LD_PRELOAD = libldap;
    conn = MongoRunner.runMongod(opts);
    assert.neq(null, conn);
    setupTest(conn);
    var adminDB = conn.getDB("admin");
    adminDB.auth("siteRootAdmin", "secret");
    globalLog = assert.commandWorked(adminDB.adminCommand({getLog: 'global'}));
    const ldapAPIInfo = getLogLine(globalLog, 24051);
    print('LDAP API Info: ' + JSON.stringify(ldapAPIInfo));

    // There is a somewhat non-intuitive decision tree involved in choosing
    // which warnings will be shown. See SERVER-56617 for more context
    const w24052 = getLogLine(globalLog, 24052);
    const w5661701 = getLogLine(globalLog, 5661701);
    const w5661703 = getLogLine(globalLog, 5661703);
    const w7818800 = getLogLine(globalLog, 7818800);
    callback(ldapAPIInfo, w24052, w5661701, w5661703, w7818800);
    MongoRunner.stopMongod(conn);
}

const osRelease = getOSRelease();
jsTest.log(osRelease);
if (["ubuntu", "rhel", "amzn"].indexOf(osRelease.id) < 0) {
    jsTest.log("this test only covers Ubuntu, RedHat, or Amazon linux flavors");
    quit();
}

let conn, globalLog;
const configGenerator = new LDAPTestConfigGenerator();
configGenerator.startMockupServer();
const opts = configGenerator.generateMongodConfig();

runTest("libldap_r.so", (ldapAPIInfo, w24052, w5661701, w5661703, w7818800) => {
    assert(ldapAPIInfo);
    assert.eq(null, w7818800);
    if (osRelease.id == "ubuntu") {
        assert.eq("GnuTLS", ldapAPIInfo.attr.options.tlsPackage);
    } else if (osRelease.id == "rhel" || osRelease.id == "amzn") {
        assert(ldapAPIInfo.attr.options.tlsPackage == "OpenSSL" ||
               ldapAPIInfo.attr.options.tlsPackage == "MozNSS");
    }
    assert.neq(-1, ldapAPIInfo.attr.extensions.indexOf("THREAD_SAFE"));
    assert.eq(null, w24052);
    if (ldapAPIInfo.attr.options.tlsPackage == "OpenSSL" && ldapAPIInfo.attr.options.slowLocking) {
        assert.neq(null, w5661701, "must warn that performance would be affected by openssl<1.1.1");
    } else {
        assert.eq(null, w5661701);
    }

    assert.eq(null, w5661703);
});

runTest("libldap.so", (ldapAPIInfo, w24052, w5661701, w5661703, w7818800) => {
    assert(ldapAPIInfo);
    assert.eq(null, w7818800);
    if (osRelease.id === "ubuntu") {
        assert.eq("GnuTLS", ldapAPIInfo.attr.options.tlsPackage);
        // On Ubuntu libldap.so is thread safe
        assert.neq(-1, ldapAPIInfo.attr.extensions.indexOf("THREAD_SAFE"));
        assert.eq(null, w24052);
        assert.eq(null, w5661701);
        assert.eq(null, w5661703);
    } else if ((osRelease.id === "rhel" && osRelease.ver >= 9) ||
               (osRelease.id === "amzn" && (osRelease.ver === 2022 || osRelease.ver === 2023))) {
        assert.eq("OpenSSL", ldapAPIInfo.attr.options.tlsPackage);
        // On RHEL9 and Amazon 2023 libldap.so is thread safe
        assert.neq(-1, ldapAPIInfo.attr.extensions.indexOf("THREAD_SAFE"));
        assert.eq(null, w24052);
        assert.eq(null, w5661701);
        assert.eq(null, w5661703);
    } else if (osRelease.id == "rhel" || osRelease.id == "amzn") {
        // On RHEL7-8, Amazon Linux, and Amazon Linux 2, libldap.so is not thread safe.
        // Additionally, they may either use OpenSSL with or without TLS_MOZNSS_COMPATIBILITY or
        // MozNSS as the TLS provider.
        assert(ldapAPIInfo.attr.options.tlsPackage == "OpenSSL" ||
               ldapAPIInfo.attr.options.tlsPackage == "MozNSS");
        assert.eq(-1, ldapAPIInfo.attr.extensions.indexOf("THREAD_SAFE"));
        if (ldapAPIInfo.attr.options.tlsPackage == "OpenSSL") {
            assert.eq(null, w24052);
            assert.eq(null, w5661701);

            if (ldapAPIInfo.attr.options.mozNSSCompat) {
                assert.neq(null, w5661703, "must warn that only libldap_r is safe with NSS shim");
            } else {
                assert.eq(null, w5661703);
            }
        } else if (ldapAPIInfo.attr.options.tlsPackage == "MozNSS") {
            assert.neq(null, w24052, "must warn that performance would be degraded");
            assert.eq(null, w5661701);
            assert.eq(null, w5661703);
        } else {
            assert(false, `Provider ${ldapAPIInfo.attr.options.tlsPackage} impossible`);
        }
    }
});

// When ldapForceMultithreadMode is true, the LDAP API info should be logged, but all OS-specific
// warnings should be omitted since connection pooling/multithreading is being force-enabled.
function ldapForceMultithreadModeCb(ldapAPIInfo, w24052, w5661701, w5661703, w7818800) {
    assert(ldapAPIInfo);
    assert.neq(null, w7818800, "must warn that ldapForceMultithreadMode is being used");

    assert.eq(null, w24052);
    assert.eq(null, w5661701);
    assert.eq(null, w5661703);
}

opts.setParameter.ldapForceMultiThreadMode = true;

runTest("libldap_r.so", ldapForceMultithreadModeCb);
runTest("libldap.so", ldapForceMultithreadModeCb);

configGenerator.stopMockupServer();
