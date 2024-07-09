/**
 * Tests that mongoauditdecrypt can successfully decrypt an
 * encrypted audit log file, which is generated for each of
 * the key store types and wrapping methods.
 * @tags: [uses_pykmip, requires_gcm, incompatible_with_s390x]
 */

import {FeatureFlagUtil} from "jstests/libs/feature_flag_util.js";
import {determineSSLProvider} from "jstests/ssl/libs/ssl_helpers.js";
import {
    AuditSpooler,
    ShardingFixture,
    StandaloneFixture
} from "src/mongo/db/modules/enterprise/jstests/audit/lib/audit.js";
import {
    AUDIT_LOCAL_KEY_ENCRYPT_KEYFILE,
    KMIPEncryptFixture,
    KMIPGetFixture,
    LocalFixture,
} from "src/mongo/db/modules/enterprise/jstests/audit/lib/audit_encryption.js";

const kMongoSchema = 'mongo';
const kOCSFSchema = 'ocsf';

// note: this port number must be unique among the audit tests that use
// the PyKMIP server, so that these tests can run in parallel without
// EADDRINUSE issues.
const kmipServerPort = 6566;

if (determineSSLProvider() !== "windows") {
    run("chmod", "600", AUDIT_LOCAL_KEY_ENCRYPT_KEYFILE);
}

/**
 * Tries parsing a JSON line, will return true if successful
 */
function isValidJSON(json) {
    try {
        JSON.parse(json);
        return true;
    } catch (e) {
        return false;
    }
}

/**
 * Returns the compressed message, will return {error: true} if fail
 */
function getCompressedMessage(json) {
    try {
        const auditLineParsed = JSON.parse(json);
        return auditLineParsed.log;
    } catch (e) {
        return {error: true};
    }
}

let bOCSFEnabled = undefined;

print("Testing audit log decryptor program");
function testAuditLogDecryptor(serverFixture, isMongos, keyManagerFixture, schema) {
    const enableCompression = true;
    keyManagerFixture.startKeyServer();
    let opts = keyManagerFixture.generateOptsWithDefaults(enableCompression);

    if (isMongos) {
        opts = {other: {mongosOptions: opts}};
    }

    const kFormat = 'JSON';
    jsTest.log(`Testing ${schema}/${kFormat}: ` + tojson(opts));
    const {conn, audit, admin} = serverFixture.startProcess(opts, kFormat, schema);

    admin.createUser({user: 'admin', pwd: 'pwd', roles: ['root']});
    assert(admin.auth('admin', 'pwd'));

    if (bOCSFEnabled === undefined) {
        // JIT initialize of bOCSFEnabled,
        // lets us know if we can run the OCSF variant next.
        bOCSFEnabled =
            FeatureFlagUtil.isEnabled(admin, "OCSF", {username: 'admin', password: 'pwd'});
        jsTest.log('featureFlagOCSF === ' + tojson(bOCSFEnabled));
    }

    // Skips first line since it's the header
    audit.setCurrentAuditLine(audit.getCurrentAuditLine() + 1);

    assert.soon(() => {
        const auditLine = audit.getNextEntryNoParsing();

        const base64Line = getCompressedMessage(auditLine);

        // Audit log was compressed and encoded as base64 so it can't be parsed
        return !isValidJSON(base64Line);
    }, "Audit log was not compressed and encoded as base64");
    serverFixture.stopProcess();

    // Prevent race condition trying to read the audit file before it is written and saved
    sleep(2000);

    const auditPath = conn.fullOptions.auditPath;
    const mode = isMongos ? 'sharded' : 'standalone';
    const fixtureName = keyManagerFixture.getName();
    const runId = `${mode}_${fixtureName}_${schema}`;
    const outputFile = MongoRunner.dataPath + "decompressedLog_" + runId + ".json";
    const decryptPid = keyManagerFixture.runDecryptor(auditPath, outputFile);
    assert.eq(waitProgram(decryptPid), 0);

    const outputAudit = new AuditSpooler(outputFile, false);
    const outputLine = outputAudit.getNextEntryNoParsing();

    // Audit log was decoded and decompressed so it was able to be parsed
    assert.eq(isValidJSON(outputLine), true);

    keyManagerFixture.stopKeyServer();
}

let keyManagerFixtures = [
    new LocalFixture(),
    new KMIPGetFixture(kmipServerPort, false /* useLegacyProtocol */),
    new KMIPGetFixture(kmipServerPort, true /* useLegacyProtocol */),
    new KMIPEncryptFixture(kmipServerPort, false /* useLegacyProtocol */)
];

for (const keyManagerFixture of keyManagerFixtures) {
    jsTest.log("Testing with key store type " + keyManagerFixture.getName());

    {
        const standaloneFixture = new StandaloneFixture();
        jsTest.log("Testing decrypt of mongo audit file from standalone");
        testAuditLogDecryptor(standaloneFixture, false, keyManagerFixture, kMongoSchema);
    }
    if (bOCSFEnabled) {
        const standaloneFixture = new StandaloneFixture();
        jsTest.log("Testing decrypt of OCSF audit file from standalone");
        testAuditLogDecryptor(standaloneFixture, false, keyManagerFixture, kOCSFSchema);
    }

    const schemas = [kMongoSchema];
    if (bOCSFEnabled) {
        schemas.push(kOCSFSchema);
    }
    schemas.forEach(function(schema) {
        const shardingFixture = new ShardingFixture();
        jsTest.log(`Testing decrypt of ${schema} audit file fromm sharded cluster`);
        testAuditLogDecryptor(shardingFixture, true, keyManagerFixture, schema);
    });
}
