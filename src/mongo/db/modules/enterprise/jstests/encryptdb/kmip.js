// This test tests KMIP options and key rotation for encrypted storage engine
// It assumes that PyKMIP is installed
// @tags: [uses_pykmip, incompatible_with_s390x]

import {getPython3Binary} from "jstests/libs/python.js";
import {requireSSLProvider} from "jstests/ssl/libs/ssl_helpers.js";
import {
    killPyKMIPServer,
    platformSupportsGCM
} from "src/mongo/db/modules/enterprise/jstests/encryptdb/libs/helpers.js";

const testDir = "src/mongo/db/modules/enterprise/jstests/encryptdb/";

function runTest(cipherMode, extra_opts = {}) {
    let dbNameCounter = 0;
    function runEncryptedMongod(params) {
        const defaultParams = {
            enableEncryption: "",
            kmipServerName: "127.0.0.1",
            // default port is 5696, we're setting it here to test option parsing
            kmipPort: "6666",
            kmipServerCAFile: "jstests/libs/trusted-ca.pem",
            encryptionCipherMode: cipherMode,
        };

        const opts = Object.merge(defaultParams, params, extra_opts);

        if (opts.kmipClientCertificateSelector !== undefined) {
            // Certificate selector specified,
            // no need to specify a certificate file.
        } else if (/OpenSSL/.test(getBuildInfo().openssl.running)) {
            // Linux (via OpenSSL) supports encryption, use it.
            opts.kmipClientCertificateFile = testDir + "libs/trusted_client_password_protected.pem";
            opts.kmipClientCertificatePassword = "qwerty";
        } else {
            // Windows and Apple SSL providers don't support encrypted PEM files.
            opts.kmipClientCertificateFile = "jstests/libs/trusted-client.pem";
        }

        return MongoRunner.runMongod(opts);
    }

    function assertFind(md) {
        const testDB = md.getDB("test" + dbNameCounter);
        const doc = testDB.test.findOne({}, {_id: 0});

        assert.eq({"a": dbNameCounter}, doc, "Document did not have expected value");

        dbNameCounter += 1;
        const newDB = md.getDB("test" + dbNameCounter);
        assert.commandWorked(newDB.test.insert({a: dbNameCounter}));
    }

    function assertKeyId(md, keyId, extra_opts = {}) {
        // restart with no keyID should work
        md = runEncryptedMongod(Object.assign({
            restart: md,
        },
                                              extra_opts));
        assert.neq(null, md, "Wasn't able to restart mongod without a keyID");
        assertFind(md);
        MongoRunner.stopMongod(md);

        // restart explicitly with the keyID should also work
        md = runEncryptedMongod(Object.assign({
            restart: md,
            kmipKeyIdentifier: keyId,
        },
                                              extra_opts));
        assert.neq(null, md, "Wasn't able to restart mongod with the correct keyID of " + keyId);
        assertFind(md);
        MongoRunner.stopMongod(md);
    }

    clearRawMongoProgramOutput();
    let kmipServerPid = _startMongoProgram(getPython3Binary(), testDir + "kmip_server.py");
    // Assert here that PyKMIP is compatible with the default Python version
    assert(checkProgram(kmipServerPid));
    // wait for PyKMIP, a KMIP server framework, to start
    assert.soon(() => rawMongoProgramOutput(".*").search("Starting connection service") !== -1);

    // start mongod with default keyID of "1"
    let md = runEncryptedMongod();
    let testDB = md.getDB("test0");
    testDB.test.insert({a: 0});
    MongoRunner.stopMongod(md);
    assertKeyId(md, 1);

    // do a key rotation, keyID is now "2"
    runEncryptedMongod({
        restart: md,
        kmipRotateMasterKey: "",
    });
    assertKeyId(md, 2);

    // do a key rotation and explicitly specify the keyID "1"
    runEncryptedMongod({restart: md, kmipRotateMasterKey: "", kmipKeyIdentifier: "1"});
    assertKeyId(md, 1);

    jsTestLog("Test running mongod's with multiple KMIP servers provided");

    // start mongod with default keyID of "1", and multiple KMIP servers, of which 1 works
    md = runEncryptedMongod({
        kmipServerName: "10.0.0.1, 127.0.0.1, 192.168.1.1",
        restart: md,
    });
    testDB = md.getDB("test0");
    testDB.test.insert({b: 0});
    MongoRunner.stopMongod(md);
    assertKeyId(md, 1);

    // start mongod with default keyID of "1", and multiple KMIP servers, of which 0 work
    md = runEncryptedMongod({
        kmipServerName: "10.0.0.1,192.168.1.1",
        restart: md,
        waitForConnect: false,
    });

    let code = waitProgram(md.pid);
    assert.eq(code, MongoRunner.EXIT_ABRUPT, "Ran mongod with invalid KMIP server address");
    killPyKMIPServer(kmipServerPid);

    jsTestLog(
        "Test running mongod with and without useLegacyProtocol when KMIP server is legacy-only");

    clearRawMongoProgramOutput();
    dbNameCounter = 0;

    // Start a KMIP server which is locked to using protocol version 1.0.
    kmipServerPid =
        _startMongoProgram(getPython3Binary(), testDir + "kmip_server.py", "--version", "1.0");
    assert(checkProgram(kmipServerPid));
    assert.soon(() => rawMongoProgramOutput(".*").search("Starting connection service") !== -1);

    // Start mongod without useLegacyProtocol, it should fail to do anything since the KMIP session
    // assigned to it will die immediately upon trying to start up
    md = runEncryptedMongod({waitForConnect: false});
    code = waitProgram(md.pid);
    assert.eq(code, MongoRunner.EXIT_ABRUPT);

    // This time, add useLegacyProtocol -- everything should now work
    md = runEncryptedMongod({kmipUseLegacyProtocol: true});
    testDB = md.getDB("test0");
    testDB.test.insert({a: 0});
    MongoRunner.stopMongod(md);
    assertKeyId(md, 1, {kmipUseLegacyProtocol: true});

    killPyKMIPServer(kmipServerPid);
}

runTest("AES256-CBC");
if (platformSupportsGCM) {
    runTest("AES256-GCM");
}

// Windows and macOS specific tests using certificate selectors.

function makeTrustedClientSelector() {
    if (!_isWindows()) {
        return 'thumbprint=9CA511552F14D3FC2009D425873599BF77832238';
    }
    // SChannel backed follows Windows rules and only trusts Root in LocalMachine
    runProgram("certutil.exe", "-addstore", "-f", "Root", "jstests\\libs\\trusted-ca.pem");

    // Import a pfx file since it contains both a cert and private key and is easy to import
    // via command line.
    runProgram(
        "certutil.exe", "-importpfx", "-f", "-p", "qwerty", "jstests\\libs\\trusted-client.pfx");
}

requireSSLProvider(['apple', 'windows'], function() {
    const SELECTOR = makeTrustedClientSelector();
    runTest('AES256-CBC', {kmipClientCertificateSelector: SELECTOR});
    if (platformSupportsGCM) {
        runTest('AES256-GCM', {kmipClientCertificateSelector: SELECTOR});
    }
});
