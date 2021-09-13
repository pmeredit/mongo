// This test tests KMIP options and key rotation for encrypted storage engine
// It assumes that PyKMIP is installed

(function() {
"use strict";

load('jstests/ssl/libs/ssl_helpers.js');

const testDir = "src/mongo/db/modules/enterprise/jstests/encryptdb/";
load(testDir + "libs/helpers.js");

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

    function assertKeyId(md, keyId) {
        // restart with no keyID should work
        md = runEncryptedMongod({
            restart: md,
        });
        assert.neq(null, md, "Wasn't able to restart mongod without a keyID");
        assertFind(md);
        MongoRunner.stopMongod(md);

        // restart explicitly with the keyID should also work
        md = runEncryptedMongod({
            restart: md,
            kmipKeyIdentifier: keyId,
        });
        assert.neq(null, md, "Wasn't able to restart mongod with the correct keyID of " + keyId);
        assertFind(md);
        MongoRunner.stopMongod(md);
    }

    clearRawMongoProgramOutput();
    const kmipServerPid = _startMongoProgram("python", testDir + "kmip_server.py");
    // Assert here that PyKMIP is compatible with the default Python version
    assert(checkProgram(kmipServerPid));
    // wait for PyKMIP, a KMIP server framework, to start
    assert.soon(() => rawMongoProgramOutput().search("KMIP server") !== -1);

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
    assert.throws(() => runEncryptedMongod({
                      kmipServerName: "10.0.0.1,192.168.1.1",
                      restart: md,
                  }),
                  [],
                  "Ran mongod with invalid KMIP server address");

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
})();
