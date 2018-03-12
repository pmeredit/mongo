// This test tests KMIP options and key rotation for encrypted storage engine
// It assumes that PyKMIP is installed

(function() {
    "use strict";

    var testDir = "src/mongo/db/modules/enterprise/jstests/encryptdb/";

    function runEncryptedMongod(params) {
        var defaultParams = {
            enableEncryption: "",
            kmipServerName: "127.0.0.1",
            // default port is 5696, we're setting it here to test option parsing
            kmipPort: "6666",
            kmipServerCAFile: "jstests/libs/ca.pem",
        };

        // Windows and Apple SSL providers don't support encrypted PEM files.
        if (/OpenSSL/.test(getBuildInfo().openssl.running)) {
            defaultParams.kmipClientCertificateFile =
                testDir + "libs/client_password_protected.pem";
            defaultParams.kmipClientCertificatePassword = "qwerty";
        } else {
            defaultParams.kmipClientCertificateFile = "jstests/libs/client.pem";
        }

        return MongoRunner.runMongod(Object.merge(params, defaultParams));
    }

    function assertFind(md) {
        var testDB = md.getDB("test");
        var doc = testDB.test.findOne({}, {_id: 0});

        assert.eq({"a": 1}, doc, "Document did not have expected value");
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

    var pid = _startMongoProgram("python", testDir + "kmip_server.py");
    // Assert here that PyKMIP is compatible with the default Python version
    assert(checkProgram(pid));
    // wait for PyKMIP, a KMIP server framework, to start
    assert.soon(function() {
        return rawMongoProgramOutput().search("KMIP server") !== -1;
    });

    // start mongod with default keyID of "1"
    var md = runEncryptedMongod();
    var testDB = md.getDB("test");
    testDB.test.insert({a: 1});
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

    stopMongoProgramByPid(pid);
})();
