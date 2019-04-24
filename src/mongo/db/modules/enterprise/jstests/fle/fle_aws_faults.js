/**
 * Verify the AWS KMS implementation can handle a buggy KMS.
 */

load("src/mongo/db/modules/enterprise/jstests/fle/lib/mock_kms.js");
load('jstests/ssl/libs/ssl_helpers.js');

(function() {
    "use strict";

    const x509_options = {sslMode: "requireSSL", sslPEMKeyFile: SERVER_CERT, sslCAFile: CA_CERT};

    const randomAlgorithm = "AEAD_AES_256_CBC_HMAC_SHA_512-Random";

    const conn = MongoRunner.runMongod(x509_options);
    const test = conn.getDB("test");
    const collection = test.coll;

    function runKMS(mock_kms, func) {
        mock_kms.start();

        const awsKMS = {
            accessKeyId: "access",
            secretAccessKey: "secret",
            url: mock_kms.getURL(),
        };

        const clientSideFLEOptions = {
            kmsProviders: {
                aws: awsKMS,
            },
            keyVaultCollection: collection,
        };

        const shell = Mongo(conn.host, clientSideFLEOptions);
        const cleanCacheShell = Mongo(conn.host, clientSideFLEOptions);

        collection.drop();

        func(shell, cleanCacheShell);

        mock_kms.stop();
    }

    function testBadEncryptResult(fault) {
        const mock_kms = new MockKMSServer(fault, false);

        runKMS(mock_kms, (shell) => {
            const keyStore = shell.getKeyStore();

            assert.throws(() => keyStore.createKey(
                              "aws", "arn:aws:kms:us-east-1:fake:fake:fake", ["mongoKey"]));
            assert.eq(keyStore.getKeys("mongoKey").toArray().length, 0);
        });
    }

    testBadEncryptResult(FAULT_ENCRYPT);
    testBadEncryptResult(FAULT_ENCRYPT_WRONG_FIELDS);
    testBadEncryptResult(FAULT_ENCRYPT_BAD_BASE64);

    function testBadDecryptResult(fault) {
        const mock_kms = new MockKMSServer(fault, false);

        runKMS(mock_kms, (shell) => {
            const keyStore = shell.getKeyStore();
            assert.writeOK(
                keyStore.createKey("aws", "arn:aws:kms:us-east-1:fake:fake:fake", ["mongoKey"]));
            const keyId = keyStore.getKeys("mongoKey").toArray()[0]._id;
            const str = "mongo";
            assert.throws(() => {
                const encStr = shell.encrypt(keyId, str, randomAlgorithm);
            });
        });
    }

    testBadDecryptResult(FAULT_DECRYPT);

    function testBadDecryptKeyResult(fault) {
        const mock_kms = new MockKMSServer(fault, true);

        runKMS(mock_kms, (shell, cleanCacheShell) => {
            const keyStore = shell.getKeyStore();

            assert.writeOK(
                keyStore.createKey("aws", "arn:aws:kms:us-east-1:fake:fake:fake", ["mongoKey"]));
            const keyId = keyStore.getKeys("mongoKey").toArray()[0]._id;
            const str = "mongo";
            const encStr = shell.encrypt(keyId, str, randomAlgorithm);

            mock_kms.enableFaults();

            assert.throws(() => {
                var str = cleanCacheShell.decrypt(encStr);
            });

        });
    }

    testBadDecryptKeyResult(FAULT_DECRYPT_WRONG_KEY);

    MongoRunner.stopMongod(conn);
}());