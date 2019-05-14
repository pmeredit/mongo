/**
 * Verify the AWS KMS implementation can handle a buggy KMS.
 */

load("src/mongo/db/modules/enterprise/jstests/fle/lib/mock_kms.js");
load('jstests/ssl/libs/ssl_helpers.js');

(function() {
    "use strict";

    const x509_options = {sslMode: "requireSSL", sslPEMKeyFile: SERVER_CERT, sslCAFile: CA_CERT};

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

        collection.drop();

        func(shell);

        mock_kms.stop();
    }

    function testBadEncryptResult(fault) {
        const mock_kms = new MockKMSServer(fault, false);

        runKMS(mock_kms, (shell) => {
            const keyStore = shell.getKeyStore();

            assert.throws(() => keyStore.createKey("aws", "arn:aws:kms:us-east-1:fake:fake:fake"));
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
            assert.writeOK(keyStore.createKey("aws", "arn:aws:kms:us-east-1:fake:fake:fake"));
            const keyId = keyStore.getKeys("mongoKey").toArray()[0]._id;
            const str = "mongo";
            assert.throws(() => {
                const encStr = shell.encrypt(keyId, str);
            });
        });
    }

    testBadDecryptResult(FAULT_DECRYPT);

    function testBadDecryptKeyResult(fault) {
        const mock_kms = new MockKMSServer(fault, true);

        runKMS(mock_kms, (shell) => {
            const keyStore = shell.getKeyStore();

            assert.writeOK(keyStore.createKey("aws", "arn:aws:kms:us-east-1:fake:fake:fake"));
            const keyId = keyStore.getKeys("mongoKey").toArray()[0]._id;
            const str = "mongo";
            const encStr = shell.encrypt(keyId, str);

            mock_kms.enableFaults();
            assert.throws(() => {
                shell.decrypt(encStr);
            });

        });
    }

    testBadDecryptKeyResult(FAULT_DECRYPT_WRONG_KEY);

    MongoRunner.stopMongod(conn);
}());