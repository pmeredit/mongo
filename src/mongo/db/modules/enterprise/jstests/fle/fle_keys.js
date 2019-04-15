/**
 * Check functionality of KeyStore.js
 */

load("src/mongo/db/modules/enterprise/jstests/fle/lib/mock_kms.js");
load('jstests/ssl/libs/ssl_helpers.js');

(function() {
    "use strict";

    const mock_kms = new MockKMSServer();
    mock_kms.start();

    const x509_options = {sslMode: "requireSSL", sslPEMKeyFile: SERVER_CERT, sslCAFile: CA_CERT};

    const conn = MongoRunner.runMongod(x509_options);
    const test = conn.getDB("test");
    const collection = test.coll;

    const awsKMS = {
        accessKeyId: "access",
        secretAccessKey: "secret",
        url: mock_kms.getURL(),
    };

    const clientSideFLEOptions = {
        awsKMS: awsKMS,
        keyVaultCollection: collection,
    };

    const conn_str = "mongodb://" + conn.host + "/?ssl=true";
    const shell = Mongo(conn_str, clientSideFLEOptions);
    const keyStore = shell.getKeyStore();

    var key = keyStore.createKey("customerMasterKey", ['mongoKey']);
    assert.eq(1, keyStore.getKeys().itcount());

    var result = keyStore.createKey("newKey", {});
    assert.eq("TypeError: key alternate names must be of Array type.", result);

    result = keyStore.createKey("newKey", [1]);
    assert.eq("TypeError: items in key alternate names must be of String type.", result);

    assert.eq(1, keyStore.getKeyByAltName("mongoKey").itcount());

    var keyId = keyStore.getKeyByAltName("mongoKey").toArray()[0]._id;

    keyStore.addKeyAlternateName(keyId, "mongoKey2");

    assert.eq(1, keyStore.getKeyByAltName("mongoKey2").itcount());
    assert.eq(2, keyStore.getKey(keyId).toArray()[0].keyAltNames.length);
    assert.eq(1, keyStore.getKeys().itcount());

    result = keyStore.addKeyAlternateName(keyId, [2]);
    assert.eq("TypeError: key alternate name cannot be object or array type.", result);

    keyStore.removeKeyAlternateName(keyId, "mongoKey2");
    assert.eq(1, keyStore.getKey(keyId).toArray()[0].keyAltNames.length);

    result = keyStore.deleteKey(keyId);
    assert.eq(0, keyStore.getKey(keyId).itcount());
    assert.eq(0, keyStore.getKeys().itcount());

    assert.writeOK(keyStore.createKey("key1"));
    assert.writeOK(keyStore.createKey("key2"));
    assert.writeOK(keyStore.createKey("key3"));

    assert.eq(3, keyStore.getKeys().itcount());

    MongoRunner.stopMongod(conn);
    mock_kms.stop();
}());