/**
 * Check functionality of KeyStore.js
 */

(function() {
    "use strict";

    const conn = MongoRunner.runMongod();
    const shell = Mongo(conn.host);
    const keyStore = shell.getKeyStore();

    assert.writeOK(keyStore.createKey(['mongoKey']));
    assert.eq(1, keyStore.getKeys().itcount());

    var result = keyStore.createKey("mongoKey");
    assert.eq("TypeError: key alternate names must be of Array type.", result);

    result = keyStore.createKey([1]);
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

    assert.writeOK(keyStore.createKey());
    assert.writeOK(keyStore.createKey());
    assert.writeOK(keyStore.createKey());

    assert.eq(3, keyStore.getKeys().itcount());

    MongoRunner.stopMongod(conn);
}());