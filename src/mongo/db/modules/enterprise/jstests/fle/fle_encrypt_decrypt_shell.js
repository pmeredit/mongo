/**
* Check the functionality of encrypt and decrypt
* functions in KeyStore.js
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

    var localKMS = {
        key: BinData(
            0,
            "/i8ytmWQuCe1zt3bIuVa4taPGKhqasVp0/0yI4Iy0ixQPNmeDF1J5qPUbBYoueVUJHMqj350eRTwztAWXuBdSQ=="),
    };

    const clientSideFLEOptions = {
        kmsProviders: {
            aws: awsKMS,
            local: localKMS,
        },
        keyVaultCollection: collection,
    };

    const kmsTypes = ["aws", "local"];

    for (const kmsType of kmsTypes) {
        collection.drop();

        const shell = Mongo(conn.host, clientSideFLEOptions);
        const keyStore = shell.getKeyStore();

        assert.writeOK(
            keyStore.createKey(kmsType, "arn:aws:kms:us-east-1:fake:fake:fake", ['mongoKey']));
        const keyId = keyStore.getKeyByAltName("mongoKey").toArray()[0]._id;

        const str = "mongo";
        const encStr = shell.encrypt(keyId, str);
        assert.eq(str, shell.decrypt(encStr));

        const obj = {"value": "mongo"};
        const encObj = shell.encrypt(keyId, obj);
        assert.eq(obj, shell.decrypt(encObj));

        const num = 12;
        const encNum = shell.encrypt(keyId, num);
        assert.eq(num, shell.decrypt(encNum));

        const numLong = NumberLong(13);
        const encNumLong = shell.encrypt(keyId, numLong);
        assert.eq(numLong, shell.decrypt(encNumLong));

        const int = NumberInt(23);
        const encInt = shell.encrypt(keyId, int);
        assert.eq(int, shell.decrypt(encInt));

        const dec = NumberDecimal(0.1234);
        const encDec = shell.encrypt(keyId, dec);
        assert.eq(dec, shell.decrypt(encDec));

        const uuid = UUID();
        const encUUID = shell.encrypt(keyId, uuid);
        assert.eq(uuid, shell.decrypt(encUUID));

        const date = ISODate();
        const encDate = shell.encrypt(keyId, date);
        assert.eq(date, shell.decrypt(encDate));

        const jsDate = new Date('December 17, 1995 03:24:00');
        const encJSDate = shell.encrypt(keyId, jsDate);
        assert.eq(jsDate, shell.decrypt(encJSDate));

        const binData = BinData(2, '1234');
        const encBinData = shell.encrypt(keyId, binData);
        assert.eq(binData, shell.decrypt(encBinData));

        const encBoolT = shell.encrypt(keyId, true);
        assert.eq(true, shell.decrypt(encBoolT));

        const encBoolF = shell.encrypt(keyId, false);
        assert.eq(false, shell.decrypt(encBoolF));

        const code = Code("function() { return true; }");
        const encCode = shell.encrypt(keyId, code);
        assert.eq(code, shell.decrypt(encCode));

        const timestamp = new Timestamp(1, 2);
        const encTimestamp = shell.encrypt(keyId, timestamp);
        assert.eq(timestamp, shell.decrypt(encTimestamp));

        const oid = new ObjectId();
        const encOid = shell.encrypt(keyId, oid);
        assert.eq(oid, shell.decrypt(encOid));

        const dbPointer = new DBPointer(str, oid);
        const encDBPointer = shell.encrypt(keyId, dbPointer);
        assert.eq(dbPointer, shell.decrypt(encDBPointer));

        const regex = /test/;
        const encRegex = shell.encrypt(keyId, regex);
        assert.eq(regex, shell.decrypt(encRegex));

        assert.throws(shell.encrypt, [keyId, null]);

        assert.throws(shell.encrypt, [keyId, undefined]);

        assert.throws(shell.encrypt, [keyId, MinKey()]);

        assert.throws(shell.encrypt, [keyId, MaxKey()]);

        assert.throws(shell.encrypt, [keyId, DBRef("test", "test", "test")]);
    }

    MongoRunner.stopMongod(conn);
    mock_kms.stop();
}());