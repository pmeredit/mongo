// This tests rolling over the database keys used by the encrypted storage engine when
// the cipher mode is AES256-GCM - this is separate from rolling over the master key
// which encrypts the keystore itself, this rolls over the keys used to encrypt user
// data in mongodb.
(function() {
    'use strict';
    const assetsPath = "src/mongo/db/modules/enterprise/jstests/encryptdb/libs/";

    load(assetsPath + "helpers.js");

    if (!platformSupportsGCM) {
        // This test doesn't get run on platforms that do not support GCM
        return;
    }

    const keyFilePath = assetsPath + "ekf";
    run("chmod", "600", keyFilePath);

    let mongod = MongoRunner.runMongod({
        enableEncryption: "",
        encryptionKeyFile: keyFilePath,
        encryptionCipherMode: "AES256-GCM",
    });

    assert(mongod, "Could not start mongod with GCM encryption enabled");

    let testdb = mongod.getDB("test");
    assert.commandWorked(testdb.test.insert({foo: "bar"}));

    MongoRunner.stopMongod(mongod);

    mongod = MongoRunner.runMongod({
        restart: mongod,
        remember: true,
        enableEncryption: "",
        encryptionKeyFile: keyFilePath,
        encryptionCipherMode: "AES256-GCM",
        eseDatabaseKeyRollover: ""
    });

    testdb = mongod.getDB("test");
    assert.commandWorked(testdb.test.insert({bizz: "buzz"}));
    const findResults = testdb.test.find({}, {_id: 0}).toArray();

    print(tojson(findResults));
    assert.eq(findResults.length, 2);
    assert.contains({bizz: "buzz"}, findResults);
    assert.contains({foo: "bar"}, findResults);

    MongoRunner.stopMongod(mongod);
})();
