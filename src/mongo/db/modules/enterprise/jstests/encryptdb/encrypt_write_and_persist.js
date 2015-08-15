// This tests that encrypted storage engines can be written to, rebooted, and read from
(function () {
    'use strict';

var runTest = function(cipherMode, expectSuccessfulStartup) {
    var key = "src/mongo/db/modules/enterprise/jstests/encryptdb/libs/ekf";
    run("chmod", "600", key);

    var md = MongoRunner.runMongod({enableEncryption: "", encryptionKeyFile: key,
                                    encryptionCipherMode: cipherMode});
    if (!expectSuccessfulStartup) {
        assert.eq(null, md, "Was able to start mongodb with invalid cipher " + cipherMode);
        return;
    }
    assert.neq(null, md, "Unable to start mongodb with " + cipherMode);

    var testdb = md.getDB("test");
    for (var i = 0; i < 1000; i++) {
        testdb["foo"].insert({x: i, str: "A string of sensitive data to be encrypted",
                              fun: function() {return "A result";},
                              data: BinData(0, "BBBBBBBBBBBB")});
    }
    MongoRunner.stopMongod(md);

    md = MongoRunner.runMongod({restart: md, remember: true, enableEncryption: "",
                                encryptionKeyFile: key, encryptionCipherMode: cipherMode});
    assert.neq(null, md, "Could not restart mongod with " + cipherMode);
    testdb = md.getDB("test");

    assert.eq(1000, testdb["foo"].count(), "Could not read encrypted storage.");
    var result = testdb["foo"].findOne({x: 500});
    assert.eq("A result", result.fun(), "Could not get out an expected value");

    MongoRunner.stopMongod(md);
};

runTest("AES256-CBC", true);
runTest("AES256-GCM", true);
runTest("BadCipher", false);

}) ();
