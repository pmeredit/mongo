(function () {
    'use strict';

    var ekfValid1 = "src/mongo/db/modules/enterprise/jstests/libs/ekf";
    var ekfValid2 = "src/mongo/db/modules/enterprise/jstests/libs/ekf2";
    var ekfInvalid = "src/mongo/db/modules/enterprise/jstests/libs/badekf";

    var md1 = MongoRunner.runMongod({enableEncryption: "", encryptionKeyFile: ekfInvalid});
    assert.eq(null, md1, "Possible to start mongodb with an invalid encryption key file.");

    var md2 = MongoRunner.runMongod({enableEncryption: "", encryptionKeyFile: ekfValid1});
    assert.neq(null, md2, "Mongod did not start up with a valid key file.");

    var testdb = md2.getDB("test");
    testdb["foo"].insert({x: 1});

    MongoRunner.stopMongod(md2);

    var md3 = MongoRunner.runMongod({restart: md2, remember: true, enableEncryption: "", encryptionKeyFile: ekfValid2});
    assert.eq(null, md3, "Possible to start mongodb with an encryption key file with bad key.");

    var md4 = MongoRunner.runMongod({restart: md2, remember: true, enableEncryption: "", encryptionKeyFile: ekfValid1});
    assert.neq(null, md4, "Mongod did not start up with a valid key file.");

    testdb = md4.getDB("test");
    assert.eq(1, testdb["foo"].count(), "Could not read encrypted storage.");

    MongoRunner.stopMongod(md4);
}) ();
