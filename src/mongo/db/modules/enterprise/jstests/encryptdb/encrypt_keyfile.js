(function() {
'use strict';

var assetsPath = "src/mongo/db/modules/enterprise/jstests/encryptdb/libs/";

var ekfValid1 = assetsPath + "ekf";
var ekfValid2 = assetsPath + "ekf2";
var ekfInvalid = assetsPath + "badekf";

run("chmod", "600", ekfValid1);
run("chmod", "600", ekfValid2);
run("chmod", "600", ekfInvalid);

assert.throws(() => MongoRunner.runMongod({enableEncryption: "", encryptionKeyFile: ekfInvalid}),
              [],
              "Possible to start mongodb with an invalid encryption key file.");

var md2 = MongoRunner.runMongod({enableEncryption: "", encryptionKeyFile: ekfValid1});
assert.neq(null, md2, "Mongod did not start up with a valid key file.");

var testdb = md2.getDB("test");
testdb["foo"].insert({x: 1});

MongoRunner.stopMongod(md2);

assert.throws(
    () => MongoRunner.runMongod(
        {restart: md2, remember: true, enableEncryption: "", encryptionKeyFile: ekfValid2}),
    [],
    "Possible to start mongodb with an encryption key file with bad key.");

var md4 = MongoRunner.runMongod(
    {restart: md2, remember: true, enableEncryption: "", encryptionKeyFile: ekfValid1});
assert.neq(null, md4, "Mongod did not start up with a valid key file.");

testdb = md4.getDB("test");
assert.eq(1, testdb["foo"].count(), "Could not read encrypted storage.");

MongoRunner.stopMongod(md4);
})();
