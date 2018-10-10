// Create a lot of databases to exercise the Windows secure memory allocator
// so that Windows needs to grow beyond the default 1413120 bytes maxWorkingSetSize per
// SetProcessWorkingSetSizeEx docs.
(function() {
    'use strict';

    if (!_isWindows()) {
        return;
    }

    var assetsPath = "src/mongo/db/modules/enterprise/jstests/encryptdb/libs/";

    var ekfValid1 = assetsPath + "ekf";

    const m = MongoRunner.runMongod({enableEncryption: "", encryptionKeyFile: ekfValid1});
    assert.neq(null, m, "Mongod did not start up with a valid key file.");

    for (let i = 0; i < 400; i++) {
        print(i);
        assert.commandWorked(m.getDB("a" + i).getCollection("a").insert({x: 1}));
    }

    MongoRunner.stopMongod(m);
})();
