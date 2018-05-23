// This tests that encrypted storage engines can be written to, rebooted, and read from
(function() {
    'use strict';

    var runTest = function(cipherMode, expectSuccessfulStartup, readOnly) {
        var key = "src/mongo/db/modules/enterprise/jstests/encryptdb/libs/ekf";
        run("chmod", "600", key);

        var md = MongoRunner.runMongod(
            {enableEncryption: "", encryptionKeyFile: key, encryptionCipherMode: cipherMode});
        if (!expectSuccessfulStartup) {
            assert.eq(null, md, "Was able to start mongodb with invalid cipher " + cipherMode);
            return;
        }
        assert.neq(null, md, "Unable to start mongodb with " + cipherMode);

        // verify that we can make a lot of databases
        for (var i = 0; i < 100; i++) {
            var testdb = md.getDB("test_" + i);
            testdb["foo"].insert({
                x: 0,
            });
        }

        var testdb = md.getDB("test");
        for (var i = 0; i < 1000; i++) {
            testdb["foo"].insert({
                x: i,
                str: "A string of sensitive data to be encrypted",
                fun: function() {
                    return "A result";
                },
                data: BinData(0, "BBBBBBBBBBBB")
            });
        }
        MongoRunner.stopMongod(md);

        let options = {
            restart: md,
            remember: true,
            enableEncryption: "",
            encryptionKeyFile: key,
            encryptionCipherMode: cipherMode,
        };
        if (readOnly) {
            options.queryableBackupMode = "";
        }

        md = MongoRunner.runMongod(options);
        assert.neq(null, md, "Could not restart mongod with " + cipherMode);
        testdb = md.getDB("test");

        assert.eq(1000, testdb["foo"].count(), "Could not read encrypted storage.");
        var result = testdb["foo"].findOne({x: 500});

        // With --enableJavaScriptProtection, functions are presented as Code objects.
        if (result.fun instanceof Code) {
            result.fun = eval("(" + result.fun.code + ")");
        }
        assert.eq("A result", result.fun(), "Could not get out an expected value");

        MongoRunner.stopMongod(md);
    };

    // Ubuntu 12.04, SUSE and RHEL5 have a bug in their copy of OpenSSL which keeps us from running
    // with GCM.
    // Detect these platforms, and assert accordingly.

    var md = MongoRunner.runMongod({});
    assert.neq(null, md, "Failed to start mongod to probe host type");
    var db = md.getDB("test");
    var hostInfo = db.hostInfo();
    MongoRunner.stopMongod(md);

    var isUbuntu1204 = (hostInfo.os.type == "Linux" && hostInfo.os.name == "Ubuntu" &&
                        hostInfo.os.version == "12.04");
    var isSUSE11 = (hostInfo.os.type == "Linux" && hostInfo.os.name.match("SUSE.+11"));
    var isRHEL5 = (hostInfo.os.type == "Linux" &&
                   hostInfo.os.name.match("Red Hat Enterprise Linux Server release 5"));
    var isOSX = (hostInfo.os.type == "Darwin");

    const isWindowsSchannel =
        (hostInfo.os.type == "Windows" && /SChannel/.test(buildInfo().openssl.running));

    const platformSupportsGCM =
        !(isUbuntu1204 || isSUSE11 || isRHEL5 || isOSX || isWindowsSchannel);

    runTest("AES256-CBC", true, false);
    runTest("AES256-CBC", true, true);
    runTest("AES256-GCM", platformSupportsGCM, false);
    runTest("AES256-GCM", platformSupportsGCM, true);
    runTest("BadCipher", false);

})();
