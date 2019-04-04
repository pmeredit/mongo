// Tests that a mongod instance starting with a v0 keystore will
// upgrade to a v1 keystore on being requested for a rollover.
// Restarts several times adding more data to multiple databases
// and ensuring it's still readable after every invocation.

(function() {
    'use strict';

    const assetsPath = "src/mongo/db/modules/enterprise/jstests/encryptdb/libs/";
    load(assetsPath + "helpers.js");
    if (!platformSupportsGCM) {
        print("Skipping test: Platform does not support GCM");
        return;
    }

    const keyFilePath = assetsPath + "ekf";
    run("chmod", "600", keyFilePath);

    let startCount = 0;
    let expectSchema = 0;
    let rolloverCount = 0;
    let lastSystemKeyId = 0;

    function runMongod(cipher, opts = {}, features = {}) {
        const expectRollover =
            features.expectRollover || (opts.eseDatabaseKeyRollover !== undefined);

        opts = Object.assign({},
                             {
                               dbpath: MongoRunner.dataPath + 'ese-database_key_upgrade',
                               enableEncryption: '',
                               encryptionKeyFile: keyFilePath,
                               encryptionCipherMode: cipher,
                             },
                             opts);

        if (startCount) {
            opts.noCleanData = true;
        }

        const mongod = MongoRunner.runMongod(opts);
        if (expectRollover && (cipher == 'AES256-CBC')) {
            assert(!mongod, "Mongod rolled over a CBC store.");
            return;
        }
        assert(mongod, "Could not start mongod");

        if (expectRollover) {
            expectSchema = 1;
            ++rolloverCount;
        }

        const testdb = mongod.getDB("test");
        assert.commandWorked(testdb.test.insert({foo: "bar"}));

        const info =
            assert.commandWorked(mongod.adminCommand({getParameter: 1, keystoreSchemaVersion: 1}))
                .keystoreSchemaVersion;
        printjson(info);
        assert.eq(info.version, expectSchema);
        assert.eq(info.rolloverId, rolloverCount);

        if (expectSchema === 0) {
            assert.eq(typeof(info.systemKeyId), 'string');
            assert.eq(info.systemKeyId, "");
        } else if (expectRollover) {
            assert.gt(Number(info.systemKeyId), lastSystemKeyId);
            lastSystemKeyId = Number(info.systemKeyId);
        } else {
            assert.eq(Number(info.systemKeyId), lastSystemKeyId);
        }

        // Check for data written on previous runs,
        // and add new data so we reach N dbs of N collections with N documents each.
        for (let dbNum = 0; dbNum < startCount; ++dbNum) {
            for (let collNum = 0; collNum < startCount; ++collNum) {
                const coll = mongod.getDB('db' + dbNum).getCollection('coll' + collNum);
                assert.eq(coll.count(), startCount);
                for (let recNum = 0; recNum < startCount; ++recNum) {
                    assert.eq(coll.find({x: recNum}).count(), 1);
                }
                // Fill out each collection with an additional record.
                assert.commandWorked(coll.insert({x: startCount}));
            }

            // Add a new collection to each db.
            const newColl = mongod.getDB('db' + dbNum).getCollection('coll' + startCount);
            for (let recNum = 0; recNum <= startCount; ++recNum) {
                assert.commandWorked(newColl.insert({x: recNum}));
            }
        }
        // Add a new db.
        for (let collNum = 0; collNum <= startCount; ++collNum) {
            const newColl = mongod.getDB('db' + startCount).getCollection('coll' + collNum);
            for (let recNum = 0; recNum <= startCount; ++recNum) {
                assert.commandWorked(newColl.insert({x: recNum}));
            }
        }

        if (features.openBackupCursor) {
            mongod.getDB('admin').aggregate([{$backupCursor: {}}]);
        }

        ++startCount;
        MongoRunner.stopMongod(mongod);
    }

    runMongod('AES256-GCM', {setParameter: {keystoreSchemaVersion: 0}});
    runMongod('AES256-GCM');
    runMongod('AES256-GCM', {eseDatabaseKeyRollover: ''});
    runMongod('AES256-GCM');
    runMongod('AES256-GCM', {eseDatabaseKeyRollover: ''});
    runMongod('AES256-GCM');

    // Sanity check the counters.
    assert.eq(startCount, 6);
    assert.eq(expectSchema, 1);
    assert.eq(rolloverCount, 2);

    // Trigger a "dirty" state by opening a backup cursor,
    // which should cause a rollover.
    runMongod('AES256-GCM', {}, {openBackupCursor: true});
    runMongod('AES256-GCM', {}, {expectRollover: true});

    assert.eq(startCount, 8);
    assert.eq(expectSchema, 1);
    assert.eq(rolloverCount, 3);

    // Reset state for CBC sanity check.
    startCount = 0;
    expectSchema = 0;
    rolloverCount = 0;
    lastSystemKeyId = 0;

    runMongod('AES256-CBC');
    runMongod('AES256-CBC');
    runMongod('AES256-CBC', {eseDatabaseKeyRollover: ''});

    assert.eq(startCount, 2);
    assert.eq(expectSchema, 0);
    assert.eq(rolloverCount, 0);
})();
