/**
 * Test that the 'sizeStorer.wt' file gets flushed the most up-to-date information before beginning
 * a nonblocking backup.
 *
 * @tags: [requires_fsync, requires_persistence]
 */
(function() {
    'use strict';

    load('jstests/libs/backup_utils.js');

    function addDocuments(db, starting) {
        let size = 100;
        jsTest.log("Creating " + size + " test documents.");
        let bulk = db.getCollection(collName).initializeUnorderedBulkOp();
        for (let i = starting; i < starting + size; ++i) {
            bulk.insert({i: i});
        }
        assert.writeOK(bulk.execute());
    }

    const dbName = "test";
    const collName = "coll";

    const rst = new ReplSetTest({nodes: 2});
    rst.startSet();
    rst.initiate();

    const primary = rst.getPrimary();
    const primaryDB = primary.getDB(dbName);

    // Add some documents and force a checkpoint to have an initial sizeStorer file to backup.
    addDocuments(primaryDB, 0);
    rst.awaitReplication();
    assert.commandWorked(primaryDB.adminCommand({fsync: 1}));

    const backupPath = MongoRunner.dataPath + 'backup_restore';

    // Add more documents and begin a backup immediately after, before another checkpoint can run.
    addDocuments(primaryDB, 100);
    rst.awaitReplication();

    let numRecords = primaryDB.getCollection(collName).find({}).count();
    assert.eq(200, numRecords);

    backupData(primary, backupPath);

    let copiedFiles = ls(backupPath);
    assert.gt(copiedFiles.length, 0);

    rst.stopSet();

    const conn = MongoRunner.runMongod({dbpath: backupPath, noCleanData: true});
    assert.neq(conn, null);

    // Check that the size information is correct for the collection.
    const db = conn.getDB(dbName);
    numRecords = db.getCollection(collName).find({}).count();
    assert.eq(200, numRecords);

    MongoRunner.stopMongod(conn);
}());
