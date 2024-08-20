/**
 * Test that the 'sizeStorer.wt' file gets flushed the most up-to-date information before beginning
 * a nonblocking backup, including when a backup cursor is extended.
 *
 * @tags: [
 *   requires_fsync,
 *   requires_persistence,
 * ]
 */
import {
    copyBackupCursorExtendFiles,
    copyBackupCursorFiles,
    extendBackupCursor,
    getBackupCursorMetadata,
    openBackupCursor,
} from "jstests/libs/backup_utils.js";
import {ReplSetTest} from "jstests/libs/replsettest.js";

// When opening a backup cursor, only checkpointed data is backed up. However, the most up-to-date
// size storer information is used. Thus the fast count may be inaccurate.
TestData.skipEnforceFastCountOnValidate = true;

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

function testBackup(extend) {
    const rst = new ReplSetTest({nodes: 2});
    rst.startSet();
    rst.initiate();

    const primary = rst.getPrimary();
    const primaryDB = primary.getDB(dbName);

    // Add some documents and force a checkpoint to have an initial sizeStorer file to backup.
    addDocuments(primaryDB, 0);
    rst.awaitLastOpCommitted();
    assert.commandWorked(primaryDB.adminCommand({fsync: 1}));

    const backupPath = MongoRunner.dataPath + 'backup_restore';

    // Add more documents and begin a backup immediately after, before another checkpoint can run.
    addDocuments(primaryDB, 100);
    rst.awaitLastOpCommitted();

    let numRecords = primaryDB.getCollection(collName).find({}).count();
    assert.eq(200, numRecords);

    const backupCursor = openBackupCursor(primary.getDB("admin"));
    const metadata = getBackupCursorMetadata(backupCursor);
    copyBackupCursorFiles(
        backupCursor, /*namespacesToSkip=*/[], metadata.dbpath, backupPath, false /* async */);

    // Add more documents after backing up the previously inserted documents. These will not be
    // backed up unless we are testing with backup cursor extension.
    addDocuments(primaryDB, 200);
    rst.awaitLastOpCommitted();

    const res = assert.commandWorked(primaryDB.runCommand({count: collName}));
    assert.eq(300, res.n);

    if (extend) {
        const extendedCursor = extendBackupCursor(primary, metadata.backupId, res.operationTime);
        copyBackupCursorExtendFiles(extendedCursor,
                                    /*namespacesToSkip=*/[],
                                    metadata.dbpath,
                                    backupPath,
                                    false /* async */);
    }

    let copiedFiles = ls(backupPath);
    assert.gt(copiedFiles.length, 0);

    rst.stopSet();

    const conn = MongoRunner.runMongod({dbpath: backupPath, noCleanData: true});
    assert.neq(conn, null);

    // Check that the size information is correct for the collection.
    const db = conn.getDB(dbName);
    numRecords = db.getCollection(collName).find({}).count();
    assert.eq(extend ? 300 : 200, numRecords);

    MongoRunner.stopMongod(conn);
}

testBackup(false /* extend */);
testBackup(true /* extend */);