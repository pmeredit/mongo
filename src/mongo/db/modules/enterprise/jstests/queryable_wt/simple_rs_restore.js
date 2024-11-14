/**
 * Takes a full backup against a replica set and restores it using queryable.
 *
 * @tags: [requires_wiredtiger]
 */
import {copyFileHelper, openBackupCursor} from "jstests/libs/backup_utils.js";
import {ReplSetTest} from "jstests/libs/replsettest.js";
import {
    kSeparator
} from "src/mongo/db/modules/enterprise/jstests/hot_backups/libs/incremental_backup_helpers.js";

// Windows doesn't guarantee synchronous file operations.
if (_isWindows()) {
    print("Skipping test on windows");
    quit();
}

// When opening a backup cursor, only checkpointed data is backed up. However, the most up-to-date
// size storer information is used. Thus the fast count may be inaccurate.
TestData.skipEnforceFastCountOnValidate = true;

const rst = new ReplSetTest({
    nodes: 1,
    nodeOptions: {
        // Disable periodic checkpoints.
        syncdelay: 0,
    }
});
rst.startSet();
rst.initiate();

resetDbpath(TestData.queryable_dbpath);
mkdir(TestData.queryable_dbpath + kSeparator + "journal");

const primary = rst.getPrimary();

// Take the initial checkpoint.
assert.commandWorked(primary.adminCommand({fsync: 1}));

// Create a collection and insert some data.
const dbName = "test";
const collName = jsTestName();

let db = primary.getDB(dbName);
assert.commandWorked(db.createCollection(collName));
let coll = db.getCollection(collName);

const kNumDocs = 10;
for (let i = 0; i < kNumDocs; i++) {
    assert.commandWorked(coll.insert({_id: i}));
}

const backupCursor = openBackupCursor(primary.getDB("admin"));

// Print the metadata document.
assert(backupCursor.hasNext());
jsTestLog(backupCursor.next());

while (backupCursor.hasNext()) {
    const doc = backupCursor.next();

    // Copy all the files for the full backup.
    jsTestLog("Copying file: " + tojson(doc));
    copyFileHelper({filename: doc.filename, fileSize: doc.fileSize},
                   primary.dbpath,
                   TestData.queryable_dbpath);
}

backupCursor.close();

rst.stopSet();

// We don't need to update the `storage.bson` file to modify the storage engine from wiredTiger to
// queryable_wt as the file isn't copied by the backup cursor.
let conn = MongoRunner.runMongod({
    dbpath: TestData.queryable_dbpath,
    storageEngine: "queryable_wt",
    noCleanData: true,
    queryableBackupApiUri: "localhost:8080",
    queryableSnapshotId: "123456781234567812345678",
    queryableBackupMode: ""
});
assert(conn);

db = conn.getDB(dbName);
coll = db.getCollection(collName);

// All documents to the collection were written after the checkpoint the backup was taken against.
// This test scenario does not replay the oplog.
assert.eq(0, coll.find().toArray().length);
MongoRunner.stopMongod(conn);
