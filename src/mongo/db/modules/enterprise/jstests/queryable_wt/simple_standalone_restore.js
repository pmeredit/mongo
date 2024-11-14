/**
 * Takes a full backup against a standalone and restores it using queryable.
 *
 * @tags: [requires_wiredtiger]
 */
import {copyFileHelper, openBackupCursor} from "jstests/libs/backup_utils.js";
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

let conn = MongoRunner.runMongod();
assert(conn);

resetDbpath(TestData.queryable_dbpath);
mkdir(TestData.queryable_dbpath + kSeparator + "journal");

// Take the initial checkpoint.
assert.commandWorked(conn.adminCommand({fsync: 1}));

// Create a collection and insert some data.
const dbName = "test";
const collName = jsTestName();

let db = conn.getDB(dbName);
assert.commandWorked(db.createCollection(collName));
let coll = db.getCollection(collName);

const kNumDocs = 10;
for (let i = 0; i < kNumDocs; i++) {
    assert.commandWorked(coll.insert({_id: i}));
}

const backupCursor = openBackupCursor(conn.getDB("admin"));

// Print the metadata document.
assert(backupCursor.hasNext());
jsTestLog(backupCursor.next());

while (backupCursor.hasNext()) {
    const doc = backupCursor.next();

    // Copy all the files for the full backup.
    jsTestLog("Copying file: " + tojson(doc));
    copyFileHelper(
        {filename: doc.filename, fileSize: doc.fileSize}, conn.dbpath, TestData.queryable_dbpath);
}

backupCursor.close();

MongoRunner.stopMongod(conn);

// We don't need to update the `storage.bson` file to modify the storage engine from wiredTiger to
// queryable_wt as the file isn't copied by the backup cursor.
conn = MongoRunner.runMongod({
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
// However, in standalone mode all tables have logging enabled and we expect the documents to be
// recovered.
assert.eq(kNumDocs, coll.find().toArray().length);
MongoRunner.stopMongod(conn);
