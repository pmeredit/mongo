/**
 * Tests that two-phase index builds are restarted when started up at a checkpoint before the index
 * build is completed and started using the '--recoverFromOplogAsStandalone' startup parameter.
 *
 * @tags: [
 *      requires_persistence,
 *      requires_replication,
 *      requires_wiredtiger,
 * ]
 */
import {copyFileHelper, openBackupCursor} from "jstests/libs/backup_utils.js";
import {ReplSetTest} from "jstests/libs/replsettest.js";
import {IndexBuildTest} from "jstests/noPassthrough/libs/index_build.js";

// When opening a backup cursor, only checkpointed data is backed up. However, the most up-to-date
// size storer information is used. Thus the fast count may be inaccurate.
TestData.skipEnforceFastCountOnValidate = true;

const rst = new ReplSetTest({nodes: 1});
rst.startSet();
rst.initiate();

const primary = rst.getPrimary();

// Opening backup cursors can race with taking a checkpoint, so disable checkpoints.
// This makes testing quicker and more predictable. In production, a poorly interleaved checkpoint
// will return an error, requiring retry.
assert.commandWorked(
    primary.adminCommand({configureFailPoint: "pauseCheckpointThread", mode: "alwaysOn"}));

const dbName = "test";
let db = primary.getDB(dbName);

const collName = "a";
assert.commandWorked(db.createCollection(collName));

for (let i = 0; i < 5; i++) {
    assert.commandWorked(db.getCollection(collName).insert({x: i, y: i}));
}

IndexBuildTest.pauseIndexBuilds(primary);

// Start a two-phase index build without finishing it. Generates a 'startIndexBuild' oplog entry.
// However, since this will be part of the checkpoint, this isn't replayed during oplog recovery.
let awaitIndexBuild = IndexBuildTest.startIndexBuild(primary, "test.a", {x: 1});
IndexBuildTest.waitForIndexBuildToScanCollection(db, collName, "x_1");

// Take the checkpoint to be used by the backup cursor. Operations done beyond this point will be
// replayed from the oplog during startup recovery.
assert.commandWorked(db.adminCommand({fsync: 1}));

// Finish the index build, generating a 'commitIndexBuild' oplog entry.
IndexBuildTest.resumeIndexBuilds(primary);
awaitIndexBuild();

const backupDbPath = primary.dbpath + "/backup";
resetDbpath(backupDbPath);
mkdir(backupDbPath + "/journal");

// Open a backup cursor on the checkpoint.
let backupCursor = openBackupCursor(primary.getDB("admin"));

// Print the metadata document.
assert(backupCursor.hasNext());
jsTestLog(backupCursor.next());

while (backupCursor.hasNext()) {
    let doc = backupCursor.next();

    jsTestLog("Copying for backup: " + tojson(doc));
    copyFileHelper({filename: doc.filename, fileSize: doc.fileSize}, primary.dbpath, backupDbPath);
}

backupCursor.close();

assert.commandWorked(
    primary.adminCommand({configureFailPoint: "pauseCheckpointThread", mode: "off"}));

rst.stopSet(/*signal=*/ null, /*forRestart=*/ true);

// Startup with '--recoverFromOplogAsStandalone'.
let conn = MongoRunner.runMongod({
    dbpath: backupDbPath,
    noCleanData: true,
    setParameter: {recoverFromOplogAsStandalone: true, takeUnstableCheckpointOnShutdown: true}
});
assert(conn);

db = conn.getDB(dbName);
assert.soonNoExcept(() => {
    // We rebuilt "x_1" to completion.
    IndexBuildTest.assertIndexes(
        db.getCollection(collName), 2, ["_id_", "x_1"], [], {includeBuildUUIDs: true});
    return true;
});

MongoRunner.stopMongod(conn);
