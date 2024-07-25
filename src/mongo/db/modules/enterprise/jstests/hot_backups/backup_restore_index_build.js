/**
 * Tests that two-phase index builds started using the '--recoverFromOplogAsStandalone' startup
 * parameter do not crash the server.
 *
 * @tags: [
 *      requires_persistence,
 *      requires_replication,
 *      requires_wiredtiger
 * ]
 */
import {copyFileHelper, openBackupCursor} from "jstests/libs/backup_utils.js";
import {ReplSetTest} from "jstests/libs/replsettest.js";
import {IndexBuildTest} from "jstests/noPassthrough/libs/index_build.js";

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
    assert.commandWorked(db.getCollection(collName).insert({x: i}));
}

// Take the checkpoint to be used by the backup cursor.
assert.commandWorked(db.adminCommand({fsync: 1}));

IndexBuildTest.pauseIndexBuilds(primary);

// Start a two-phase index build without finishing it.
const awaitIndexBuild = IndexBuildTest.startIndexBuild(primary, "test.a", {x: 1});
IndexBuildTest.waitForIndexBuildToScanCollection(db, collName, "x_1");

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

// Finish the index build.
IndexBuildTest.resumeIndexBuilds(primary);
awaitIndexBuild();

assert.commandWorked(
    primary.adminCommand({configureFailPoint: "pauseCheckpointThread", mode: "off"}));

rst.stopSet(/*signal=*/ null, /*forRestart=*/ true);

// Startup with '--recoverFromOplogAsStandalone' and index builds paused.
let conn = MongoRunner.runMongod({
    dbpath: backupDbPath,
    noCleanData: true,
    setParameter: {
        recoverFromOplogAsStandalone: true,
        takeUnstableCheckpointOnShutdown: true,
        "failpoint.hangAfterStartingIndexBuild": tojson({mode: "alwaysOn"})
    }
});
assert(conn);

// Read-only mode is enabled due to '--recoverFromOplogAsStandalone' parameter.
checkLog.containsJson(conn, 21558);

// Oplog recovery is done at this point. Resume the index build.
db = conn.getDB(dbName);
IndexBuildTest.waitForIndexBuildToScanCollection(db, collName, "x_1");
IndexBuildTest.resumeIndexBuilds(conn);

// Collection scan done for the index build.
checkLog.containsJson(conn, 20391);

// The index build will not be finished in standalone mode.
assert.soonNoExcept(() => {
    IndexBuildTest.assertIndexes(
        db.getCollection(collName), 2, ["_id_"], ["x_1"], {includeBuildUUIDs: true});
    return true;
});

MongoRunner.stopMongod(conn);
