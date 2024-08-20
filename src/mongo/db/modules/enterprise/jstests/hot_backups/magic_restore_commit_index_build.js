/**
 * Tests that two-phase index builds are restarted when started up at a checkpoint before the index
 * build is completed and started using magic restore. This tests both PIT and non-PIT restore.
 *
 * @tags: [
 *      requires_persistence,
 *      requires_replication,
 *      requires_wiredtiger,
 * ]
 */
import {MagicRestoreUtils} from "jstests/libs/backup_utils.js";
import {ReplSetTest} from "jstests/libs/replsettest.js";
import {IndexBuildTest} from "jstests/noPassthrough/libs/index_build.js";

// TODO SERVER-86034: Run on Windows machines once named pipe related failures are resolved.
if (_isWindows()) {
    jsTestLog("Temporarily skipping test for Windows variants. See SERVER-86034.");
    quit();
}

// When opening a backup cursor, only checkpointed data is backed up. However, the most up-to-date
// size storer information is used. Thus the fast count may be inaccurate.
TestData.skipEnforceFastCountOnValidate = true;

function runTest(pit) {
    let rst = new ReplSetTest({nodes: 1});
    rst.startSet();
    rst.initiate();

    let primary = rst.getPrimary();

    // Opening backup cursors can race with taking a checkpoint, so disable checkpoints.
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

    // Start a two-phase index build without finishing it. Generates a 'startIndexBuild' oplog
    // entry. However, since this will be part of the checkpoint, this isn't replayed during oplog
    // recovery.
    let awaitIndexBuild = IndexBuildTest.startIndexBuild(primary, "test.a", {x: 1});
    IndexBuildTest.waitForIndexBuildToScanCollection(db, collName, "x_1");

    const magicRestoreUtils = new MagicRestoreUtils(
        {backupSource: primary, pipeDir: MongoRunner.dataDir, insertHigherTermOplogEntry: false});

    magicRestoreUtils.takeCheckpointAndOpenBackup();

    // Finish the index build, generating a 'commitIndexBuild' oplog entry.
    IndexBuildTest.resumeIndexBuilds(primary);
    awaitIndexBuild();

    const checkpointTimestamp = magicRestoreUtils.getCheckpointTimestamp();
    let {lastOplogEntryTs, entriesAfterBackup} = magicRestoreUtils.getEntriesAfterBackup(primary);
    // Update config.system.indexBuilds, commitIndexBuild, drop entry from config.system.indexBuilds
    assert.eq(entriesAfterBackup.length, 3);

    // Assert that the commitIndexBuild entry exists.
    assert(entriesAfterBackup[1].o.commitIndexBuild);

    magicRestoreUtils.copyFilesAndCloseBackup();

    assert.commandWorked(
        primary.adminCommand({configureFailPoint: "pauseCheckpointThread", mode: "off"}));

    let expectedConfig = assert.commandWorked(primary.adminCommand({replSetGetConfig: 1})).config;
    // The new node will be allocated a new port by the test fixture.
    expectedConfig.members[0].host = getHostName() + ":" + (Number(primary.port) + 2);
    rst.stopSet(null /* signal */, false /* forRestart */, {noCleanData: true});

    let restoreConfiguration = {
        "nodeType": "replicaSet",
        "replicaSetConfig": expectedConfig,
        "maxCheckpointTs": checkpointTimestamp,
    };
    if (pit) {
        // Restore to the timestamp of the last oplog entry on the source cluster.
        restoreConfiguration.pointInTimeTimestamp = lastOplogEntryTs;
    }

    restoreConfiguration =
        magicRestoreUtils.appendRestoreToHigherTermThanIfNeeded(restoreConfiguration);

    magicRestoreUtils.writeObjsAndRunMagicRestore(
        restoreConfiguration, pit ? entriesAfterBackup : [], {"replSet": jsTestName()});

    rst = new ReplSetTest({nodes: 1});
    rst.startSet({dbpath: magicRestoreUtils.getBackupDbPath(), noCleanData: true});

    primary = rst.getPrimary();
    magicRestoreUtils.postRestoreChecks({
        node: primary,
        expectedConfig: expectedConfig,
        dbName: dbName,
        collName: collName,
        expectedOplogCountForNs: 5,
        opFilter: "i",
        expectedNumDocsSnapshot: 5,
    });

    db = primary.getDB(dbName);
    assert.soonNoExcept(() => {
        // We rebuilt "x_1" to completion.
        IndexBuildTest.assertIndexes(
            db.getCollection(collName), 2, ["_id_", "x_1"], [], {includeBuildUUIDs: true});
        return true;
    });

    rst.stopSet();
}

runTest(true /* pit */);
runTest(false /* pit */);
