/*
 * Tests a PIT replica set selective restore with magic restore. The test does the following:
 *
 * - Starts a replica set, inserts some initial data, and opens a backup cursor.
 * - Writes additional data that will be truncated by magic restore, since the writes occur after
 *   the checkpoint timestamp.
 * - Copies data files to the restore dbpath and closes the backup cursor.
 * - Writes a restore configuration object and the source oplog entries from after the checkpoint
 *   timestamp to a named pipe via the mongo shell.
 * - Starts a mongod with --magicRestore that parses the restore configuration, inserts and applies
 *   the additional oplog entries, and exits cleanly.
 * - Restarts the initial replica set and asserts the replica set config and data are what we
 *   expect.
 *
 * @tags: [
 *     requires_persistence,
 *     requires_wiredtiger,
 *     incompatible_with_windows_tls
 * ]
 */

import {MagicRestoreTest} from "jstests/libs/magic_restore_test.js";
import {ReplSetTest} from "jstests/libs/replsettest.js";

// TODO SERVER-87225: Enable fast count on validate when operations applied during a restore are
// counted correctly.
TestData.skipEnforceFastCountOnValidate = true;

function runTest(nodeOptionsArg) {
    jsTestLog("Running PIT magic restore with nodeOptionsArg: " + tojson(nodeOptionsArg));
    const sourceCluster = new ReplSetTest({nodes: 1, nodeOptions: nodeOptionsArg});
    sourceCluster.startSet();
    sourceCluster.initiateWithHighElectionTimeout();

    const sourcePrimary = sourceCluster.getPrimary();
    const dbName = "db";
    const coll = "coll";
    const excludedColl = "coll2";
    const namespacesToSkip = ["db.coll2"];

    const sourceDb = sourcePrimary.getDB(dbName);
    // Insert some data to restore. This data will be reflected in the restored node.
    ['a', 'b', 'c'].forEach(key => {
        assert.commandWorked(sourceDb.getCollection(excludedColl).insert({[key]: 1}));
        assert.commandWorked(sourceDb.getCollection(coll).insert({[key]: 1}));
    });

    // This timestamp will be used for a snapshot read.
    const snapshotTs = assert.commandWorked(sourcePrimary.adminCommand({replSetGetStatus: 1}))
                           .optimes.lastCommittedOpTime.ts;

    const magicRestoreUtils =
        new MagicRestoreUtils({rst: sourceCluster, pipeDir: MongoRunner.dataDir});

    magicRestoreUtils.takeCheckpointAndOpenBackup();

    // These documents will be truncated by magic restore, since they were written after the backup
    // cursor was opened. We will pass these oplog entries to magic restore to perform a PIT
    // restore, so they will be reinserted and reflected in the final state of the data.
    ['e', 'f', 'g', 'h'].forEach(key => {
        assert.commandWorked(sourceDb.getCollection(excludedColl).insert({[key]: 1}));
        assert.commandWorked(sourceDb.getCollection(coll).insert({[key]: 1}));
    });
    assert.eq(sourceDb.getCollection(coll).find().toArray().length, 7);
    assert.eq(sourceDb.getCollection(excludedColl).find().toArray().length, 7);

    const checkpointTimestamp = magicRestoreUtils.getCheckpointTimestamp();
    let {lastOplogEntryTs, entriesAfterBackup} =
        magicRestoreUtils.getEntriesAfterBackup(sourcePrimary, namespacesToSkip);
    // entriesAfterBackup should not include oplog entries for the excluded namespaces, so we should
    // only have 4 entries for the 8 inserts we did.
    assert.eq(entriesAfterBackup.length, 4);

    magicRestoreUtils.copyFilesAndCloseBackup(namespacesToSkip);

    let expectedConfig = magicRestoreUtils.getExpectedConfig();
    // The new node will be allocated a new port by the test fixture.
    expectedConfig.members[0].host = getHostName() + ":" + (Number(sourcePrimary.port) + 2);
    let restoreConfiguration = {
        "nodeType": "replicaSet",
        "replicaSetConfig": expectedConfig,
        "maxCheckpointTs": checkpointTimestamp,
        // Restore to the timestamp of the last oplog entry on the source cluster.
        "pointInTimeTimestamp": lastOplogEntryTs,
        "collectionsToRestore": magicRestoreUtils.getCollectionsToRestore()
    };
    restoreConfiguration =
        magicRestoreUtils.appendRestoreToHigherTermThanIfNeeded(restoreConfiguration);

    magicRestoreUtils.writeObjsAndRunMagicRestore(
        restoreConfiguration, entriesAfterBackup, {"replSet": jsTestName()});

    // Start a new replica set fixture on the dbpath.
    const destinationCluster = new ReplSetTest({nodes: 1});
    destinationCluster.startSet({dbpath: magicRestoreUtils.getBackupDbPath(), noCleanData: true});

    const destPrimary = destinationCluster.getPrimary();

    // We skipped over db.coll2 so make sure there are no entries found for that namespace on the
    // restored node.
    assert.eq(destPrimary.getDB("db").getCollection(excludedColl).find().toArray().length, 0);

    magicRestoreUtils.postRestoreChecks({
        node: destPrimary,
        dbName: dbName,
        collName: coll,
        expectedOplogCountForNs: 7,
        opFilter: "i",
        expectedNumDocsSnapshot: 7
    });

    // The original node still maintains the history store, so point-in-time reads will succeed.
    let res = sourcePrimary.getDB("db").runCommand(
        {find: "coll", readConcern: {level: "snapshot", atClusterTime: snapshotTs}});
    assert.commandWorked(res);
    assert.eq(res.cursor.firstBatch.length, 3);

    let diff = DataConsistencyChecker.getDiff(
        sourcePrimary.getDB("db").getCollection("coll").find().sort({_id: 1}),
        destPrimary.getDB("db").getCollection("coll").find().sort({_id: 1}));

    assert.eq(diff,
              {docsWithDifferentContents: [], docsMissingOnFirst: [], docsMissingOnSecond: []});

    sourceCluster.stopSet();
    destinationCluster.stopSet();
}

// Make sure we handle selective restore properly with different values for directoryPerDb and
// wiredTigerDirectoryForIndexes.
runTest({});
runTest({directoryperdb: ""});
runTest({wiredTigerDirectoryForIndexes: ""});
runTest({wiredTigerDirectoryForIndexes: "", directoryperdb: ""});
