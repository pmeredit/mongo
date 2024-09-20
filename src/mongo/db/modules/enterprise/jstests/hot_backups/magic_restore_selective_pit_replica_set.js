/*
 * Tests a PIT replica set selective restore with magic restore. The test does the following:
 *
 * - Starts a replica set, inserts some initial data to two collections, and opens a backup cursor.
 * - Writes additional data that will be truncated by magic restore, since the writes occur after
 *   the checkpoint timestamp.
 * - Copies data files from the first collection to the restore dbpath and closes the backup cursor.
 *   The data files for the second collection are skipped over, as we pass in a list of namespaces
 *   to restore.
 * - Writes a restore configuration object and the source oplog entries from after the checkpoint
 *   timestamp to a named pipe via the mongo shell.
 * - Starts a mongod with --magicRestore and --restore that parses the restore configuration,
 *   inserts and applies the additional oplog entries, and exits cleanly. The --restore signals that
 *   this is a selective restore.
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
    const collToRestore = "collToRestore";
    const excludedColl = "collToSkip";
    const collectionsToRestore = ["db.collToRestore"];

    const sourceDb = sourcePrimary.getDB(dbName);
    // Insert some data to restore. This data will be reflected in the restored node.
    ['a', 'b', 'c'].forEach(key => {
        assert.commandWorked(sourceDb.getCollection(excludedColl).insert({[key]: 1}));
        assert.commandWorked(sourceDb.getCollection(collToRestore).insert({[key]: 1}));
    });

    // This timestamp will be used for a snapshot read.
    const snapshotTs = assert.commandWorked(sourcePrimary.adminCommand({replSetGetStatus: 1}))
                           .optimes.lastCommittedOpTime.ts;

    const magicRestoreTest =
        new MagicRestoreTest({rst: sourceCluster, pipeDir: MongoRunner.dataDir});

    magicRestoreTest.takeCheckpointAndOpenBackup();

    // These documents will be truncated by magic restore, since they were written after the backup
    // cursor was opened. We will pass these oplog entries to magic restore to perform a PIT
    // restore, so they will be reinserted and reflected in the final state of the data.
    ['e', 'f', 'g', 'h'].forEach(key => {
        assert.commandWorked(sourceDb.getCollection(excludedColl).insert({[key]: 1}));
        assert.commandWorked(sourceDb.getCollection(collToRestore).insert({[key]: 1}));
    });
    assert.eq(sourceDb.getCollection(collToRestore).find().toArray().length, 7);
    assert.eq(sourceDb.getCollection(excludedColl).find().toArray().length, 7);

    const checkpointTimestamp = magicRestoreTest.getCheckpointTimestamp();
    // Note that entriesAfterBackup will contain oplog entries referencing unrestored collections.
    // The server will ignore these oplog entries during application for selective restores.
    let {lastOplogEntryTs, entriesAfterBackup} =
        magicRestoreTest.getEntriesAfterBackup(sourcePrimary);
    assert.eq(entriesAfterBackup.length, 8);

    magicRestoreTest.copyFilesAndCloseBackup(collectionsToRestore);

    let expectedConfig = magicRestoreTest.getExpectedConfig();
    // The new node will be allocated a new port by the test fixture.
    expectedConfig.members[0].host = getHostName() + ":" + (Number(sourcePrimary.port) + 2);
    let restoreConfiguration = {
        "nodeType": "replicaSet",
        "replicaSetConfig": expectedConfig,
        "maxCheckpointTs": checkpointTimestamp,
        // Restore to the timestamp of the last oplog entry on the source cluster.
        "pointInTimeTimestamp": lastOplogEntryTs
    };
    restoreConfiguration =
        magicRestoreTest.appendRestoreToHigherTermThanIfNeeded(restoreConfiguration);

    magicRestoreTest.writeObjsAndRunMagicRestore(
        restoreConfiguration, entriesAfterBackup, {"replSet": jsTestName(), "restore": ''});

    // Start a new replica set fixture on the dbpath.
    const destinationCluster = new ReplSetTest({nodes: 1});
    destinationCluster.startSet({dbpath: magicRestoreTest.getBackupDbPath(), noCleanData: true});

    const destPrimary = destinationCluster.getPrimary();

    // We skipped over db.collToSkip so make sure the collection does not exist on the restored
    // node. WiredTiger will also remove the unrestored collection from the catalog on
    // startup.
    const collections = assert
                            .commandWorked(destPrimary.getDB(dbName).runCommand(
                                {listCollections: 1, nameOnly: true}))
                            .cursor.firstBatch;
    assert(collections.length === 1 && collections[0].name === collToRestore);

    magicRestoreTest.postRestoreChecks({
        node: destPrimary,
        dbName: dbName,
        collName: collToRestore,
        expectedOplogCountForNs: 7,
        opFilter: "i",
        expectedNumDocsSnapshot: 7
    });

    // The original node still maintains the history store, so point-in-time reads will succeed.
    let res = sourcePrimary.getDB("db").runCommand(
        {find: collToRestore, readConcern: {level: "snapshot", atClusterTime: snapshotTs}});
    assert.commandWorked(res);
    assert.eq(res.cursor.firstBatch.length, 3);

    let diff = DataConsistencyChecker.getDiff(
        sourcePrimary.getDB("db").getCollection(collToRestore).find().sort({_id: 1}),
        destPrimary.getDB("db").getCollection(collToRestore).find().sort({_id: 1}));

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
