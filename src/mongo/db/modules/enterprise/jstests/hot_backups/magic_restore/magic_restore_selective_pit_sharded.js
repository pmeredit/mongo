/*
 * Tests a selective magic sharded cluster PIT restore. The test does the following:
 *
 * - Starts a sharded cluster, shards the collection, moves chunks, inserts some initial data.
 * - Creates the backup data files, copies data files to the restore dbpath. Since this is a
 *   selective restore, we pass in a list of collections to copy over, while we skip the
 *   unrestored collections.
 * - Writes additional data that will be truncated by magic restore, since they occur after the
 *   checkpoint timestamp. However, these writes will still be reflected in the final state of
 *   the data due to the PIT restore.
 * - Computes dbHashes of collections pre-restore.
 * - Computes maxCheckpointTs, extends the cursors to maxCheckpointTs and closes the backup cursor.
 * - Writes a restore configuration object and the source oplog entries from after the checkpoint
 *   timestamp to a named pipe via the mongo shell.
 * - Stops all nodes.
 * - Starts the nodes with --magicRestore and --restore. --magicRestore parses the restore
 *   configuration, inserts and applies the additional oplog entries, and exits cleanly, while
 *   --restore signals we're performing a selective restore.
 * - Restarts the nodes in the initial sharded cluster and asserts that the replica set config and
 *   data are as expected, and that the dbHashes match.
 *
 * @tags: [
 *     requires_persistence,
 *     requires_wiredtiger,
 *     incompatible_with_windows_tls
 * ]
 */

import {FeatureFlagUtil} from "jstests/libs/feature_flag_util.js";
import {ShardedMagicRestoreTest} from "jstests/libs/sharded_magic_restore_test.js";
import {ShardingTest} from "jstests/libs/shardingtest.js";
import {isConfigCommitted} from "jstests/replsets/rslib.js";

// TODO SERVER-87225: Enable fast count on validate when operations applied during a restore are
// counted correctly.
TestData.skipEnforceFastCountOnValidate = true;

function runTest(nodeOptionsArg) {
    jsTestLog("Running selective PIT magic restore with nodeOptionsArg: " + tojson(nodeOptionsArg));
    // Setting priorities on the second node because assertConfigIsCorrect checks terms:
    // With only 2 nodes, it might happen that both nodes try to run for primary at the same time
    // and vote for themselves, which would increase the term.
    const st = new ShardingTest({
        shards: {
            rs0: {nodes: [{}, {rsConfig: {priority: 0}}]},
            rs1: {nodes: [{}, {rsConfig: {priority: 0}}]}
        },
        mongos: 1,
        config: [{}, {rsConfig: {priority: 0}}]
    });

    // 'featureFlagTrackUnshardedCollectionsUponCreation' will store unsharded collection metadata
    // in the config server's 'config.collections' collection. As we check the contents of that
    // collection before and after selective restore, we adjust the expected size of the collection
    // based on the feature flag.
    const trackUnshardedCollections =
        FeatureFlagUtil.isEnabled(st.s, "TrackUnshardedCollectionsUponCreation");

    const dbName = "db";
    const collToRestore = "shardedCollToRestore";
    const collToSkip = "shardedCollToSkip";
    const unshardedColl = "unshardedColl";

    const ns0 = dbName + "." + collToRestore;
    const ns1 = dbName + "." + collToSkip;
    const ns2 = dbName + "." + unshardedColl;
    jsTestLog("Sharding collections " + ns0 + " and " + ns1);
    assert(st.adminCommand({enableSharding: dbName, primaryShard: st.shard0.shardName}));
    assert(st.adminCommand({shardCollection: ns0, key: {numForPartition: 1}}));
    assert(st.adminCommand({shardCollection: ns1, key: {numForPartition: 1}}));

    const db = st.getDB(dbName);

    // Split the collections into 2 chunks: [MinKey, 0), [0, MaxKey).
    assert(st.adminCommand({split: ns0, middle: {numForPartition: 0}}));
    assert(st.adminCommand({moveChunk: ns0, find: {numForPartition: 50}, to: st.shard1.shardName}));
    assert(st.adminCommand({split: ns1, middle: {numForPartition: 0}}));
    assert(st.adminCommand({moveChunk: ns1, find: {numForPartition: 50}, to: st.shard1.shardName}));

    jsTestLog("Inserting data to restore");  // This data will be reflected in the restored node.
    [-150, -50, 50, 150].forEach(val => {
        assert.commandWorked(db.getCollection(collToRestore).insert({numForPartition: val}));
    });
    let expectedDocs = db.getCollection(collToRestore).find().toArray();
    assert.eq(expectedDocs.length, 4);

    [-1000, -500, 500, 1000].forEach(val => {
        assert.commandWorked(db.getCollection(collToSkip).insert({numForPartition: val}));
    });
    expectedDocs = db.getCollection(collToSkip).find().toArray();
    assert.eq(expectedDocs.length, 4);

    [1, 2, 3, 4].forEach(val => {
        assert.commandWorked(db.getCollection(unshardedColl).insert({numForPartition: val}));
    });
    expectedDocs = db.getCollection(unshardedColl).find().toArray();
    assert.eq(expectedDocs.length, 4);

    const shardingRestoreTest = new ShardedMagicRestoreTest({
        st: st,
        pipeDir: MongoRunner.dataDir,
    });

    jsTestLog("Taking checkpoints and opening backup cursors");
    shardingRestoreTest.takeCheckpointsAndOpenBackups();

    // These documents will be truncated by magic restore, since they were written after the backup
    // cursor was opened. We will pass these oplog entries to magic restore to perform a PIT
    // restore, so they will be reinserted and reflected in the final state of the data.
    jsTestLog("Inserting data after backup cursor");
    [-151, -51, 51, 151].forEach(val => {
        assert.commandWorked(db.getCollection(collToRestore).insert({numForPartition: val}));
    });
    expectedDocs = db.getCollection(collToRestore).find().toArray();
    assert.eq(expectedDocs.length, 8);

    [-1001, -501, 501, 1001].forEach(val => {
        assert.commandWorked(db.getCollection(collToSkip).insert({numForPartition: val}));
    });
    expectedDocs = db.getCollection(collToSkip).find().toArray();
    assert.eq(expectedDocs.length, 8);

    [5, 6, 7, 8].forEach(val => {
        assert.commandWorked(db.getCollection(unshardedColl).insert({numForPartition: val}));
    });
    expectedDocs = db.getCollection(unshardedColl).find().toArray();
    assert.eq(expectedDocs.length, 8);

    jsTestLog("Getting backup cluster dbHashes");
    // The only expected databases are admin, config and db.
    const dbs =
        assert.commandWorked(st.s.adminCommand({listDatabases: 1, nameOnly: true})).databases;
    assert.eq(["admin", "config", "db"], dbs.map(obj => obj.name).sort());
    shardingRestoreTest.storePreRestoreDbHashes();

    shardingRestoreTest.getShardRestoreTests().forEach((magicRestoreTest, idx) => {
        const isPrimaryShard = idx == 0;

        magicRestoreTest.rst.nodes.forEach((node) => {
            // We inserted 8 documents per namespace and have 2 shards, so 4 per namespace per
            // shard.
            magicRestoreTest.assertOplogCountForNamespace(node, {ns: ns0, op: "i"}, 4);
            magicRestoreTest.assertOplogCountForNamespace(node, {ns: ns1, op: "i"}, 4);
            const expectedDocsUnshardedColl = isPrimaryShard ? 8 : 0;
            magicRestoreTest.assertOplogCountForNamespace(
                node, {ns: ns2, op: "i"}, expectedDocsUnshardedColl);

            // Note that entriesAfterBackup will contain oplog entries referencing unrestored
            // collections. The server will ignore these oplog entries during application for
            // selective restores.
            let {entriesAfterBackup} = magicRestoreTest.getEntriesAfterBackup(node);
            // There might be operations after the backup from periodic jobs such as rangeDeletions
            // or ensureMajorityPrimaryAndScheduleDbTask, so we filter those out for the comparison
            // but still pass them into magic restore as additional oplog entries to apply.
            const filteredEntriesAfterBackup = entriesAfterBackup.filter(
                elem => (elem.ns != "config.rangeDeletions" &&
                         elem.o != "ensureMajorityPrimaryAndScheduleDbTask"));

            // The unsharded collection lives on the primary shard.
            const expectedEntriesAfterBackup = isPrimaryShard ? 8 : 4;
            assert.eq(filteredEntriesAfterBackup.length,
                      expectedEntriesAfterBackup,
                      `filteredEntriesAfterBackup = ${
                          tojson(filteredEntriesAfterBackup)} is not of length ${
                          expectedEntriesAfterBackup}.`);
        });
    });

    // Prior to restore, the config server 'config.collections' collection should contain the two
    // sharded user collections as well as the internal sessions collection. If
    // featureFlagBalanceUnshardedCollections is enabled, this collection will also contain metadata
    // from the unsharded collection.
    shardingRestoreTest.getConfigRestoreTest().rst.nodes.forEach((node) => {
        let collections = node.getDB("config").getCollection("collections").find().toArray();
        const expectedCollectionLength = trackUnshardedCollections ? 4 : 3;
        assert.eq(collections.length,
                  expectedCollectionLength,
                  `config.collections has ${collections.length} elements, not ${
                      expectedCollectionLength}: ${tojson(collections)}`);
        assert.eq(collections[0]._id, "config.system.sessions");
        assert.eq(collections[1]._id, ns0);
        assert.eq(collections[2]._id, ns1);
        if (trackUnshardedCollections) {
            assert.eq(collections[3]._id, ns2);
        }
    });

    shardingRestoreTest.findMaxCheckpointTsAndExtendBackupCursors([ns0] /* collectionsToRestore */);
    shardingRestoreTest.setPointInTimeTimestamp();
    jsTestLog("Stopping all nodes");
    st.stop({noCleanData: true});

    jsTestLog("Running magic restore");
    shardingRestoreTest.runMagicRestore();

    jsTestLog("Starting config server after restore");
    const configUtils = shardingRestoreTest.getConfigRestoreTest();
    configUtils.rst.startSet(
        {restart: true, dbpath: configUtils.getBackupDbPath(), noCleanData: true, configsvr: ""});

    configUtils.rst.awaitNodesAgreeOnPrimary();
    // Make sure that all nodes have installed the config before moving on.
    let primary = configUtils.rst.getPrimary();
    configUtils.rst.waitForConfigReplication(primary);
    assert.soonNoExcept(() => isConfigCommitted(primary));

    // Check each node in the config server replica set.
    configUtils.rst.nodes.forEach((node) => {
        node.setSecondaryOk();
        // Even though all nodes are consistent up to a particular point in time (the maximum
        // value of these oplog entries), each individual node's stable timestamp will be the
        // latest oplog entry in its oplog.
        configUtils.postRestoreChecks({
            node: node,
            dbName: dbName,
            collName: collToRestore,
            // We don't expect the config server to have data in db.collToRestore.
            expectedOplogCountForNs: 0,
            opFilter: "i",
            expectedNumDocsSnapshot: 0,
        });

        // Magic restore ran the '_configsvrRunRestore' command, which removed 'db.collToSkip' and
        // 'db.unshardedColl' from metadata collections.
        let collections = node.getDB("config").getCollection("collections").find().toArray();
        assert.eq(
            collections.length,
            2,
            `config.collections has ${collections.length} elements, not 2: ${tojson(collections)}`);
        assert.eq(collections[0]._id, "config.system.sessions");
        assert.eq(collections[1]._id, ns0);
    });

    shardingRestoreTest.getShardRestoreTests().forEach((magicRestoreTest, idx) => {
        jsTestLog("Starting restore shard " + idx);
        magicRestoreTest.rst.startSet({
            restart: true,
            dbpath: magicRestoreTest.getBackupDbPath(),
            noCleanData: true,
            shardsvr: "",
        });
        magicRestoreTest.rst.awaitNodesAgreeOnPrimary();
        // Make sure that all nodes have installed the config before moving on.
        const primary = magicRestoreTest.rst.getPrimary();
        magicRestoreTest.rst.waitForConfigReplication(primary);
        assert.soonNoExcept(() => isConfigCommitted(primary));

        magicRestoreTest.rst.nodes.forEach((node) => {
            node.setSecondaryOk();
            const restoredDocs = node.getDB(dbName)
                                     .getCollection(collToRestore)
                                     .find()
                                     .sort({numForPartition: 1})
                                     .toArray();
            // Each shard should have half the total number of documents.
            assert.eq(restoredDocs.length, expectedDocs.length / 2);
            magicRestoreTest.postRestoreChecks({
                node: node,
                dbName: dbName,
                collName: collToRestore,
                expectedOplogCountForNs: 4,
                opFilter: "i",
                expectedNumDocsSnapshot: 4,
            });
        });
    });

    jsTestLog("Getting restore cluster dbHashes");
    const excludedCollections = [
        "system.version",
        "shards",
        "actionlog",
        "clusterParameters",
        "rangeDeletions",
        "mongos",
        "cache.databases",
        "cache.collections",
        "databases",  // Renaming shards affects the "primary" field of documents in that collection
        "chunks",     // Renaming shards affects the "shard" field of documents in that collection
        "cache.chunks.config.system.sessions",
        `cache.chunks.${dbName}.${collToRestore}`,
        `cache.chunks.${dbName}.${collToSkip}`,
        `cache.chunks.${dbName}.${unshardedColl}`,
        // As we've omitted collToSkip, the hashes for 'config.collections' will be different.
        // The contents have been checked before and after restore elsewhere.
        "collections",
        "shardedCollToSkip",
        "unshardedColl",
        "placementHistory"
    ];
    shardingRestoreTest.checkPostRestoreDbHashes(excludedCollections);

    // config.placementHistory is dropped during the restore procedure.
    assert.eq(configUtils.rst.getPrimary()
                  .getDB("config")
                  .getCollection("placementHistory")
                  .find()
                  .toArray(),
              0);

    jsTestLog("Stopping restore nodes");
    shardingRestoreTest.getShardRestoreTests().forEach(
        (magicRestoreTest) => { magicRestoreTest.rst.stopSet(); });
    configUtils.rst.stopSet();
}

// Make sure we handle selective restore properly with different values for directoryPerDb and
// wiredTigerDirectoryForIndexes.
runTest({});
runTest({directoryperdb: ""});
runTest({wiredTigerDirectoryForIndexes: ""});
runTest({wiredTigerDirectoryForIndexes: "", directoryperdb: ""});
