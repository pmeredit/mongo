/*
 * Tests a PIT sharded cluster magic restore with chunk migration and sharding renames.
 * The test does the following:
 *
 * - Starts a sharded cluster, shards the collection, moves chunks, inserts some initial data.
 * - Moves another chunk while the moveChunkHangAtStep3 failpoint is set.
 * - Creates the backup data files, copies data files to the restore dbpath.
 * - Unlock the moveChunkHangAtStep3 failpoint.
 * - Writes additional data that will be truncated by magic restore, since they occur after the
 *   checkpoint timestamp. However, these writes will still be reflected in the final state of the
 *   data due to the PIT restore.
 * - Computes dbHashes.
 * - Computes maxCheckpointTs, extends the cursors to maxCheckpointTs and closes the backup cursor.
 * - Writes a restore configuration object and the source oplog entries from after the checkpoint
 *   timestamp to a named pipe via the mongo shell.
 * - Stops all nodes.
 * - Starts the nodes with --magicRestore, which parses the restore configuration, inserts and
 *   applies the additional oplog entries, and exits cleanly.
 * - Restarts the nodes in the initial sharded cluster and asserts that the replica set config and
 *   data are as expected, that the dbHashes match and that the shard names were updated.
 *
 * @tags: [
 *     requires_persistence,
 *     requires_wiredtiger,
 *     incompatible_with_windows_tls
 * ]
 */
import {configureFailPoint} from "jstests/libs/fail_point_util.js";
import {funWithArgs} from "jstests/libs/parallel_shell_helpers.js";
import {ShardedMagicRestoreTest} from "jstests/libs/sharded_magic_restore_test.js";
import {ShardingTest} from "jstests/libs/shardingtest.js";
import {isConfigCommitted} from "jstests/replsets/rslib.js";

// TODO SERVER-87225: Enable fast count on validate when operations applied during a restore are
// counted correctly.
TestData.skipEnforceFastCountOnValidate = true;

function runTest(insertHigherTermOplogEntry) {
    jsTestLog("Running PIT magic restore with insertHigherTermOplogEntry: " +
              insertHigherTermOplogEntry);
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

    const dbName = "db";
    const coll = "coll";
    const fullNs = dbName + "." + coll;
    jsTestLog("Setting up sharded collection " + fullNs);
    assert(st.adminCommand({enableSharding: dbName, primaryShard: st.shard0.shardName}));
    assert(st.adminCommand({shardCollection: fullNs, key: {numForPartition: 1}}));

    // Split the collection into 3 chunks: [MinKey, 0), [0, 100), [100, MaxKey).
    assert(st.adminCommand({split: fullNs, middle: {numForPartition: 0}}));
    assert(st.adminCommand({split: fullNs, middle: {numForPartition: 100}}));
    assert(
        st.adminCommand({moveChunk: fullNs, find: {numForPartition: 50}, to: st.shard1.shardName}));
    assert(st.adminCommand(
        {moveChunk: fullNs, find: {numForPartition: 150}, to: st.shard1.shardName}));

    const db = st.getDB(dbName);
    jsTestLog("Inserting data to restore");  // This data will be reflected in the restored node.
    [-150, -50, 50, 150].forEach(
        val => { assert.commandWorked(db.getCollection(coll).insert({numForPartition: val})); });
    let expectedDocs = db.getCollection(coll).find().sort({numForPartition: 1}).toArray();
    assert.eq(expectedDocs.length, 4);

    const shardingRestoreTest = new ShardedMagicRestoreTest({
        st: st,
        pipeDir: MongoRunner.dataDir,
        insertHigherTermOplogEntry: insertHigherTermOplogEntry
    });

    // Pause the move chunk after the start of the cloning phase, so the move chunk is ongoing when
    // we takeCheckpointsAndOpenBackups below.
    const moveChunkHangShard1 = configureFailPoint(st.shard1, "moveChunkHangAtStep3");

    // Pause the destination shard before the bulk clone phase.
    const moveChunkHangShard0 = configureFailPoint(st.shard0, "migrateThreadHangAtStep3");

    const awaitResult = startParallelShell(
        funWithArgs(function(ns, toShardName) {
            // Move the chunk [100, MaxKey] back to shard0.
            assert.commandWorked(
                db.adminCommand({moveChunk: ns, find: {numForPartition: 150}, to: toShardName}));
        }, fullNs, st.shard0.shardName), st.s.port);

    moveChunkHangShard1.wait();
    moveChunkHangShard0.wait();

    jsTestLog("Taking checkpoints and opening backup cursors");
    shardingRestoreTest.takeCheckpointsAndOpenBackups();

    // Allow the moveChunk to finish and wait for delete on shard1.
    moveChunkHangShard1.off();
    moveChunkHangShard0.off();
    awaitResult();
    assert.soonNoExcept(() => st.rs1.getPrimary().getDB(dbName).getCollection(coll).count() == 1);

    // These documents will be truncated by magic restore, since they were written after the backup
    // cursor was opened. We will pass these oplog entries to magic restore to perform a PIT
    // restore, so they will be reinserted and reflected in the final state of the data.
    jsTestLog("Inserting data after backup cursor");
    [-151, -51, 51, 151].forEach(
        val => { assert.commandWorked(db.getCollection(coll).insert({numForPartition: val})); });
    expectedDocs = db.getCollection(coll).find().sort({numForPartition: 1}).toArray();
    assert.eq(expectedDocs.length, 8);

    jsTestLog("Getting backup cluster dbHashes");
    // expected DBs are admin, config and db
    shardingRestoreTest.storePreRestoreDbHashes();

    shardingRestoreTest.getShardRestoreTests().forEach((magicRestoreTest, idx) => {
        magicRestoreTest.rst.nodes.forEach((node) => {
            const isShard0 = (idx == 0);
            // First we inserted -150, -50, 50, 150 with those chunks:
            //      [MinKey, 0) on shard0
            //      [0, 100), [100, MaxKey) on shard1
            // so shard0 got 2 oplog entries (for the inserts of -150 and -50) and shard1 got 2
            // oplog entries (for the inserts of 50, 150).
            // Moving chunk [100, MaxKey] back to shard0 caused 150 to migrate to shard0, adding one
            // more fromMigrate oplog there.
            // Then we inserted -151, -51, 51, 151 so
            //      -151 and -51 to chunk [MinKey, 0) on shard0
            //      51 to chunk [0, 100) on shard1
            //      151 to chunk [100, MaxKey) on shard0
            // In total that gives 2 + 1 + 2 + 1 entries to shard0 and 2 + 1 on shard1.
            magicRestoreTest.assertOplogCountForNamespace(
                node, {ns: dbName + "." + coll, op: "i"}, isShard0 ? 6 : 3);

            let {entriesAfterBackup} = magicRestoreTest.getEntriesAfterBackup(node);
            // There might be operations after the backup from periodic jobs such as rangeDeletions
            // or ensureMajorityPrimaryAndScheduleDbTask and entries from moving the chunk,
            // so we filter those out for the comparison but still pass them into magic restore as
            // additional oplog entries to apply.
            const filteredEntriesAfterBackup =
                entriesAfterBackup.filter(elem => elem.op == "i" && elem.ns == fullNs);

            // On shard0: {"numForPartition": 150, "fromMigrate": true},
            // {"numForPartition": -151}, {"numForPartition": -51}, {"numForPartition": 151}
            // On shard1: {"numForPartition": 51}
            const expectedLength = isShard0 ? 4 : 1;
            assert.eq(filteredEntriesAfterBackup.length,
                      expectedLength,
                      `filteredEntriesAfterBackup = ${
                          tojson(filteredEntriesAfterBackup)} is not of length ${expectedLength}`);
        });
    });

    shardingRestoreTest.findMaxCheckpointTsAndExtendBackupCursors();
    shardingRestoreTest.setPointInTimeTimestamp();
    shardingRestoreTest.setUpShardingRenamesAndIdentityDocs();
    jsTestLog("Stopping all nodes");
    st.stop({noCleanData: true});

    jsTestLog("Running magic restore");
    shardingRestoreTest.runMagicRestore();

    jsTestLog("Starting config server restore");
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
            collName: coll,
            // We don't expect the config server to have data in db.coll.
            expectedOplogCountForNs: 0,
            opFilter: "i",
            expectedNumDocsSnapshot: 0,
        });

        let entries = node.getDB("config").getCollection("databases").find().toArray();
        assert.eq(entries.length, 1);
        for (const entry of entries) {
            assert.eq(entry["primary"], jsTestName() + "-dst-rs0");
        }

        entries = node.getDB("config").getCollection("shards").find().toArray();
        assert.eq(entries.length, 2);
        for (const entry of entries) {
            assert.includes(entry["_id"], jsTestName() + "-dst");
            assert.includes(entry["host"], jsTestName() + "-dst");
        }

        entries = node.getDB("config").getCollection("chunks").find().toArray();
        assert.eq(entries.length, 4);
        for (const entry of entries) {
            assert.includes(entry["shard"], jsTestName() + "-dst");
            for (const historyEntry of entry["history"]) {
                assert.includes(historyEntry["shard"], jsTestName() + "-dst");
            }
        }
    });

    shardingRestoreTest.getShardRestoreTests().forEach((magicRestoreTest, idx) => {
        const isShard0 = (idx == 0);
        jsTestLog("Starting restore shard " + idx);
        magicRestoreTest.rst.startSet({
            restart: true,
            dbpath: magicRestoreTest.getBackupDbPath(),
            noCleanData: true,
            shardsvr: "",
        });
        magicRestoreTest.rst.awaitNodesAgreeOnPrimary();
        // Make sure that all nodes have installed the config before moving on.
        let primary = magicRestoreTest.rst.getPrimary();
        magicRestoreTest.rst.waitForConfigReplication(primary);
        assert.soonNoExcept(() => isConfigCommitted(primary));

        magicRestoreTest.rst.nodes.forEach((node) => {
            node.setSecondaryOk();
            const restoredDocs =
                node.getDB(dbName).getCollection(coll).find().sort({numForPartition: 1}).toArray();
            // Of the 8 documents, 6 are on shard0 after the moveChunk.
            // In term of oplog entries, shard0 got 6 inserts and shard1 got 3 because one document
            // (150) is moved from shard1 to shard0 by the moveChunk.
            const expectedNumDocsSnapshot = isShard0 ? 6 : 2;
            assert.eq(restoredDocs.length, expectedNumDocsSnapshot);
            magicRestoreTest.postRestoreChecks({
                node: node,
                dbName: dbName,
                collName: coll,
                expectedOplogCountForNs: isShard0 ? 6 : 3,
                opFilter: "i",
                expectedNumDocsSnapshot: expectedNumDocsSnapshot,
            });
        });
    });

    jsTestLog("Getting restore cluster dbHashes");
    // Excluding admin.system.version, config.shards, config.actionlog, config.rangeDeletions,
    // cache.databases, cache.collections and cache.chunks.db.coll.
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
        `cache.chunks.${dbName}.${coll}`,
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

    jsTestLog("Checking sharding renames on the config shard");
    primary = configUtils.rst.getPrimary();
    const configDB = primary.getDB("config");
    const shards = configDB.getCollection("shards").find().sort({"_id": 1}).toArray();
    assert.eq(shards.length, 2);
    const shard0Name = jsTestName() + "-dst-rs0";
    assert.eq(shards[0]._id, shard0Name);
    assert.eq(shards[1]._id, jsTestName() + "-dst-rs1");

    const chunks = configDB.getCollection("chunks").find().sort({"shard": 1}).toArray();
    assert.eq(chunks.length, 4);
    for (let chunkId = 0; chunkId < 4; chunkId++) {
        const shardName = jsTestName() + `-dst-rs${chunkId == 3 ? 1 : 0}`;
        assert.eq(chunks[chunkId].shard, shardName);
        assert.eq(chunks[chunkId].history.length, 1);
        assert.eq(chunks[chunkId].history[0].shard, shardName);
    }

    const databases = configDB.getCollection("databases").find().toArray();
    assert.eq(databases.length, 1);
    assert.eq(databases[0].primary, shard0Name);

    assert.eq(configDB.getCollection("migrationCoordinators").find().toArray().length, 0);
    assert.eq(configDB.getCollection("migrationRecipients").find().toArray().length, 0);
    assert.eq(configDB.getCollection("rangeDeletions").find().toArray().length, 0);

    let shardIdentity = primary.getDB("admin")
                            .getCollection("system.version")
                            .find({"_id": "shardIdentity"})
                            .toArray();
    assert.eq(shardIdentity.length, 1);
    shardIdentity = shardIdentity[0];
    const configShardName = jsTestName() + "-configRS";
    assert(shardIdentity.configsvrConnectionString.includes(configShardName),
           `${shardIdentity.configsvrConnectionString} does not contain ${configShardName}`);

    shardingRestoreTest.getShardRestoreTests().forEach((magicRestoreTest, idx) => {
        jsTestLog("Checking sharding renames on shard " + idx);

        const primary = magicRestoreTest.rst.getPrimary();
        const configDB = primary.getDB("config");
        assert.eq(configDB.getCollection("migrationCoordinators").find().toArray().length, 0);
        assert.eq(configDB.getCollection("migrationRecipients").find().toArray().length, 0);
        assert.eq(configDB.getCollection("rangeDeletions").find().toArray().length, 0);

        let shardIdentity = primary.getDB("admin")
                                .getCollection("system.version")
                                .find({"_id": "shardIdentity"})
                                .toArray();
        assert.eq(shardIdentity.length, 1);
        shardIdentity = shardIdentity[0];
        assert(shardIdentity.configsvrConnectionString.includes(configShardName),
               `${shardIdentity.configsvrConnectionString} does not contain ${configShardName}`);
    });

    jsTestLog("Stopping restore nodes");
    shardingRestoreTest.getShardRestoreTests().forEach(
        (magicRestoreTest) => { magicRestoreTest.rst.stopSet(); });
    configUtils.rst.stopSet();
}

// Run PIT restore twice, with one run performing a no-op oplog entry insert with a higher term.
// This affects the stable timestamp on magic restore node shutdown.
runTest(false /* insertHigherTermOplogEntry */);
runTest(true /* insertHigherTermOplogEntry */);
