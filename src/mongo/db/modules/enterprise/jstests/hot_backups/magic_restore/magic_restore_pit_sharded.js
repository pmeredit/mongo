/*
 * Tests a PIT sharded cluster magic restore with sharding renames. The test does the following:
 *
 * - Starts a sharded cluster, shards the collection, moves chunks, inserts some initial data.
 * - Transitions between dedicated config server and non-dedicated config server mode.
 * - Creates the backup data files, copies data files to the restore dbpath.
 * - Writes additional data (including more config server transitions) that will be truncated by
 * magic restore, since they occur after the checkpoint timestamp. However, these writes will still
 * be reflected in the final state of the data due to the PIT restore.
 * - Computes dbHashes.
 * - Computes maxCheckpointTs, extends the cursors to maxCheckpointTs and closes the backup cursor.
 * - Writes a restore configuration object and the source oplog entries from after the checkpoint
 *   timestamp to a named pipe via the mongo shell.
 * - Stops all nodes.
 * - Starts the nodes with --magicRestore, which parses the restore configuration, inserts and
 * applies the additional oplog entries, and exits cleanly.
 * - Restarts the nodes in the initial sharded cluster and asserts that the replica set config and
 * data are as expected, and that the dbHashes match.
 *
 * @tags: [
 *     requires_persistence,
 *     requires_wiredtiger,
 *     incompatible_with_windows_tls
 * ]
 */

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

    // Initially we are in dedicated config server mode so we should only have 2 shard entries.
    let shardEntries = st.s.getDB("config").getCollection("shards").find().toArray();
    assert.eq(shardEntries.length, 2);

    // Transition out of dedicated config server mode. This command only returns ok:1 so we don't
    // have anything to check from the return object.
    assert.commandWorked(st.s.adminCommand({transitionFromDedicatedConfigServer: 1}));

    shardEntries = st.s.getDB("config").getCollection("shards").find().toArray();
    // We should now have a third entry for the config server shard.
    assert.eq(shardEntries.length, 3);
    assert.eq(shardEntries[2]._id, "config");
    assert(!shardEntries[2].draining);

    const dbName = "db";
    const coll = "coll";
    const fullNs = dbName + "." + coll;
    jsTestLog("Setting up sharded collection " + fullNs);
    assert(st.adminCommand({enableSharding: dbName, primaryShard: st.shard0.shardName}));
    assert(st.adminCommand({shardCollection: fullNs, key: {numForPartition: 1}}));

    // Split the collection into 2 chunks: [MinKey, 0), [0, MaxKey).
    assert(st.adminCommand({split: fullNs, middle: {numForPartition: 0}}));
    assert(
        st.adminCommand({moveChunk: fullNs, find: {numForPartition: 50}, to: st.shard1.shardName}));

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

    jsTestLog("Taking checkpoints and opening backup cursors");
    shardingRestoreTest.takeCheckpointsAndOpenBackups();

    // On first run the state is 'started' until some databases are dropped and then 'completed' on
    // the next one.
    let removeRes = assert.commandWorked(st.s.adminCommand({transitionToDedicatedConfigServer: 1}));
    assert.eq(removeRes.state, "started");

    shardEntries = st.s.getDB("config").getCollection("shards").find().toArray();
    // We should still have the third entry now with a draining field.
    assert.eq(shardEntries.length, 3);
    assert.eq(shardEntries[2]._id, "config");
    assert.eq(shardEntries[2].draining, true);

    removeRes = assert.commandWorked(st.s.adminCommand({transitionToDedicatedConfigServer: 1}));
    assert.eq(removeRes.state, "completed");

    shardEntries = st.s.getDB("config").getCollection("shards").find().toArray();
    // We have now transitioned back to dedicated config and should only have 2 entries.
    assert.eq(shardEntries.length, 2);

    // Running it again after it is completed causes a failure.
    assert.commandFailedWithCode(st.s.adminCommand({transitionToDedicatedConfigServer: 1}),
                                 ErrorCodes.ShardNotFound);

    shardEntries = st.s.getDB("config").getCollection("shards").find().toArray();
    // We should still have just 2 entries.
    assert.eq(shardEntries.length, 2);

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

    shardingRestoreTest.getShardRestoreTests().forEach((magicRestoreTest) => {
        magicRestoreTest.rst.nodes.forEach((node) => {
            // We inserted 8 documents and have 2 shards, so 4 per shard.
            magicRestoreTest.assertOplogCountForNamespace(node, {ns: fullNs, op: "i"}, 4);

            let {entriesAfterBackup} = magicRestoreTest.getEntriesAfterBackup(node);
            // There might be operations after the backup from periodic jobs such as rangeDeletions
            // or ensureMajorityPrimaryAndScheduleDbTask, so we filter those out for the comparison
            // but still pass them into magic restore as additional oplog entries to apply.
            const filteredEntriesAfterBackup = entriesAfterBackup.filter(
                elem => (elem.ns != "config.rangeDeletions" &&
                         elem.o.msg != "ensureMajorityPrimaryAndScheduleDbTask"));

            // Includes the 2 addOrRemoveShardInProgress entries generated by
            // transitionToDedicatedConfigServer.
            assert.eq(filteredEntriesAfterBackup.length,
                      4,
                      `filteredEntriesAfterBackup = ${
                          tojson(filteredEntriesAfterBackup)} is not of length 4`);
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
    // We should be in dedicated config server mode and only have 2 entries since we did the
    // transition during PIT restore.
    shardEntries = primary.getDB("config").getCollection("shards").find().toArray();
    assert.eq(shardEntries.length, 2);

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
        assert.eq(entries.length, 3);
        for (const entry of entries) {
            assert.includes(entry["shard"], jsTestName() + "-dst");
            for (const historyEntry of entry["history"]) {
                assert.includes(historyEntry["shard"], jsTestName() + "-dst");
            }
        }
    });

    shardingRestoreTest.getShardRestoreTests().forEach((magicRestoreTest, idx) => {
        jsTestLog("Starting restore shard " + idx);
        magicRestoreTest.rst.name = magicRestoreTest.rst.name.replace("-rs", "-dst-rs");
        magicRestoreTest.rst.startSet({
            restart: true,
            dbpath: magicRestoreTest.getBackupDbPath(),
            noCleanData: true,
            shardsvr: "",
            replSet: magicRestoreTest.rst.name
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
            // Each shard should have half the total number of documents.
            assert.eq(restoredDocs.length, expectedDocs.length / 2);
            magicRestoreTest.postRestoreChecks({
                node: node,
                dbName: dbName,
                collName: coll,
                expectedOplogCountForNs: 4,
                opFilter: "i",
                expectedNumDocsSnapshot: 4,
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

    jsTestLog("Stopping restore nodes");
    shardingRestoreTest.getShardRestoreTests().forEach(
        (magicRestoreTest) => { magicRestoreTest.rst.stopSet(); });
    configUtils.rst.stopSet();
}

// Run PIT restore twice, with one run performing a no-op oplog entry insert with a higher term.
// This affects the stable timestamp on magic restore node shutdown.
runTest(false /* insertHigherTermOplogEntry */);
runTest(true /* insertHigherTermOplogEntry */);
