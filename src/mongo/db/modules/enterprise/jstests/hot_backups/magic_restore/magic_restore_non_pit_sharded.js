/*
 * Tests a non-PIT sharded cluster restore with magic restore. The test does the following:
 *
 * - Starts a sharded cluster, shards the collection, moves chunks, inserts some initial data.
 * - Transitions between dedicated config server and non-dedicated config server mode.
 * - Creates the backup data files, copies data files to the restore dbpath, computes dbHashes.
 * - Computes maxCheckpointTs, extends the cursors to maxCheckpointTs and closes the backup cursor.
 * - Writes a restore configuration object to a named pipe via the mongo shell.
 * - Stops all nodes.
 * - Starts the nodes with --magicRestore that parses the restore configuration and exits cleanly.
 * - Restarts the nodes in the initial sharded cluster and asserts that the replica set config and
 * data are as expected, and that the dbHashes match.
 *
 * @tags: [
 *     requires_persistence,
 *     requires_wiredtiger
 * ]
 */

import {ShardedMagicRestoreTest} from "jstests/libs/magic_restore_test.js";
import {ShardingTest} from "jstests/libs/shardingtest.js";
import {isConfigCommitted} from "jstests/replsets/rslib.js";

jsTestLog("Temporarily skipping test.");
quit();

// TODO SERVER-86034: Run on Windows machines once named pipe related failures are resolved.
if (_isWindows()) {
    jsTestLog("Temporarily skipping test for Windows variants. See SERVER-86034.");
    quit();
}

function runTest(insertHigherTermOplogEntry) {
    jsTestLog("Running non-PIT magic restore with insertHigherTermOplogEntry: " +
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

    // TODO SERVER-91950 Enable this testcase after the cluster parameters are restored properly.
    // Transition out of dedicated config server mode. This command only returns ok:1 so we don't
    // have anything to check from the return object.
    // assert.commandWorked(st.s.adminCommand({transitionFromDedicatedConfigServer: 1}));

    // shardEntries = st.s.getDB("config").getCollection("shards").find().toArray();
    // We should now have a third entry for the config server shard.
    // assert.eq(shardEntries.length, 3);
    // assert.eq(shardEntries[2]._id, "config");
    // assert(!shardEntries[2].draining);

    // Get back to dedicated config mode.

    // On first run the state is 'started' until some databases are dropped and then 'completed' on
    // the next one.
    // let removeRes = assert.commandWorked(st.s.adminCommand({transitionToDedicatedConfigServer:
    // 1})); assert.eq(removeRes.state, "started");

    // shardEntries = st.s.getDB("config").getCollection("shards").find().toArray();
    // We should still have the third entry now with a draining field.
    // assert.eq(shardEntries.length, 3);
    // assert.eq(shardEntries[2]._id, "config");
    // assert.eq(shardEntries[2].draining, true);
    // TODO SERVER-91950 Enable this testcase after the cluster parameters are restored properly.

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
    const expectedDocs = db.getCollection(coll).find().sort({numForPartition: 1}).toArray();
    assert.eq(expectedDocs.length, 4);

    const shardingRestoreTest = new ShardedMagicRestoreTest({
        st: st,
        pipeDir: MongoRunner.dataDir,
        insertHigherTermOplogEntry: insertHigherTermOplogEntry
    });

    jsTestLog("Taking checkpoints and opening backup cursors");
    shardingRestoreTest.takeCheckpointsAndOpenBackups();

    jsTestLog("Getting backup cluster dbHashes");
    // expected DBs are admin, config and db
    shardingRestoreTest.storePreRestoreDbHashes();

    // TODO SERVER-91950 Enable this testcase after the cluster parameters are restored properly.
    // Finish transition to dedicated config server after backup cursor has been opened.
    // removeRes = assert.commandWorked(st.s.adminCommand({transitionToDedicatedConfigServer: 1}));
    // assert.eq(removeRes.state, "completed");

    // shardEntries = st.s.getDB("config").getCollection("shards").find().toArray();
    // // We have now transitioned back to dedicated config and should only have 2 entries.
    // assert.eq(shardEntries.length, 2);
    // TODO SERVER-91950 Enable this testcase after the cluster parameters are restored properly.

    // These documents will be truncated by magic restore, since they were written after the backup
    // cursor was opened.
    jsTestLog("Inserting data after backup cursor");
    [-151, -51, 51, 151].forEach(
        val => { assert.commandWorked(db.getCollection(coll).insert({numForPartition: val})); });
    assert.eq(db.getCollection(coll).find().toArray().length, 8);

    shardingRestoreTest.getShardRestoreTests().forEach((magicRestoreUtil) => {
        magicRestoreUtil.rst.nodes.forEach((node) => {
            magicRestoreUtil.assertOplogCountForNamespace(
                node, {ns: dbName + "." + coll, op: "i"}, 4);
            let {entriesAfterBackup} = magicRestoreUtil.getEntriesAfterBackup(node);

            // There might be rangeDeletions ops after the backup, or a
            // ensureMajorityPrimaryAndScheduleDbTask too, filtering those out.
            entriesAfterBackup = entriesAfterBackup.filter(
                elem => (elem.ns != "config.rangeDeletions" &&
                         elem.o != "ensureMajorityPrimaryAndScheduleDbTask"));

            // TODO SERVER-91950 Change this back to 4 entries.
            // Includes the 2 addOrRemoveShardInProgress entries generated by
            // transitionFromDedicatedConfigServer.
            assert.eq(entriesAfterBackup.length,
                      2,
                      `entriesAfterBackup = ${tojson(entriesAfterBackup)} is not of length 2`);
        });
    });

    shardingRestoreTest.findMaxCheckpointTsAndExtendBackupCursors();

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

    // TODO SERVER-91950 Enable this testcase after the cluster parameters are restored properly.
    // We should still have the third entry now with a draining field since we finished the shard
    // transition after the backup cursor was opened so it should be unfinished.
    // shardEntries = primary.getDB("config").getCollection("shards").find().toArray();
    // assert.eq(shardEntries.length, 3);
    // assert.eq(shardEntries[2]._id, "config");
    // assert.eq(shardEntries[2].draining, true);
    // TODO SERVER-91950 Enable this testcase after the cluster parameters are restored properly.

    // Check each node in the config server replica set.
    configUtils.rst.nodes.forEach((node) => {
        configUtils.postRestoreChecks({
            node: node,
            dbName: dbName,
            collName: coll,
            // We don't expect the config server to have data in db.coll.
            expectedOplogCountForNs: 0,
            opFilter: "i",
            expectedNumDocsSnapshot: 0,
        });
    });

    shardingRestoreTest.getShardRestoreTests().forEach((magicRestoreUtil, idx) => {
        jsTestLog("Starting restore shard " + idx);
        magicRestoreUtil.rst.startSet({
            restart: true,
            dbpath: magicRestoreUtil.getBackupDbPath(),
            noCleanData: true,
            shardsvr: "",
        });
        magicRestoreUtil.rst.awaitNodesAgreeOnPrimary();
        // Make sure that all nodes have installed the config before moving on.
        let primary = magicRestoreUtil.rst.getPrimary();
        magicRestoreUtil.rst.waitForConfigReplication(primary);
        assert.soonNoExcept(() => isConfigCommitted(primary));

        magicRestoreUtil.rst.nodes.forEach((node) => {
            node.setSecondaryOk();
            const restoredDocs =
                node.getDB(dbName).getCollection(coll).find().sort({numForPartition: 1}).toArray();
            // The later 4 writes were truncated during magic restore, so each shard should have
            // only 2.
            assert.eq(restoredDocs.length, 2);
            magicRestoreUtil.postRestoreChecks({
                node: node,
                dbName: dbName,
                collName: coll,
                expectedOplogCountForNs: 2,
                opFilter: "i",
                expectedNumDocsSnapshot: 2,
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
        "cache.chunks.config.system.sessions",
        `cache.chunks.${dbName}.${coll}`
    ];
    shardingRestoreTest.checkPostRestoreDbHashes(excludedCollections);

    jsTestLog("Stopping restore nodes");
    shardingRestoreTest.getShardRestoreTests().forEach(
        (magicRestoreUtils) => { magicRestoreUtils.rst.stopSet(); });
    configUtils.rst.stopSet();
}

// Run non-PIT restore twice, with one run performing a no-op oplog entry insert with a higher term.
// This affects the stable timestamp on magic restore node shutdown.
runTest(false /* insertHigherTermOplogEntry */);
runTest(true /* insertHigherTermOplogEntry */);
