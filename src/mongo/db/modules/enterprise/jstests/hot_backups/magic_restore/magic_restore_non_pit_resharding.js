/*
 * Tests a non-PIT sharded cluster magic restore with resharding. Note that this test does not
 * do shard renames, but we still expect to abort the resharding operation.
 * The test does the following:
 *
 * - Starts a sharded cluster, shards the collection, moves chunks, inserts some initial data.
 * - Starts a resharding operation and sets a failpoint to pause it mid-way. This will ensure
 *   resharding metadata collections exist in the backup.
 * - Creates the backup data files, copies data files to the restore dbpath.
 * - Computes dbHashes.
 * - Computes maxCheckpointTs, extends the cursors to maxCheckpointTs and closes the backup cursor.
 * - Unblocks the resharding failpoint and ensures the resharding operation finishes on the
 *   source cluster.
 * - Stops all nodes.
 * - Writes a restore configuration object to a named pipe via the mongo shell.
 * - Starts the nodes with --magicRestore, which parses the restore configuration, runs restore and
 * exits cleanly.
 * - Restarts the nodes in the initial sharded cluster and asserts that the replica set config and
 *   data are as expected, dbHashes match, and that the resharding has been aborted despite no
 * shards being renamed.
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
import {extractUUIDFromObject} from "jstests/libs/uuid_util.js";
import {isConfigCommitted} from "jstests/replsets/rslib.js";

// TODO SERVER-87225: Enable fast count on validate when operations applied during a restore are
// counted correctly.
TestData.skipEnforceFastCountOnValidate = true;

jsTestLog("Running non-PIT magic restore with resharding");
// Setting priorities on the second node because assertConfigIsCorrect checks terms:
// With only 2 nodes, it might happen that both nodes try to run for primary at the same time
// and vote for themselves, which would increase the term.
const st = new ShardingTest({
    shards: {
        rs0: {nodes: [{}, {rsConfig: {priority: 0}}]},
        rs1: {nodes: [{}, {rsConfig: {priority: 0}}]}
    },
    mongos: 1,
    config: [{}, {rsConfig: {priority: 0}}],
    initiateWithDefaultElectionTimeout: true
});

const dbName = "db";
const coll = "coll";
const fullNs = dbName + "." + coll;
jsTestLog("Setting up sharded collection " + fullNs);
assert(st.adminCommand({enableSharding: dbName, primaryShard: st.shard0.shardName}));
assert(st.adminCommand({shardCollection: fullNs, key: {numForPartition: 1}}));

// Split the collection into 2 chunks: [MinKey, 0), [0, MaxKey).
assert(st.adminCommand({split: fullNs, middle: {numForPartition: 0}}));
assert(st.adminCommand({moveChunk: fullNs, find: {numForPartition: 50}, to: st.shard1.shardName}));

const db = st.getDB(dbName);
jsTestLog("Inserting data to restore");  // This data will be reflected in the restored node.
[-150, -50, 50, 150].forEach(
    val => { assert.commandWorked(db.getCollection(coll).insert({numForPartition: val})); });
let expectedDocs = db.getCollection(coll).find().sort({numForPartition: 1}).toArray();
assert.eq(expectedDocs.length, 4);

const shardingRestoreTest = new ShardedMagicRestoreTest({
    st: st,
    pipeDir: MongoRunner.dataDir,
});

const collUuid = extractUUIDFromObject(
    shardingRestoreTest.shardRestoreTests[0].getCollUuid(st.rs0.getPrimary(), dbName, coll));

// Pause resharding so that resharding metadata collections are included in the backed up data
// files.
let reshardingHang =
    configureFailPoint(st.configRS.getPrimary(), "reshardingPauseCoordinatorBeforeBlockingWrites");
const awaitResult = startParallelShell(
    funWithArgs(function(ns) {
        assert.commandWorked(
            db.adminCommand({reshardCollection: ns, key: {numForPartition: "hashed"}}));
    }, fullNs), st.s.port);

reshardingHang.wait();

jsTestLog("Taking checkpoints and opening backup cursors");
shardingRestoreTest.takeCheckpointsAndOpenBackups();
shardingRestoreTest.findMaxCheckpointTsAndExtendBackupCursors();

jsTestLog("Getting backup cluster dbHashes");
// expected DBs are admin, config and db
shardingRestoreTest.storePreRestoreDbHashes();

shardingRestoreTest.getShardRestoreTests().forEach((magicRestoreTest) => {
    magicRestoreTest.rst.nodes.forEach((node) => {
        magicRestoreTest.assertOplogCountForNamespace(node, {ns: fullNs, op: "i"}, 2);
    });
});

// These documents will be truncated by magic restore, since they were written after the backup
// cursor was opened.
jsTestLog("Inserting data after backup cursor");
[-151, -51, 51, 151].forEach(
    val => { assert.commandWorked(db.getCollection(coll).insert({numForPartition: val})); });
expectedDocs = db.getCollection(coll).find().sort({numForPartition: 1}).toArray();
assert.eq(expectedDocs.length, 8);

// Allow the resharding operation to finish on the source cluster.
reshardingHang.off();
awaitResult();

// Ensure the shard key on the source cluster has been changed.
const shardKey = st.getDB("config").getCollection("collections").findOne({_id: fullNs});
assert.eq(shardKey.key, {numForPartition: "hashed"});

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

    const reshardingOps = node.getDB("config").getCollection("reshardingOperations").findOne();
    assert.eq(reshardingOps.reshardingKey, {numForPartition: "hashed"}, tojson(reshardingOps));

    // Ensure the resharding operation was aborted by restore.
    assert.eq(reshardingOps.state, "aborting", tojson(reshardingOps));
    assert.eq(reshardingOps.abortReason,
              {code: ErrorCodes.ReshardCollectionAborted, errmsg: "aborted by automated restore"},
              tojson(reshardingOps));

    // Confirm the shard key was not changed on the restore node, since resharding was aborted.
    const shardKey = node.getDB("config").getCollection("collections").findOne({_id: fullNs});
    assert.eq(shardKey.key, {numForPartition: 1}, tojson(shardKey));
});

shardingRestoreTest.getShardRestoreTests().forEach((magicRestoreTest, idx) => {
    jsTestLog("Starting restore shard " + idx);
    magicRestoreTest.rst.startSet({
        restart: true,
        dbpath: magicRestoreTest.getBackupDbPath(),
        noCleanData: true,
        shardsvr: "",
        replSet: magicRestoreTest.rst.name,
    });
    magicRestoreTest.rst.awaitNodesAgreeOnPrimary();
    // Make sure that all nodes have installed the config before moving on.
    const primary = magicRestoreTest.rst.getPrimary();
    magicRestoreTest.rst.waitForConfigReplication(primary);
    assert.soonNoExcept(() => isConfigCommitted(primary));

    magicRestoreTest.rst.nodes.forEach((node) => {
        node.setSecondaryOk();
        const restoredDocs =
            node.getDB(dbName).getCollection(coll).find().sort({numForPartition: 1}).toArray();
        // Each shard should have half the total number of documents.
        assert.eq(restoredDocs.length, 2);
        magicRestoreTest.postRestoreChecks({
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
// A number of these collections are expected to be different across restore.
const excludedCollections = [
    "system.version",
    "shards",
    "actionlog",
    "clusterParameters",
    "mongos",
    "cache.databases",
    "cache.collections",
    "chunks",
    "vectorClock",
    "reshardingOperations",
    // Since the resharding operation was aborted, the 'localReshardingOperations' namespace was
    // cleared and will be different pre- and post-restore.
    "localReshardingOperations.donor",
    "localReshardingOperations.recipient",
    "localReshardingOperations.recipient.progress_applier",
    "cache.chunks.config.system.sessions",
    `cache.chunks.${dbName}.${coll}`,
    `cache.chunks.db.system.resharding.${collUuid}`,
    // The source cluster had the two 'localResharding*' collections when the hash
    // snapshot was taken, so we should skip checking for them on the restored node.
    `localReshardingOplogBuffer.${collUuid}.${jsTestName()}-rs0`,
    `localReshardingOplogBuffer.${collUuid}.${jsTestName()}-rs1`,
    `localReshardingConflictStash.${collUuid}.${jsTestName()}-rs0`,
    `localReshardingConflictStash.${collUuid}.${jsTestName()}-rs1`,
    `system.resharding.${collUuid}`,
    "localReshardingResumeData.recipient",
    "system.sharding_ddl_coordinators",
    // Cancelling resharding creates a transaction.
    "transactions",
    // The 'db.coll' entry has different 'key' fields due to different resharding outcomes for
    // source and target.
    "collections",
    // 'config.changelog' has extra entry from transitioning resharding from aborting to done.
    "changelog",
    "placementHistory"
];
shardingRestoreTest.checkPostRestoreDbHashes(excludedCollections);

primary = configUtils.rst.getPrimary();
const configDB = primary.getDB("config");
// We expect the resharding services to clean up the aborted resharding metadata.
assert.soonNoExcept(() => configDB.getCollection("reshardingOperations").find().toArray().length ==
                        0);

// config.placementHistory is dropped during the restore procedure.
assert.eq(configDB.getCollection("placementHistory").find().toArray(), 0);

jsTestLog("Stopping restore nodes");
shardingRestoreTest.getShardRestoreTests().forEach(
    (magicRestoreTest) => { magicRestoreTest.rst.stopSet(); });
configUtils.rst.stopSet();
