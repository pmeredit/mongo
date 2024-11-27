/*
 * Tests a non-PIT sharded cluster restore with modifying balancer state. The test does the
 * following:
 *
 * - Starts a sharded cluster, shards the collection, moves chunks, inserts some initial data.
 * - Starts the balancer, which persists balancer state in 'config.settings'.
 * - Creates the backup data files, copies data files to the restore dbpath, computes dbHashes.
 * - Computes maxCheckpointTs, extends the cursors to maxCheckpointTs and closes the backup cursor.
 * - Writes a restore configuration object that includes a 'balancerSettings field' to a named pipe
 *   via the mongo shell.
 * - Stops all nodes.
 * - Starts the nodes with --magicRestore that parses the restore configuration and exits cleanly.
 * - Since 'balancerSettings' is present in the restore config, restore modifies the balancer
 *   settings in the 'config.settings' namespace.
 * - Restarts the nodes in the initial sharded cluster, asserts that the replica set config and
 *   data are as expected, that the dbHashes match, and that the balancer state is what we expect.
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

jsTestLog(
    "Running non-PIT magic restore with balancer settings specified in the restore configuration");
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
const expectedDocs = db.getCollection(coll).find().sort({numForPartition: 1}).toArray();
assert.eq(expectedDocs.length, 4);

const shardingRestoreTest = new ShardedMagicRestoreTest({
    st: st,
    pipeDir: MongoRunner.dataDir,
});

// Confirm the balancer is disabled initially.
let balancerEnabled = st.getDB("config").getCollection("settings").findOne({_id: "balancer"});
assert.eq(balancerEnabled.stopped, true, tojson(balancerEnabled));
// Start the balancer, and confirm the balancer is now enabled.
st.startBalancer();
balancerEnabled = st.getDB("config").getCollection("settings").findOne({_id: "balancer"});
assert.eq(balancerEnabled.stopped, false, tojson(balancerEnabled));

jsTestLog("Taking checkpoints and opening backup cursors");
shardingRestoreTest.takeCheckpointsAndOpenBackups();

jsTestLog("Getting backup cluster dbHashes");
// expected DBs are admin, config and db
shardingRestoreTest.storePreRestoreDbHashes();

// These documents will be truncated by magic restore, since they were written after the backup
// cursor was opened.
jsTestLog("Inserting data after backup cursor");
[-151, -51, 51, 151].forEach(
    val => { assert.commandWorked(db.getCollection(coll).insert({numForPartition: val})); });
assert.eq(db.getCollection(coll).find().toArray().length, 8);

shardingRestoreTest.getShardRestoreTests().forEach((magicRestoreTest) => {
    magicRestoreTest.rst.nodes.forEach((node) => {
        magicRestoreTest.assertOplogCountForNamespace(node, {ns: dbName + "." + coll, op: "i"}, 4);
        let {entriesAfterBackup} = magicRestoreTest.getEntriesAfterBackup(node);

        // There might be rangeDeletions ops after the backup, or a
        // ensureMajorityPrimaryAndScheduleDbTask too, filtering those out.
        entriesAfterBackup =
            entriesAfterBackup.filter(elem => (elem.ns != "config.rangeDeletions" &&
                                               elem.o != "ensureMajorityPrimaryAndScheduleDbTask"));

        assert.eq(entriesAfterBackup.length,
                  2,
                  `entriesAfterBackup = ${tojson(entriesAfterBackup)} is not of length 2`);
    });
});

shardingRestoreTest.findMaxCheckpointTsAndExtendBackupCursors();

jsTestLog("Stopping all nodes");
st.stop({noCleanData: true});

jsTestLog("Running magic restore");
// Turn off the balancer during restore.
shardingRestoreTest.setBalancerSettings(true /* stopped */);
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
    configUtils.postRestoreChecks({
        node: node,
        dbName: dbName,
        collName: coll,
        // We don't expect the config server to have data in db.coll.
        expectedOplogCountForNs: 0,
        opFilter: "i",
        expectedNumDocsSnapshot: 0,
    });

    // Ensure that after restore, the balancer state is set to 'stopped: true', meaning that it has
    // been stopped.
    const balancerEnabled =
        node.getDB("config").getCollection("settings").findOne({_id: "balancer"});
    assert.eq(balancerEnabled.stopped, true, tojson(balancerEnabled));
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
    let primary = magicRestoreTest.rst.getPrimary();
    magicRestoreTest.rst.waitForConfigReplication(primary);
    assert.soonNoExcept(() => isConfigCommitted(primary));

    magicRestoreTest.rst.nodes.forEach((node) => {
        node.setSecondaryOk();
        const restoredDocs =
            node.getDB(dbName).getCollection(coll).find().sort({numForPartition: 1}).toArray();
        // The later 4 writes were truncated during magic restore, so each shard should have
        // only 2.
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
// Excluding admin.system.version, config.shards, config.actionlog, config.rangeDeletions,
// cache.databases, cache.collections and cache.chunks.db.coll.
const excludedCollections = [
    "system.version",
    "shards",
    "actionlog",
    "changelog",
    "clusterParameters",
    "rangeDeletions",
    "mongos",
    "cache.databases",
    "cache.collections",
    "cache.chunks.config.system.sessions",
    `cache.chunks.${dbName}.${coll}`,
    "placementHistory",
    // 'config.settings' is different across restore.
    "settings"
];
shardingRestoreTest.checkPostRestoreDbHashes(excludedCollections);

// config.placementHistory is dropped during the restore procedure.
assert.eq(
    configUtils.rst.getPrimary().getDB("config").getCollection("placementHistory").find().toArray(),
    0);

jsTestLog("Stopping restore nodes");
shardingRestoreTest.getShardRestoreTests().forEach(
    (magicRestoreTest) => { magicRestoreTest.rst.stopSet(); });
configUtils.rst.stopSet();
