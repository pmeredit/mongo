/*
 * Tests a non-PIT sharded cluster magic restore with movePrimary and sharding renames.
 * The test does the following:
 *
 * - Starts a sharded cluster and inserts some initial data.
 * - Starts a movePrimary operation and sets a failpoint to pause it mid-way. This will ensure
 *   sharding DDL metadata collections exist in the backup.
 * - Creates the backup data files, copies data files to the restore dbpath.
 * - Computes maxCheckpointTs, extends the cursors to maxCheckpointTs and closes the backup cursor.
 * - Unblocks the movePrimary failpoint and ensures the operation finishes on the
 *   source cluster.
 * - Computes dbHashes.
 * - Stops all nodes.
 * - Writes a restore configuration object to a named pipe via the mongo shell.
 * - Starts the nodes with --magicRestore, which parses the restore configuration, runs restore and
 *   exits cleanly. We perform a shard rename, and so the node will update relevant sharding
 *   metadata with the new shard ID.
 * - Restarts the nodes in the initial sharded cluster and asserts that the replica set config and
 *   data are as expected, dbHashes match, and that the shard names were updated. We check modified
 *   sharding metadata documents to ensure we've correctly updated shard ID fields to refer to the
 *   new shard ID. We ensure the movePrimary operation completes on the restored cluster.
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

jsTestLog("Running non-PIT magic restore with movePrimary and shard renames");
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

const db = st.getDB(dbName);

assert.commandWorked(db.createCollection(coll));

// Ensure the primary shard for 'db' is shard 0.
const currentPrimaryShard = st.shard0.shardName;
assert.commandWorked(db.adminCommand({movePrimary: dbName, to: currentPrimaryShard}));
assert.eq(st.getPrimaryShardIdForDatabase(dbName), currentPrimaryShard);

jsTestLog("Inserting data to restore");  // This data will be reflected in the restored node.
['a', 'b', 'c', 'd'].forEach(
    key => { assert.commandWorked(db.getCollection(coll).insert({[key]: 1})); });
const expectedDocs = db.getCollection(coll).find().toArray();
assert.eq(expectedDocs.length, 4);

const shardingRestoreTest = new ShardedMagicRestoreTest({st: st, pipeDir: MongoRunner.dataDir});

// The test will move the primary shard for 'db' to shard 1.
const newPrimaryShard = st.shard1.shardName;
const collUuid = extractUUIDFromObject(
    shardingRestoreTest.shardRestoreTests[0].getCollUuid(st.rs0.getPrimary(), dbName, coll));

// Pause movePrimary before entering the critical section so that sharding DDL metadata
// collections are included in the backed up data files.
const movePrimaryHang =
    configureFailPoint(st.rs0.getPrimary(), "hangBeforeMovePrimaryCriticalSection");
const awaitResult =
    startParallelShell(funWithArgs(function(dbName, name) {
                           assert.commandWorked(db.adminCommand({movePrimary: dbName, to: name}));
                       }, dbName, newPrimaryShard), st.s.port);

movePrimaryHang.wait();

jsTestLog("Taking checkpoints and opening backup cursors");
shardingRestoreTest.takeCheckpointsAndOpenBackups();

shardingRestoreTest.findMaxCheckpointTsAndExtendBackupCursors();
shardingRestoreTest.setUpShardingRenamesAndIdentityDocs();

// Allow the movePrimary operation to finish on the source cluster.
movePrimaryHang.off();
awaitResult();
assert.eq(st.getPrimaryShardIdForDatabase(dbName), newPrimaryShard);

jsTestLog("Getting backup cluster dbHashes");
shardingRestoreTest.storePreRestoreDbHashes();

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

// Used to match the new shard ID in metadata documents.
const regex = new RegExp('^' + jsTestName() + '-dst-rs[01]');

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
    // Since movePrimary hasn't committed yet, the primary shard for the database should still be
    // shard0.
    assert(entries.every(entry => entry["primary"] === jsTestName() + '-dst-rs0'), tojson(entries));

    entries = node.getDB("config").getCollection("shards").find().toArray();
    assert.eq(entries.length, 2);
    assert(entries.every(entry => regex.test(entry["_id"] && entry["host"])), tojson(entries));
});

shardingRestoreTest.getShardRestoreTests().forEach((magicRestoreTest, idx) => {
    jsTestLog("Starting restore shard " + idx);
    magicRestoreTest.rst.name = magicRestoreTest.rst.name.replace("-rs", "-dst-rs");
    magicRestoreTest.rst.startSet({
        restart: true,
        dbpath: magicRestoreTest.getBackupDbPath(),
        noCleanData: true,
        shardsvr: "",
        replSet: magicRestoreTest.rst.name,
        // Prevent the shard from completing movePrimary so we can check metadata documents.
        setParameter:
            {"failpoint.hangBeforeMovePrimaryCriticalSection": tojson({mode: "alwaysOn"})},
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

        // Both shards contain the documents from db.coll, as movePrimary was in progress when the
        // backup was taken.
        assert.eq(restoredDocs.length, 4);
        magicRestoreTest.postRestoreChecks({
            node: node,
            dbName: dbName,
            collName: coll,
            expectedOplogCountForNs: 4,
            opFilter: "i",
            expectedNumDocsSnapshot: 4,
        });

        // On shard0, check the sharding DDL coordinator document to ensure the shardId has been
        // updated.
        if (idx === 0) {
            const shardingDDL =
                node.getDB("config").getCollection("system.sharding_ddl_coordinators").findOne();
            assert.eq(shardingDDL._id, {"namespace": "db", "operationType": "movePrimary"});
            assert.eq(shardingDDL.phase, "enterCriticalSection");
            assert(regex.test(shardingDDL.toShardId));

            const criticalSection =
                node.getDB("config").getCollection("collection_critical_sections").findOne();
            assert.eq(criticalSection._id, "db");
            assert.eq(criticalSection.reason.command, "movePrimary");
            assert(regex.test(criticalSection.reason.to));
        }
    });
    // Disable the failpoint.
    magicRestoreTest.rst.nodes.forEach((node) => {
        assert.commandWorked(node.adminCommand(
            {'configureFailPoint': 'hangBeforeMovePrimaryCriticalSection', 'mode': 'off'}));
    });
});

jsTestLog("Getting restore cluster dbHashes");
// A number of these collections are expected to be different across restore.
const excludedCollections = [
    "system.version",
    "shards",
    "actionlog",
    "clusterParameters",
    // Renaming shards affects the "primary" field of documents in 'config.databases'.
    "databases",
    "mongos",
    "cache.databases",
    "cache.collections",
    // Renaming shards affects the "shard" field of documents in 'config.chunks'.
    "chunks",
    "changelog",
    "vectorClock",
    "transactions",
    "cache.chunks.config.system.sessions",
    `cache.chunks.${dbName}.${coll}`,
    `cache.chunks.db.system.resharding.${collUuid}`,
    // config.placementHistory is re-inserted when movePrimary completes on the restored cluster.
    "placementHistory",
    "system.sharding_ddl_coordinators"
];

primary = configUtils.rst.getPrimary();
const configDB = primary.getDB("config");

// Ensure the primary shard has changed to shard1, indicating that the movePrimary completed.
let dbInfo;
assert.soon(() => {
    dbInfo = configDB.getCollection("databases").findOne({_id: "db"});
    return dbInfo.primary === jsTestName() + '-dst-rs1';
}, dbInfo);

shardingRestoreTest.checkPostRestoreDbHashes(excludedCollections);

jsTestLog("Checking sharding renames on the config server");
const shards = configDB.getCollection("shards").find().sort({"_id": 1}).toArray();
assert.eq(shards.length, 2);
assert(shards.every(shard => { return regex.test(shard._id) && regex.test(shard.host); }),
       tojson(shards));

jsTestLog("Checking the config server shard identity document");
const cfgShardIdentity =
    primary.getDB("admin").getCollection("system.version").findOne({"_id": "shardIdentity"});
// We didn't rename the config server replica set.
const configShardName = jsTestName() + "-configRS";
assert(
    cfgShardIdentity.configsvrConnectionString.includes(configShardName),
    `${tojson(cfgShardIdentity)} 'configsvrConnectionString' does not contain ${configShardName}`);

shardingRestoreTest.getShardRestoreTests().forEach((magicRestoreTest, idx) => {
    jsTestLog("Checking the renamed shard identity document for shard" + idx);
    const primary = magicRestoreTest.rst.getPrimary();
    const shardIdentity =
        primary.getDB("admin").getCollection("system.version").findOne({"_id": "shardIdentity"});
    assert(shardIdentity.configsvrConnectionString.includes(configShardName),
           `${shardIdentity} 'configsvrConnectionString' does not contain ${configShardName}`);
});

jsTestLog("Stopping restore nodes");
shardingRestoreTest.getShardRestoreTests().forEach(
    (magicRestoreTest) => { magicRestoreTest.rst.stopSet(); });
configUtils.rst.stopSet();
