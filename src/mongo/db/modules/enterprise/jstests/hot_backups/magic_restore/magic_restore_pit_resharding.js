/*
 * Tests a PIT sharded cluster magic restore with resharding and sharding renames.
 * The test does the following:
 *
 * - Starts a sharded cluster, shards the collection, moves chunks, inserts some initial data.
 * - Starts a resharding operation and sets a failpoint to pause it mid-way. This will ensure
 *   resharding metadata collections exist in the backup.
 * - Creates the backup data files, copies data files to the restore dbpath.
 * - Writes additional data that will be truncated by magic restore, since they occur after the
 *   checkpoint timestamp. However, these writes will still be reflected in the final state of the
 *   data due to the PIT restore.
 * - Computes dbHashes.
 * - Computes maxCheckpointTs, extends the cursors to maxCheckpointTs and closes the backup cursor.
 * - Unblocks the resharding failpoint and ensures the resharding operation finishes on the
 *   source cluster.
 * - Stops all nodes.
 * - Writes a restore configuration object and the source oplog entries from after the checkpoint
 *   timestamp to a named pipe via the mongo shell.
 * - Starts the nodes with --magicRestore, which parses the restore configuration, inserts and
 *   applies the additional oplog entries, and exits cleanly. We perform a shard rename, and so the
 *   node will update relevant sharding metadata with the new shard ID.
 * - Restarts the nodes in the initial sharded cluster and asserts that the replica set config and
 *   data are as expected, dbHashes match, and that the shard names were updated. We check modified
 *   sharding metadata documents to ensure we've correctly updated shard ID fields to refer to the
 *   new shard ID.
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

    const collUuid = extractUUIDFromObject(
        shardingRestoreTest.shardRestoreTests[0].getCollUuid(st.rs0.getPrimary(), dbName, coll));

    // Pause resharding so that resharding metadata collections are included in the backed up data
    // files.
    let reshardingHang = configureFailPoint(st.configRS.getPrimary(),
                                            "reshardingPauseCoordinatorBeforeBlockingWrites");
    const awaitResult = startParallelShell(
        funWithArgs(function(ns) {
            assert.commandWorked(db.adminCommand(
                {reshardCollection: ns, key: {numForPartition: "hashed"}, numInitialChunks: 2}));
        }, fullNs), st.s.port);

    reshardingHang.wait();

    jsTestLog("Taking checkpoints and opening backup cursors");
    shardingRestoreTest.takeCheckpointsAndOpenBackups();

    // These documents will be truncated by magic restore, since they were written after the backup
    // cursor was opened. We will pass these oplog entries to magic restore to perform a PIT
    // restore, so they will be reinserted and reflected in the final state of the data.
    jsTestLog("Inserting data after backup cursor");
    [-151, -51, 51, 151].forEach(
        val => { assert.commandWorked(db.getCollection(coll).insert({numForPartition: val})); });
    expectedDocs = db.getCollection(coll).find().sort({numForPartition: 1}).toArray();
    assert.eq(expectedDocs.length, 8);
    // It's possible the config server may not have any oplog entries after the backup checkpoint
    // timestamp. In our PIT restore test helpers, we expect there to always be entries after the
    // backup. To allow that constraint to hold and make our testing stronger, perform a no-op.
    assert.commandWorked(
        st.configRS.getPrimary().adminCommand({appendOplogNote: 1, data: {msg: "no-op"}}));

    jsTestLog("Getting backup cluster dbHashes");
    // expected DBs are admin, config and db
    shardingRestoreTest.storePreRestoreDbHashes();

    shardingRestoreTest.getShardRestoreTests().forEach((magicRestoreTest) => {
        magicRestoreTest.rst.nodes.forEach((node) => {
            magicRestoreTest.assertOplogCountForNamespace(node, {ns: fullNs, op: "i"}, 4);

            let {entriesAfterBackup} = magicRestoreTest.getEntriesAfterBackup(node);
            // There might be operations after the backup from periodic jobs such as rangeDeletions
            // or ensureMajorityPrimaryAndScheduleDbTask and entries from moving the chunk,
            // so we filter those out for the comparison but still pass them into magic restore as
            // additional oplog entries to apply.
            const filteredEntriesAfterBackup =
                entriesAfterBackup.filter(elem => elem.op == "i" && elem.ns == fullNs);
            assert.eq(filteredEntriesAfterBackup.length,
                      2,
                      `filteredEntriesAfterBackup = ${
                          tojson(filteredEntriesAfterBackup)} is not of length 2`);
        });
    });

    shardingRestoreTest.findMaxCheckpointTsAndExtendBackupCursors();
    shardingRestoreTest.setPointInTimeTimestamp();
    shardingRestoreTest.setUpShardingRenamesAndIdentityDocs();

    // Allow the resharding operation to finish on the source cluster.
    reshardingHang.off();
    awaitResult();

    // Ensure the shard key on the source cluster has been changed due to the resharding operation
    // completing successfully.
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

        const reshardingOps = node.getDB("config").getCollection("reshardingOperations").findOne();
        assert.eq(reshardingOps.reshardingKey, {numForPartition: "hashed"}, tojson(reshardingOps));

        // Ensure the resharding operation was aborted by restore.
        assert.eq(reshardingOps.state, "aborting", tojson(reshardingOps));
        assert.eq(
            reshardingOps.abortReason,
            {code: ErrorCodes.ReshardCollectionAborted, errmsg: "aborted by automated restore"},
            tojson(reshardingOps));

        // Check that documents in the 'config.reshardingOperations' collection refer to the new
        // shard ID.
        assert(
            reshardingOps.donorShards.every(donorShard => regex.test(donorShard.id)),
            "config.reshardingOperations donorShards entry ID does not match new shard ID regex. " +
                tojson(reshardingOps.donorShards));
        assert(
            reshardingOps.recipientShards.every(recipientShard => regex.test(recipientShard.id)),
            "config.reshardingOperations recipientShards entry ID does not match new shard ID regex. " +
                tojson(reshardingOps.recipientShards));

        // Confirm the shard key was not changed on the restore node, since resharding was aborted.
        const shardKey = node.getDB("config").getCollection("collections").findOne({_id: fullNs});
        assert.eq(shardKey.key, {numForPartition: 1}, tojson(shardKey));

        let entries = node.getDB("config").getCollection("databases").find().toArray();
        assert.eq(entries.length, 1);
        assert(entries.every(entry => regex.test(entry["primary"])), tojson(entries));

        entries = node.getDB("config").getCollection("shards").find().toArray();
        assert.eq(entries.length, 2);
        assert(entries.every(entry => regex.test(entry["_id"] && entry["host"])), tojson(entries));

        entries = node.getDB("config").getCollection("chunks").find().toArray();
        assert.eq(entries.length, 5);
        // Each 'config.chunks' entry has the following shape:
        // {
        //   shard: <shardId>,
        //   ...
        //   history: [
        //     {
        //       shard: <shardId>,
        //       ...
        //     }
        //   ]
        //   ...
        // }
        assert(entries.every(entry => {
            return regex.test(entry["shard"]) &&
                entry["history"].every(historyEntry => regex.test(historyEntry["shard"]));
        }),
               tojson(entries));
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
            // Prevent the 'localReshardingOperations' documents from being dropped, so we can check
            // their fields for the shard rename.
            setParameter: {
                "failpoint.removeRecipientDocFailpoint": tojson({mode: "alwaysOn"}),
                "failpoint.removeDonorDocFailpoint": tojson({mode: "alwaysOn"})
            },
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
            assert.eq(restoredDocs.length, 4);
            magicRestoreTest.postRestoreChecks({
                node: node,
                dbName: dbName,
                collName: coll,
                expectedOplogCountForNs: 4,
                opFilter: "i",
                expectedNumDocsSnapshot: 4,
            });

            // Each 'config.localReshardingOperations.donor' entry has the following shape:
            // {
            //   ...
            //   mutableState: {
            //       abortReason: {
            //           code: <ErrorCode>
            //           ...
            //       }
            //   },
            //   ...
            //   recipientShards: [
            //       <shardId0>,
            //       <shardId1>,
            //       ...
            //   ]
            // }
            assert.soonNoExcept(() => {
                const donors = node.getDB("config")
                                   .getCollection("localReshardingOperations.donor")
                                   .find()
                                   .toArray();
                return donors.every(({mutableState, recipientShards}) => {
                    return mutableState.abortReason.code === ErrorCodes.ReshardCollectionAborted &&
                        recipientShards.every(recipientShardId => regex.test(recipientShardId));
                });
            });

            // Each 'config.localReshardingOperations.recipient' entry has the following shape:
            // {
            //   ...
            //   mutableState: {
            //     abortReason: {
            //         code: <ErrorCode>
            //         ...
            //     }
            //   },
            //   ...
            //   donorShards: [
            //     {
            //       shardId: <shardId>,
            //       ...
            //     },
            //     ...
            //   ]
            // }
            assert.soonNoExcept(() => {
                const recipients = node.getDB("config")
                                       .getCollection("localReshardingOperations.recipient")
                                       .find()
                                       .toArray();
                return recipients.every(({mutableState, donorShards}) => {
                    return mutableState.abortReason.code === ErrorCodes.ReshardCollectionAborted &&
                        donorShards.every(donorShardId => regex.test(donorShardId.shardId));
                });
            });

            // Disable the failpoint so that the documents in the 'localReshardingOperations'
            // collection can be dropped.
            magicRestoreTest.rst.nodes.forEach((node) => {
                assert.commandWorked(node.adminCommand(
                    {'configureFailPoint': 'removeRecipientDocFailpoint', 'mode': 'off'}));
                assert.commandWorked(node.adminCommand(
                    {'configureFailPoint': 'removeDonorDocFailpoint', 'mode': 'off'}));
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
        // Renaming shards affects the "primary" field of documents in 'config.databases'.
        "databases",
        // Renaming shards affects the "shard" field of documents in 'config.chunks'.
        "chunks",
        "vectorClock",
        "reshardingOperations",
        // Since the resharding operation was aborted, the 'localReshardingOperations' namespace was
        // cleared and will be different pre- and post-restore.
        "localReshardingOperations.donor",
        "localReshardingOperations.recipient",
        "localReshardingOperations.recipient.progress_applier",
        "localReshardingOperations.recipient.progress_fetcher",
        "cache.chunks.config.system.sessions",
        `cache.chunks.${dbName}.${coll}`,
        `cache.chunks.db.system.resharding.${collUuid}`,
        // The source cluster had local resharding metadata collections when the hash
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
    // config.placementHistory is dropped during the restore procedure.
    assert.eq(configDB.getCollection("placementHistory").find().toArray(), 0);

    jsTestLog("Checking sharding renames on the config shard");
    const shards = configDB.getCollection("shards").find().sort({"_id": 1}).toArray();
    assert.eq(shards.length, 2);
    assert(shards.every(shard => { return regex.test(shard._id) && regex.test(shard.host); }),
           tojson(shards));

    // We expect the resharding services to clean up the aborted resharding metadata.
    assert.soonNoExcept(
        () => configDB.getCollection("reshardingOperations").find().toArray().length == 0);

    const chunks = configDB.getCollection("chunks").find().sort({"shard": 1}).toArray();
    // After the restore, resharding is aborted and there are only 2 entries for the 2 chunks
    // (minKey, 0) and [0, maxKey) plus the original (minKey, maxKey) entry.
    assert.eq(chunks.length, 3);
    for (let chunkId = 0; chunkId < chunks.length; chunkId++) {
        const shardName = jsTestName() + `-dst-rs${chunkId >= 2 ? 1 : 0}`;
        assert.eq(chunks[chunkId].shard, shardName);
        assert.eq(chunks[chunkId].history.length, 1);
        assert.eq(chunks[chunkId].history[0].shard, shardName);
    }

    jsTestLog("Checking the config server shard identity document");
    const cfgShardIdentity =
        primary.getDB("admin").getCollection("system.version").findOne({"_id": "shardIdentity"});
    // We didn't rename the config server replica set.
    const configShardName = jsTestName() + "-configRS";
    assert(cfgShardIdentity.configsvrConnectionString.includes(configShardName),
           `${tojson(cfgShardIdentity)} 'configsvrConnectionString' does not contain ${
               configShardName}`);

    shardingRestoreTest.getShardRestoreTests().forEach((magicRestoreTest, idx) => {
        jsTestLog("Checking the renamed shard identity document for shard" + idx);
        const primary = magicRestoreTest.rst.getPrimary();
        const shardIdentity = primary.getDB("admin").getCollection("system.version").findOne({
            "_id": "shardIdentity"
        });
        assert(shardIdentity.configsvrConnectionString.includes(configShardName),
               `${shardIdentity} 'configsvrConnectionString' does not contain ${configShardName}`);
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
