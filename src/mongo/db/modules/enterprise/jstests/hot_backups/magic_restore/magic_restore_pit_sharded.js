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
 *     requires_wiredtiger
 * ]
 */

import {MagicRestoreUtils} from "jstests/libs/backup_utils.js";
import {ReplSetTest} from "jstests/libs/replsettest.js";
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
    jsTestLog("Running PIT magic restore with insertHigherTermOplogEntry: " +
              insertHigherTermOplogEntry);
    const numShards = 2;
    const numNodes = 2;
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
    const clusterId = st.s.getCollection('config.version').findOne().clusterId;
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

    // The last entries in magicRestoreUtilsArray are for the config servers.
    const magicRestoreUtilsArray = [];

    const allNodesIncludingConfig = MagicRestoreUtils.getAllNodes(numShards + 1, numNodes);
    const allNodesExcludingConfig = allNodesIncludingConfig.slice(0, -numNodes);

    jsTestLog("Taking checkpoints and opening backup cursors");

    for (const [rsIndex, nodeIndex] of allNodesIncludingConfig) {
        magicRestoreUtilsArray.push(new MagicRestoreUtils({
            backupSource: rsIndex < numShards ? st["rs" + rsIndex].nodes[nodeIndex]
                                              : st.configRS.nodes[nodeIndex],
            pipeDir: MongoRunner.dataDir,
            insertHigherTermOplogEntry: insertHigherTermOplogEntry,
            backupDbPathSuffix: `${rsIndex}_${nodeIndex}`
        }));

        magicRestoreUtilsArray[numNodes * rsIndex + nodeIndex].takeCheckpointAndOpenBackup();
    }

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

    // TODO SERVER-90356: remove this if we decide to allow PIT without oplog entries after
    // the checkpoint timestamp.
    assert.commandWorked(db.adminCommand({addShardToZone: st.shard0.shardName, zone: 'x'}));

    jsTestLog("Getting backup cluster dbHashes");
    // expected DBs are admin, config and db
    const dbHashes =
        MagicRestoreUtils.getDbHashes(st, numShards, numNodes, 3 /* expectedDBCount */);

    // We store all the last oplog entry timestamps for each node so we can compare them to the
    // stable timestamp for each node later. Even though all nodes are consistent up to a particular
    // point in time (the maximum value of these oplog entries), each individual node's stable
    // timestamp will be the latest oplog entry in its oplog.
    const lastOplogEntryTss = [];
    let maxLastOplogEntryTs = new Timestamp(0, 0);
    const entriesAfterBackups = [];
    for (const [rsIndex, nodeIndex] of allNodesExcludingConfig) {
        const node = st["rs" + rsIndex].nodes[nodeIndex];
        const magicRestoreUtils = magicRestoreUtilsArray[numNodes * rsIndex + nodeIndex];

        // We inserted 8 documents and have 2 shards, so 4 per shard.
        magicRestoreUtils.assertOplogCountForNamespace(node, dbName + "." + coll, 4, "i");

        let {lastOplogEntryTs, entriesAfterBackup} = magicRestoreUtils.getEntriesAfterBackup(node);
        lastOplogEntryTss.push(lastOplogEntryTs);
        jsTestLog(`Computed lastOplogEntryTs ${tojson(lastOplogEntryTs)} for node ${
            nodeIndex} of shard ${rsIndex}`);
        if (timestampCmp(lastOplogEntryTs, maxLastOplogEntryTs) > 0) {
            maxLastOplogEntryTs = lastOplogEntryTs;
        }

        // There might be operations after the backup from periodic jobs such as rangeDeletions
        // or ensureMajorityPrimaryAndScheduleDbTask, so we filter those out for the comparison but
        // still pass them into magic restore as additional oplog entries to apply.
        const filteredEntriesAfterBackup =
            entriesAfterBackup.filter(elem => (elem.ns != "config.rangeDeletions" &&
                                               elem.o != "ensureMajorityPrimaryAndScheduleDbTask"));

        // Includes the 2 addOrRemoveShardInProgress entries generated by
        // transitionToDedicatedConfigServer.
        assert.eq(filteredEntriesAfterBackup.length,
                  4,
                  `filteredEntriesAfterBackup = ${
                      tojson(filteredEntriesAfterBackup)} is not of length 4`);

        entriesAfterBackups.push(entriesAfterBackup);
    }

    // Update the maxLastOplogEntryTs with the config server nodes lastOplogEntryTs.
    for (let nodeIndex = 0; nodeIndex < numNodes; nodeIndex++) {
        const node = st.configRS.nodes[nodeIndex];
        const magicRestoreUtils = magicRestoreUtilsArray[numNodes * numShards + nodeIndex];

        let {lastOplogEntryTs, entriesAfterBackup} = magicRestoreUtils.getEntriesAfterBackup(node);
        lastOplogEntryTss.push(lastOplogEntryTs);
        jsTestLog(
            `Computed lastOplogEntryTs ${tojson(lastOplogEntryTs)} for config sever ${nodeIndex}`);
        if (timestampCmp(lastOplogEntryTs, maxLastOplogEntryTs) > 0) {
            maxLastOplogEntryTs = lastOplogEntryTs;
        }

        entriesAfterBackups.push(entriesAfterBackup);
    }
    jsTestLog("Computed maxLastOplogEntryTs: " + tojson(maxLastOplogEntryTs));

    // Compute maxCheckpointTs from the shards and config servers.
    let maxCheckpointTs = magicRestoreUtilsArray[0].getCheckpointTimestamp();
    for (const [rsIndex, nodeIndex] of allNodesIncludingConfig) {
        const magicRestoreUtils = magicRestoreUtilsArray[numNodes * rsIndex + nodeIndex];
        magicRestoreUtils.copyFiles();
        const ts = magicRestoreUtils.getCheckpointTimestamp();
        if (timestampCmp(ts, maxCheckpointTs) > 0) {
            maxCheckpointTs = ts;
        }
    }
    jsTestLog("Computed maxCheckpointTs: " + tojson(maxCheckpointTs));

    jsTestLog("Extending backup cursors");
    for (const [rsIndex, nodeIndex] of allNodesIncludingConfig) {
        magicRestoreUtilsArray[numNodes * rsIndex + nodeIndex].extendAndCloseBackup(
            rsIndex < numShards ? st["rs" + rsIndex].nodes[nodeIndex]
                                : st.configRS.nodes[nodeIndex],
            maxCheckpointTs);
    }

    const expectedConfigs = [];
    for (let i = 0; i < numShards + 1; i++) {
        const primary = i < numShards ? st["rs" + i].getPrimary() : st.configRS.getPrimary();
        let expectedConfig =
            assert.commandWorked(primary.adminCommand({replSetGetConfig: 1})).config;
        expectedConfigs.push(expectedConfig);
    }

    const ports = [];
    for (const [rsIndex, nodeIndex] of allNodesIncludingConfig) {
        ports.push(rsIndex < numShards ? st["rs" + rsIndex].getPort(nodeIndex)
                                       : st.configRS.getPort(nodeIndex));
    }

    jsTestLog("Stopping all nodes");
    st.stop({noCleanData: true});

    const shardingRename = [];
    const shardIdentityDocuments = [];
    for (let i = 0; i < numShards; i++) {
        shardingRename.push({
            sourceShardName: st["shard" + i].shardName,
            destinationShardName: st["shard" + i].shardName.replace("-rs", "-dst-rs"),
            destinationShardConnectionString: st["shard" + i].host.replace("-rs", "-dst-rs")
        });
        shardIdentityDocuments.push({
            clusterId: clusterId,
            shardName: shardingRename[i].destinationShardName,
            configsvrConnectionString: st.configRS.getURL()
        });
    }
    shardIdentityDocuments.push({
        clusterId: clusterId,
        shardName: "config",
        configsvrConnectionString: st.configRS.getURL()
    });

    jsTestLog("Running Magic Restore with shardingRename = " + tojson(shardingRename));
    for (const [rsIndex, nodeIndex] of allNodesIncludingConfig) {
        let restoreConfiguration = {
            "nodeType": rsIndex < numShards ? "shard" : "configServer",
            "replicaSetConfig": expectedConfigs[rsIndex],
            "maxCheckpointTs": maxCheckpointTs,
            "pointInTimeTimestamp": maxLastOplogEntryTs,  // Restore to the max timestamp of the
                                                          // last oplog entry of the shards.
            "shardingRename": shardingRename,
            "shardIdentityDocument": shardIdentityDocuments[rsIndex]
        };

        const magicRestoreUtils = magicRestoreUtilsArray[numNodes * rsIndex + nodeIndex];
        restoreConfiguration =
            magicRestoreUtils.appendRestoreToHigherTermThanIfNeeded(restoreConfiguration);
        magicRestoreUtils.writeObjsAndRunMagicRestore(
            restoreConfiguration,
            entriesAfterBackups[numNodes * rsIndex + nodeIndex],
            {"replSet": jsTestName() + (rsIndex < numShards ? "-rs" + rsIndex : "-configRS")});
    }

    jsTestLog("Starting restore config server");
    // Get the last numNodes ports and turn them into a list of {"port": port}.
    const configsvr = new ReplSetTest({nodes: ports.slice(-numNodes).map(port => ({port}))});
    configsvr.startSet({
        dbpath: MagicRestoreUtils.parameterizeDbpath(
            magicRestoreUtilsArray[numNodes * numShards].getBackupDbPath()),
        noCleanData: true,
        replSet: jsTestName() + "-configRS",
        configsvr: ""
    });

    configsvr.awaitNodesAgreeOnPrimary();
    // Make sure that all nodes have installed the config before moving on.
    let primary = configsvr.getPrimary();
    configsvr.waitForConfigReplication(primary);
    assert.soonNoExcept(() => isConfigCommitted(primary));

    // We should be in dedicated config server mode and only have 2 entries since we did the
    // transition during PIT restore.
    shardEntries = primary.getDB("config").getCollection("shards").find().toArray();
    assert.eq(shardEntries.length, 2);

    // Check each node in the config server replica set.
    for (let nodeIndex = 0; nodeIndex < numNodes; nodeIndex++) {
        jsTestLog(`Checking config server ${nodeIndex}`);
        const node = configsvr.nodes[nodeIndex];
        const magicRestoreUtils = magicRestoreUtilsArray[numNodes * numShards + nodeIndex];

        magicRestoreUtils.postRestoreChecks({
            node: node,
            expectedConfig: expectedConfigs[numShards],
            dbName: dbName,
            collName: coll,
            // We don't expect the config server to have data in db.coll.
            expectedOplogCountForNs: 0,
            opFilter: "i",
            expectedNumDocsSnapshot: 0,
            shardLastOplogEntryTs: lastOplogEntryTss[numNodes * numShards + nodeIndex],
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
    }

    const replicaSets = [];
    for (let rsIndex = 0; rsIndex < numShards; rsIndex++) {
        jsTestLog("Starting restore shard " + rsIndex);
        // Restart the destination replica set.
        const rst = new ReplSetTest({
            nodes:
                ports.slice(numNodes * rsIndex, numNodes * rsIndex + numNodes).map(port => ({port}))
        });
        rst.startSet({
            dbpath: MagicRestoreUtils.parameterizeDbpath(
                magicRestoreUtilsArray[numNodes * rsIndex].getBackupDbPath()),
            noCleanData: true,
            shardsvr: "",
            replSet: jsTestName() + "-rs" + rsIndex
        });
        rst.awaitNodesAgreeOnPrimary();
        // Make sure that all nodes have installed the config before moving on.
        let primary = rst.getPrimary();
        rst.waitForConfigReplication(primary);
        assert.soonNoExcept(() => isConfigCommitted(primary));

        replicaSets.push(rst);
    }

    for (const [rsIndex, nodeIndex] of allNodesExcludingConfig) {
        jsTestLog(`Checking node ${nodeIndex} of shard ${rsIndex}`);
        const node = replicaSets[rsIndex].nodes[nodeIndex];
        node.setSecondaryOk();

        const magicRestoreUtils = magicRestoreUtilsArray[numNodes * rsIndex + nodeIndex];
        const restoredDocs =
            node.getDB(dbName).getCollection(coll).find().sort({numForPartition: 1}).toArray();
        assert.eq(restoredDocs, expectedDocs.slice(4 * rsIndex, 4 * rsIndex + 4));

        magicRestoreUtils.postRestoreChecks({
            node: node,
            expectedConfig: expectedConfigs[rsIndex],
            dbName: dbName,
            collName: coll,
            // We inserted 8 documents and have 2 shards, so 4 per shard.
            expectedOplogCountForNs: 4,
            opFilter: "i",
            expectedNumDocsSnapshot: 4,
            shardLastOplogEntryTs: lastOplogEntryTss[numNodes * rsIndex + nodeIndex],
        });
    }

    jsTestLog("Getting restore cluster dbHashes");
    // Excluding admin.system.version, config.shards, config.actionlog, config.rangeDeletions,
    // cache.databases, cache.collections and cache.chunks.db.coll.
    const excludedCollections = [
        "system.version",
        "shards",
        "actionlog",
        "rangeDeletions",
        "cache.databases",
        "cache.collections",
        "databases",  // Renaming shards affects the "primary" field of documents in that collection
        "chunks",     // Renaming shards affects the "shard" field of documents in that collection
        `cache.chunks.${dbName}.${coll}`
    ];
    MagicRestoreUtils.checkDbHashes(
        dbHashes, [...replicaSets, configsvr], excludedCollections, numShards, numNodes);

    jsTestLog("Stopping restore nodes");
    for (let i = 0; i < numShards; i++) {
        replicaSets[i].stopSet();
    }
    configsvr.stopSet();
}

// Run PIT restore twice, with one run performing a no-op oplog entry insert with a higher term.
// This affects the stable timestamp on magic restore node shutdown.
runTest(false /* insertHigherTermOplogEntry */);
runTest(true /* insertHigherTermOplogEntry */);
