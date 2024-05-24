/*
 * Tests a non-PIT sharded cluster restore with magic restore. The test does the following:
 *
 * - Starts a sharded cluster, shards the collection, moves chunks, inserts some initial data.
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

import {MagicRestoreUtils} from "jstests/libs/backup_utils.js";

// TODO SERVER-86034: Run on Windows machines once named pipe related failures are resolved.
if (_isWindows()) {
    jsTestLog("Temporarily skipping test for Windows variants. See SERVER-86034.");
    quit();
}

function runTest(insertHigherTermOplogEntry) {
    jsTestLog("Running non-PIT magic restore with insertHigherTermOplogEntry: " +
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

    jsTestLog("Getting backup cluster dbHashes");
    // expected DBs are admin, config and db
    const dbHashes =
        MagicRestoreUtils.getDbHashes(st, numShards, numNodes, 3 /* expectedDBCount */);

    // These documents will be truncated by magic restore, since they were written after the backup
    // cursor was opened.
    jsTestLog("Inserting data after backup cursor");
    [-151, -51, 51, 151].forEach(
        val => { assert.commandWorked(db.getCollection(coll).insert({numForPartition: val})); });
    assert.eq(db.getCollection(coll).find().toArray().length, 8);

    for (const [rsIndex, nodeIndex] of allNodesExcludingConfig) {
        const node = rsIndex < numShards ? st["rs" + rsIndex].nodes[nodeIndex]
                                         : st.configRS.nodes[nodeIndex];
        const magicRestoreUtils = magicRestoreUtilsArray[numNodes * rsIndex + nodeIndex];
        magicRestoreUtils.assertOplogCountForNamespace(node, dbName + "." + coll, 4, "i");

        let {entriesAfterBackup} = magicRestoreUtils.getEntriesAfterBackup(node);

        // There might be rangeDeletions ops after the backup, or a
        // ensureMajorityPrimaryAndScheduleDbTask too, filtering those out.
        entriesAfterBackup =
            entriesAfterBackup.filter(elem => (elem.ns != "config.rangeDeletions" &&
                                               elem.o != "ensureMajorityPrimaryAndScheduleDbTask"));
        assert.eq(entriesAfterBackup.length,
                  2,
                  `entriesAfterBackup = ${tojson(entriesAfterBackup)} is not of length 2`);
    }

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

    jsTestLog("Running Magic Restore");
    for (const [rsIndex, nodeIndex] of allNodesIncludingConfig) {
        let restoreConfiguration = {
            "nodeType": rsIndex < numShards ? "shard" : "configServer",
            "replicaSetConfig": expectedConfigs[rsIndex],
            "maxCheckpointTs": maxCheckpointTs
        };
        const magicRestoreUtils = magicRestoreUtilsArray[numNodes * rsIndex + nodeIndex];
        restoreConfiguration =
            magicRestoreUtils.appendRestoreToHigherTermThanIfNeeded(restoreConfiguration);
        magicRestoreUtils.writeObjsAndRunMagicRestore(restoreConfiguration, [], {
            "replSet": jsTestName() + (rsIndex < numShards ? "-rs" + rsIndex : "-configRS")
        });
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

    const primary = configsvr.getPrimary();
    const restoredConfig = assert.commandWorked(primary.adminCommand({replSetGetConfig: 1})).config;

    // Check each node in the config server replica set.
    for (let nodeIndex = 0; nodeIndex < numNodes; nodeIndex++) {
        const node = configsvr.nodes[nodeIndex];
        const magicRestoreUtils = magicRestoreUtilsArray[numNodes * numShards + nodeIndex];
        magicRestoreUtilsArray[numNodes * numShards + nodeIndex].assertConfigIsCorrect(
            expectedConfigs[numShards], restoredConfig);
        magicRestoreUtils.assertMinValidIsCorrect(node);
        magicRestoreUtils.assertStableCheckpointIsCorrectAfterRestore(node);
        magicRestoreUtils.assertCannotDoSnapshotRead(node, 0 /* expectedNumDocs */);
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
        replicaSets.push(rst);
    }

    for (const [rsIndex, nodeIndex] of allNodesExcludingConfig) {
        const node = replicaSets[rsIndex].nodes[nodeIndex];
        node.setSecondaryOk();

        const magicRestoreUtils = magicRestoreUtilsArray[numNodes * rsIndex + nodeIndex];
        const restoredConfig =
            assert.commandWorked(node.adminCommand({replSetGetConfig: 1})).config;
        magicRestoreUtils.assertConfigIsCorrect(expectedConfigs[rsIndex], restoredConfig);
        const restoredDocs =
            node.getDB(dbName).getCollection(coll).find().sort({numForPartition: 1}).toArray();
        // The later 4 writes were truncated during magic restore, so each shard should have
        // only 2.
        assert.eq(restoredDocs.length, 2);
        assert.eq(restoredDocs, [expectedDocs[2 * rsIndex], expectedDocs[2 * rsIndex + 1]]);

        magicRestoreUtils.assertOplogCountForNamespace(node, dbName + "." + coll, 2, "i");
        magicRestoreUtils.assertMinValidIsCorrect(node);
        magicRestoreUtils.assertStableCheckpointIsCorrectAfterRestore(node);
        magicRestoreUtils.assertCannotDoSnapshotRead(node, 2 /* expectedNumDocs */);
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

// Run non-PIT restore twice, with one run performing a no-op oplog entry insert with a higher term.
// This affects the stable timestamp on magic restore node shutdown.
runTest(false /* insertHigherTermOplogEntry */);
runTest(true /* insertHigherTermOplogEntry */);
