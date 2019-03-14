/**
 * This test demonstrates how to do sharded cluster backup/restore.
 *
 * Test setup:
 *  - The sharded cluster has 4 shards, each of which is a 3-node replica set.
 *  - There is a writer client in the background doing causally related writes to 4 shards. By
 * splitting and assigning chunks in a delicate way, the first write will go to shard0, the second
 * will go to shard1, ..., the Nth write will go to shard[(N-1) % 4].
 *
 * Backup process:
 *  1. Open up a $backupCursor on a node(primary/secondary) of each shard and config servers.
 *  2. Spawn a copy worker thread to copy the data files for each node.
 *  3. Get the maxTimestamp given all the checkpoint timestamps return by $backupCursor.
 *  4. Call $backupCursorExtend on each node and copy additional files.
 *  5. Wait until all the copy worker threads have done the work.
 *  6. Close $backupCursor on each node. (This test closes all the cursors in the end out of
 *     convenience and there is no correctness reason to postpone closing a backup cursor. The best
 *     practice in real application is to close the backup cursor on a node immediately after that
 *     node's files have been copied.)
 *
 * Invalidate the backup due to a topology change:
 *  1. After the backup cursor on the config server has been extended, read "config.shards" with
 *     readConcern {level: "majority", afterClusterTime: maxTimestamp}.
 *  2. Invalidate the backup if the result does not match with the topology we ran backup on.
 *  3. If they match, check the oplog entries since maxTimestamp to see if there is any
 *     "removeShard" operation. Invalidate the backup if there is any.
 *
 * Restore process:
 *  1. Start mongod in standalone mode with the copied data for each node.
 *  2. Modify the "local" database:
 *      2.1 Update the oplogTruncateAfterPoint document to (maxTimestamp + 1).
 *      2.2 Update the port numbers in replset configuration document.
 *      2.3 Drop some collections.
 *  3. Shutdown mongod. Restart mongod with setParameters "recoverFromOplogAsStandalone" and
 *     "takeUnstableCheckpointOnShutdown" being true and then shutdown. The mongod will replay oplog
 *     to maxTimestamp on startup and take an unstable checkpoint on shutdown.
 *  4. For each copied data, start mongod again in standalone mode.
 *  5. If the node is a config server, update the port numbers in config.shards collection.
 *  6. If the node is a shard server,
 *      6.1 Update config server's information in "admin.system.version" collection.
 *      6.2 Drop all the "config.cache.xxx" collections.
 *  7. Shutdown each node.
 *  8. Start the sharded cluster.
 *
 * Input:
 *  - concurrentWorkWhileBackup gives a way to concurrently run other work while backup is in
 * progress. (i.e. Changing sharding topology).
 *
 * Output:
 *  - a message notifying the caller about what happened (i.e. Backup failed due to topology
 * changes)
 *
 */
var ShardedBackupRestoreTest = function(concurrentWorkWhileBackup) {
    "use strict";

    load("jstests/libs/backup_utils.js");

    const numShards = 4;
    const dbName = "test";
    const collName = "continuous_writes";

    //////////////////////////////////////////////////////////////////////////////////////
    /////////// Helper functions for checking causal consistency of the backup ///////////
    //////////////////////////////////////////////////////////////////////////////////////

    function _setupShardedCollectionForCausalWrites(st) {
        const fullNs = dbName + "." + collName;

        st.adminCommand({enableSharding: dbName});
        st.ensurePrimaryShard(dbName, st.shard0.shardName);

        // Shard the collection on "numForPartition".
        st.adminCommand({shardCollection: fullNs, key: {numForPartition: 1}});

        // Split the collection into 4 chunks: [MinKey, -100), [-100, 0), [0, 100), [100, MaxKey).
        st.adminCommand({split: fullNs, middle: {numForPartition: -100}});
        st.adminCommand({split: fullNs, middle: {numForPartition: 0}});
        st.adminCommand({split: fullNs, middle: {numForPartition: 100}});

        // Move chunks to shard 1, 2, 3.
        st.adminCommand({moveChunk: fullNs, find: {numForPartition: -50}, to: st.shard1.shardName});
        st.adminCommand({moveChunk: fullNs, find: {numForPartition: 50}, to: st.shard2.shardName});
        st.adminCommand({moveChunk: fullNs, find: {numForPartition: 150}, to: st.shard3.shardName});
    }

    function _startCausalWriterClient(st) {
        // This 1st write goes to shard 0, 2nd goes to shard 1, ..., Nth goes to
        // shard N-1, and then N+1th goes to shard 0 again, ...
        const numFallsIntoShard = [-150, -50, 50, 150];
        let docId = 0;

        // Make sure some writes are persistent on disk in order to have a meaningful checkpoint
        // to backup.
        while (docId < 100) {
            assert.commandWorked(st.getDB(dbName)[collName].insert({
                shardId: docId % numShards,
                numForPartition: numFallsIntoShard[docId % numShards],
                docId: docId
            },
                                                                   {writeConcern: {w: 3}}));
            docId++;
        }

        for (let i = 0; i < numShards; i++) {
            for (let node of st["rs" + i].nodes) {
                // Force a checkpoint.
                assert.commandWorked(node.getDB(dbName).adminCommand({fsync: 1}));
            }
        }

        const writerClientCmds = function(dbName, collName, numShards) {
            let session = db.getMongo().startSession({causalConsistency: true});
            let sessionColl = session.getDatabase(dbName).getCollection(collName);

            // This 1st write goes to shard 0, 2nd goes to shard 1, ..., Nth goes to
            // shard N-1, and then N+1th goes to shard 0 again, ...
            const numFallsIntoShard = [-150, -50, 50, 150];
            let docId = 100;

            // Run indefinitely.
            while (1) {
                assert.commandWorked(sessionColl.insert({
                    shardId: docId % numShards,
                    numForPartition: numFallsIntoShard[docId % numShards],
                    docId: docId
                }));
                docId++;
            }
        };

        return startMongoProgramNoConnect(
            MongoRunner.mongoShellPath,
            '--eval',
            '(' + writerClientCmds + ')("' + dbName + '", "' + collName + '", ' + numShards + ')',
            st.s.host);
    }

    function _stopWriterClient(writerPid) {
        let writerStatus = checkProgram(writerPid);
        assert(writerStatus.alive,
               "writer client was not running at end of test and exited with code: " +
                   writerStatus.exitCode);
        stopMongoProgramByPid(writerPid);
    }

    function _verifyDataIsCausallyConsistent(mongos) {
        let coll = mongos.getDB(dbName).getCollection(collName);
        let docs = coll.find().toArray();
        let total = docs.length;
        assert.gt(total, 0, "There is no doc.");
        jsTestLog("There are " + total + " documents in total.");

        let docSet = new Set();
        for (let i = 0; i < total; i++) {
            docSet.add(docs[i].docId);
        }
        for (let i = 0; i < total; i++) {
            assert(docSet.has(i), () => {
                return "Doc " + i + " is missing.";
            });
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////
    /////////////////// Helper functions for detecting topology changes //////////////////
    //////////////////////////////////////////////////////////////////////////////////////

    function _isTopologyChanged(configServer, oldTopologyInfo, restorePIT) {
        // Ideally, this should be a transactional collection scan when transaction is allowed on
        // the "config" database.
        let res = assert.commandWorked(configServer.getDB("config").runCommand({
            find: "shards",
            sort: {_id: 1},
            readConcern: {level: "majority", afterClusterTime: restorePIT}
        }));
        let newTopologyInfo = res.cursor.firstBatch;
        jsTestLog("New Shards Information: " + tojson(newTopologyInfo));

        // 1. If "newTopologyInfo" does not match with "oldTopologyInfo", we can conservatively
        // assume that the topology change happened between the time we record "oldTopologyInfo" and
        // "restorePIT".
        if (oldTopologyInfo.length != newTopologyInfo.length) {
            return true;
        }
        for (let i = 0; i < numShards; i++) {
            if (oldTopologyInfo[i]._id != newTopologyInfo[i]._id) {
                return true;
            }
        }

        // 2. If "newTopologyInfo" matches with "oldTopologyInfo", it is still possible a shard was
        // added before the "restorePIT" and removed after it.  If this is the case, the topology at
        // the "oldTopologyInfo" time is a subset of the topology at the "restorePIT" (do not
        // match).  We again conservatively assume that any presence of "removeShard" operations
        // between "restorePIT" and now indicates this happened.
        let oplogEntries =
            configServer.getDB("local").oplog.rs.find({"ts": {$gt: restorePIT}}).toArray();
        jsTestLog("Oplog entries after restorePIT " + restorePIT + ": " + tojson(oplogEntries));
        for (let oplog of oplogEntries) {
            if (oplog.ns == "config.shards" && oplog.op == "d") {
                jsTestLog("A 'removeShard' oplog has been detected: " + oplog);
                return true;
            }
        }
        return false;
    }

    //////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////// Runs the test ////////////////////////////////////
    //////////////////////////////////////////////////////////////////////////////////////

    this.run = function() {
        // Set the secondaries to priority 0 and votes 0 to prevent the primary from stepping down.
        const st = new ShardingTest({
            name: jsTestName(),
            shards: numShards,
            rs: {
                nodes:
                    [{}, {rsConfig: {priority: 0, votes: 0}}, {rsConfig: {priority: 0, votes: 0}}],
                syncdelay: 1,
                oplogSize: 1,
                setParameter: {writePeriodicNoops: true}
            }
        });

        _setupShardedCollectionForCausalWrites(st, dbName, collName);

        // Launch writer client
        let writerPid = _startCausalWriterClient(st);

        resetDbpath(MongoRunner.dataPath + "forRestore/");
        const restorePaths = [
            MongoRunner.dataPath + "forRestore/shard0",
            MongoRunner.dataPath + "forRestore/shard1",
            MongoRunner.dataPath + "forRestore/shard2",
            MongoRunner.dataPath + "forRestore/shard3",
            MongoRunner.dataPath + "forRestore/config"
        ];
        const configServerIdx = numShards;

        // TAKE THE BACKUP.
        let failureMessage;
        let backupPointInTime;
        try {
            // nodesToBackup and shardsInfo are hardcoded here out of convenience. In real
            // application, it should be constructed based on an arbitrarily stale read on the
            // information about shards.
            const nodesToBackup = [
                st.rs0.getSecondary(),
                st.rs1.getSecondaries()[1],
                st.rs2.getPrimary(),
                st.rs3.getSecondary(),
                st.config1
            ];
            const shardsInfo = [
                {_id: jsTestName() + "-rs0"},
                {_id: jsTestName() + "-rs1"},
                {_id: jsTestName() + "-rs2"},
                {_id: jsTestName() + "-rs3"}
            ];

            let copyWorkers = [];
            let heartbeaters = [];
            let backupCursors = [];
            let dbpaths = [];
            let backupIds = [];
            let maxTimestamp = Timestamp();
            let stopCounter = new CountDownLatch(1);
            for (let i = 0; i < numShards + 1; i++) {
                let metadata;
                // Since there might not be enough time for the server to have a checkpoint
                // timestamp, we simply retry until it has one.
                backupCursors[i] = openBackupCursor(nodesToBackup[i]);
                metadata = getBackupCursorMetadata(backupCursors[i]);
                assert("checkpointTimestamp" in metadata);
                heartbeaters.push(startHeartbeatThread(nodesToBackup[i].host,
                                                       backupCursors[i],
                                                       nodesToBackup[i].getDB("admin").getSession(),
                                                       stopCounter));
                let copyThread = copyBackupCursorFiles(
                    backupCursors[i], metadata["dbpath"], restorePaths[i], true);
                jsTestLog("Opened up backup cursor on " + nodesToBackup[i] + ": " +
                          tojson(metadata));
                dbpaths[i] = metadata.dbpath;
                backupIds[i] = metadata.backupId;
                let checkpointTimestamp = metadata.checkpointTimestamp;
                if (timestampCmp(checkpointTimestamp, maxTimestamp) > 0) {
                    maxTimestamp = checkpointTimestamp;
                }
                copyWorkers.push(copyThread);
            }

            concurrentWorkWhileBackup.setup();
            concurrentWorkWhileBackup.runBeforeExtend(st.s);

            for (let i = 0; i < numShards + 1; i++) {
                let cursor = extendBackupCursor(nodesToBackup[i], backupIds[i], maxTimestamp);
                let thread = copyBackupCursorExtendFiles(cursor, dbpaths[i], restorePaths[i], true);
                copyWorkers.push(thread);
                cursor.close();
            }

            concurrentWorkWhileBackup.runAfterExtend(st.s);

            copyWorkers.forEach((thread) => {
                thread.join();
            });

            // Stop the heartbeating thread.
            stopCounter.countDown();
            heartbeaters.forEach((heartbeater) => {
                heartbeater.join();
            });

            if (_isTopologyChanged(st.config1, shardsInfo, maxTimestamp)) {
                throw "Sharding topology has been changed during backup.";
            }

            backupCursors.forEach((cursor) => {
                cursor.close();
            });

            backupPointInTime = maxTimestamp;
            jsTestLog("The data of sharded cluster at timestamp " + backupPointInTime +
                      " has been successfully backed up at " + tojson(restorePaths));
        } catch (e) {
            failureMessage = "Failed to backup: " + e;
            jsTestLog(failureMessage);
        } finally {
            concurrentWorkWhileBackup.teardown();
            // Stop writer client.
            _stopWriterClient(writerPid);
            // Shutdown the original sharded cluster (optional).
            st.stop();
        }

        // End the test earlier if the backup failed (i.e. due to topology changes).
        if (failureMessage != undefined) {
            return failureMessage;
        }

        // RESTORE THE SHARDED CLUSTER.
        const restoredNodePorts = allocatePorts(numShards + 1);

        // Modify replica set metadata on all nodes.
        const truncateAfterPoint =
            Timestamp(backupPointInTime.getTime(), backupPointInTime.getInc() + 1);
        for (let i = 0; i < numShards + 1; i++) {
            let conn = MongoRunner.runMongod({dbpath: restorePaths[i], noCleanData: true});
            let localDb = conn.getDB("local");
            assert.commandWorked(localDb.replset.oplogTruncateAfterPoint.update(
                {_id: "oplogTruncateAfterPoint"},
                {$set: {"oplogTruncateAfterPoint": truncateAfterPoint}},
                true));
            let firstOplogToRemove = localDb.oplog.rs.find({ts: {$gte: truncateAfterPoint}})
                                         .sort({ts: 1})
                                         .limit(1)
                                         .toArray()[0];
            jsTestLog(restorePaths[i] + ": Truncating all the oplog after " + truncateAfterPoint +
                      "(inclusive), starting from oplog: " + tojson(firstOplogToRemove));

            localDb.replset.election.drop();
            localDb.replset.minValid.drop();
            assert.commandWorked(localDb.system.replset.update({}, {
                $set:
                    {members: [{_id: NumberInt(0), host: "localhost:" + restoredNodePorts[i]}]}
            }));

            MongoRunner.stopMongod(conn, {noCleanData: true});

            jsTestLog(restorePaths[i] + ": Replaying oplog to timestamp " + backupPointInTime);
            conn = MongoRunner.runMongod({
                dbpath: restorePaths[i],
                noCleanData: true,
                setParameter:
                    {recoverFromOplogAsStandalone: true, takeUnstableCheckpointOnShutdown: true}
            });
            assert.neq(conn, null);
            MongoRunner.stopMongod(conn, {noCleanData: true});
        }

        // Modify config.shards on config server.
        jsTestLog("Modify config server's metadata at " + restorePaths[configServerIdx]);
        let conn =
            MongoRunner.runMongod({dbpath: restorePaths[configServerIdx], noCleanData: true});
        let configDb = conn.getDB("config");
        jsTestLog("Old config.shards: " + tojson(configDb.shards.find().toArray()));
        configDb.shards.remove({});
        for (let i = 0; i < numShards; i++) {
            assert.commandWorked(configDb.shards.insert({
                "_id": jsTestName() + "-rs" + i,
                "host": jsTestName() + "-rs" + i + "/localhost:" + restoredNodePorts[i],
                "state": 1
            }));
        }
        jsTestLog("New config.shards: " + tojson(configDb.shards.find().toArray()));
        MongoRunner.stopMongod(conn, {noCleanData: true});

        // Modify shardIdentity document on shard servers.
        for (let i = 0; i < numShards; i++) {
            jsTestLog("Modify shard metadata at " + restorePaths[i]);
            let conn = MongoRunner.runMongod({dbpath: restorePaths[i], noCleanData: true});

            let adminDb = conn.getDB("admin");
            assert.commandWorked(adminDb.system.version.update({_id: "shardIdentity"}, {
                $set: {
                    shardName: jsTestName() + "-rs" + i,
                    configsvrConnectionString:
                        jsTestName() + "-configRS/localhost:" + restoredNodePorts[configServerIdx]
                }
            }));

            // Drop all the "config.cache.xxx" collections.
            let configDb = conn.getDB("config");
            let res = assert.commandWorked(configDb.runCommand(
                {listCollections: 1, nameOnly: true, filter: {name: {$regex: /cache\..*/}}}));
            let collInfos = new DBCommandCursor(configDb, res).toArray();
            collInfos.forEach(collInfo => {
                configDb[collInfo.name].drop();
            });
            MongoRunner.stopMongod(conn, {noCleanData: true});
        }

        // Start the restored sharded cluster.
        let configRS = new ReplSetTest({
            name: jsTestName() + "-configRS",
            nodes: [{
                noCleanData: true,
                dbpath: restorePaths[configServerIdx],
                port: restoredNodePorts[configServerIdx]
            }]
        });
        // Sadly, we need to manually update this field because ReplSetTest assumes "ports"
        // field is always set by itself.
        configRS.ports = [restoredNodePorts[numShards]];

        jsTestLog("Starting restored Config Server with data from " +
                  restorePaths[configServerIdx] + " at port " + restoredNodePorts[configServerIdx]);
        configRS.startSet({journal: "", configsvr: ""});

        let restoredShards = [];
        for (let i = 0; i < numShards; i++) {
            jsTestLog("Starting restored shard" + i + " with data from " + restorePaths[i] +
                      " at port " + restoredNodePorts[i]);
            restoredShards[i] = new ReplSetTest({
                name: jsTestName() + "-rs" + i,
                nodes: [{noCleanData: true, dbpath: restorePaths[i], port: restoredNodePorts[i]}],
            });
            restoredShards[i].startSet({shardsvr: ""});
        }

        let mongos = MongoRunner.runMongos({configdb: configRS.getURL()});

        _verifyDataIsCausallyConsistent(mongos);

        // Stop the sharded cluster.
        MongoRunner.stopMongos(mongos);
        configRS.stopSet();
        for (let i = 0; i < numShards; i++) {
            restoredShards[i].stopSet();
        }

        return "Test succeeded.";
    };
};
