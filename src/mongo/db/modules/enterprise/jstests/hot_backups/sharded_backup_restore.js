/**
 * This library specifies, and tests, the supported sharded cluster backup/restore procedure. This
 * procedure allows for restoring the data from the source cluster into a different destination
 * cluster or the same source cluster.
 *
 * Note that regardless of restoring to different destination cluster or same source cluster, the
 * first step is always stopping all mongods and clearing all node dbpaths. The destination cluster
 * concept is only to express that at the restore's conclusion, the expectation is that an end user
 * can still use the same mongoURI they'd been using for that cluster previously, even if the source
 * cluster's snapshot has entirely different hostname:ports. No actual data from the destination
 * cluster will exist or gets "merged" with the snapshot, nor will a snapshot be restored for one
 * member into a running cluster.
 *
 * The restore procedure can accept a Cloud Provider Snapshot (CPS), but that snapshot must be
 * indistinguishable from a backup taken with a backup cursor. This library only tests backups taken
 * with backup cursors, but since the backups are indistinguishable the restore specification works
 * for both types of backups.
 *
 * Test setup:
 *  - The sharded cluster has 4 shards, each of which is a 3-node replica set.
 *  - There is a writer client in the background doing causally related writes to 4 shards. By
 *    splitting and assigning chunks in a delicate way, the first write will go to shard0, the
 *    second will go to shard1, ..., the Nth write will go to shard[(N-1) % 4].
 *
 * Input:
 *  - concurrentWorkWhileBackup gives a way to concurrently run other work while backup is in
 *    progress. (i.e. Changing sharding topology).
 *
 * Output:
 *  - a message notifying the caller about what happened (i.e. Backup failed due to topology
 *    changes).
 *
 * For any functions that are not defined in this file, please see `jstests/libs/backup_utils.js`.
 */
var ShardedBackupRestoreTest = function(concurrentWorkWhileBackup) {
    "use strict";

    load("jstests/libs/backup_utils.js");

    const numShards = 4;
    const dbName = "test";
    const collName = "continuous_writes";
    const restorePaths = [
        MongoRunner.dataPath + "forRestore/shard0",
        MongoRunner.dataPath + "forRestore/shard1",
        MongoRunner.dataPath + "forRestore/shard2",
        MongoRunner.dataPath + "forRestore/shard3",
        MongoRunner.dataPath + "forRestore/config"
    ];
    const configServerIdx = numShards;

    const csrsName = jsTestName() + "-configRS";
    function _shardName(shardNum) {
        return jsTestName() + "-rs" + shardNum;
    }

    //////////////////////////////////////////////////////////////////////////////////////
    /////////// Helper functions for checking causal consistency of the backup ///////////
    //////////////////////////////////////////////////////////////////////////////////////

    function _setupShardedCollectionForCausalWrites(st) {
        const fullNs = dbName + "." + collName;
        jsTestLog("Setting up sharded collection " + fullNs);

        st.adminCommand({enableSharding: dbName});
        st.ensurePrimaryShard(dbName, st.shard0.shardName);

        st.adminCommand({shardCollection: fullNs, key: {numForPartition: 1}});

        // Split the collection into 4 chunks: [MinKey, -100), [-100, 0), [0, 100), [100, MaxKey).
        st.adminCommand({split: fullNs, middle: {numForPartition: -100}});
        st.adminCommand({split: fullNs, middle: {numForPartition: 0}});
        st.adminCommand({split: fullNs, middle: {numForPartition: 100}});

        st.adminCommand({moveChunk: fullNs, find: {numForPartition: -50}, to: st.shard1.shardName});
        st.adminCommand({moveChunk: fullNs, find: {numForPartition: 50}, to: st.shard2.shardName});
        st.adminCommand({moveChunk: fullNs, find: {numForPartition: 150}, to: st.shard3.shardName});
    }

    function _startCausalWriterClient(st) {
        jsTestLog("Starting causal writer client");

        // This 1st write goes to shard 0, 2nd goes to shard 1, ..., Nth goes to shard N-1, and
        // then N+1th goes to shard 0 again, ...
        const numFallsIntoShard = [-150, -50, 50, 150];
        let docId = 0;

        // Make sure some writes are persistent on disk in order to have a meaningful checkpoint
        // to backup.
        while (docId < 100) {
            assert.commandWorked(st.getDB(dbName)[collName].insert({
                shardId: docId % numShards,
                numForPartition: numFallsIntoShard[docId % numShards],
                docId: docId
            }));
            docId++;
        }
        for (let i = 0; i < numShards; i++) {
            const shard = st["rs" + i];
            shard.awaitReplication();
            for (let node of shard.nodes) {
                assert.commandWorked(node.getDB(dbName).adminCommand({fsync: 1}));
            }
        }

        const writerClientCmds = function(dbName, collName, numShards) {
            const session = db.getMongo().startSession({causalConsistency: true});
            const sessionColl = session.getDatabase(dbName).getCollection(collName);

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
            `(${writerClientCmds})("${dbName}", "${collName}", ${numShards})`,
            st.s.host);
    }

    function _stopWriterClient(writerPid) {
        jsTestLog("Stopping writer client");

        const writerStatus = checkProgram(writerPid);
        assert(writerStatus.alive,
               "writer client was not running at end of test and exited with code: " +
                   writerStatus.exitCode);
        stopMongoProgramByPid(writerPid);
    }

    function _verifyDataIsCausallyConsistent(mongos, shards) {
        jsTestLog("Verifying data is causally consistent");

        const coll = mongos.getDB(dbName).getCollection(collName);
        const docs = coll.find().toArray();
        const total = docs.length;
        assert.gt(total, 0, "There is no doc.");
        jsTestLog("There are " + total + " documents in total.");

        const docSet = docs.map((v) => v.docId);
        for (let i = 0; i < total; i++) {
            assert(docSet.includes(i), function() {
                print("Full docSet: " + tojson(docSet));
                shards.forEach(function(shard) {
                    print("Shard " + shard.name + ": " +
                          tojson(shard.getPrimary()
                                     .getDB(dbName)
                                     .getCollection(collName)
                                     .find()
                                     .toArray()));
                });

                return "Doc " + i + " is missing";
            });
        }
    }

    function _checkDataConsistency(restoredNodePorts) {
        jsTestLog("Checking data consistency");

        const configRS = new ReplSetTest({
            name: csrsName,
            nodes: [{
                noCleanData: true,
                dbpath: restorePaths[configServerIdx],
                port: restoredNodePorts[configServerIdx]
            }]
        });

        // We need to manually update this field because ReplSetTest assumes the "ports" field is
        // always set by itself.
        configRS.ports = [restoredNodePorts[numShards]];

        jsTestLog("Starting restored Config Server with data from " +
                  restorePaths[configServerIdx] + " at port " + restoredNodePorts[configServerIdx]);
        configRS.startSet({journal: "", configsvr: ""});

        let restoredShards = [];
        for (let i = 0; i < numShards; i++) {
            jsTestLog("Starting restored shard" + i + " with data from " + restorePaths[i] +
                      " at port " + restoredNodePorts[i]);
            restoredShards[i] = new ReplSetTest({
                name: _shardName(i),
                nodes: [{noCleanData: true, dbpath: restorePaths[i], port: restoredNodePorts[i]}],
            });
            restoredShards[i].startSet({shardsvr: ""});
        }

        const mongos = MongoRunner.runMongos({configdb: configRS.getURL()});

        _verifyDataIsCausallyConsistent(mongos, restoredShards);

        jsTestLog("Stopping cluster after checking data consistency");
        MongoRunner.stopMongos(mongos);
        for (let i = 0; i < numShards; i++) {
            restoredShards[i].stopSet();
        }
        configRS.stopSet();
    }

    //////////////////////////////////////////////////////////////////////////////////////
    /////////////////// Helper functions for detecting topology changes //////////////////
    //////////////////////////////////////////////////////////////////////////////////////

    function _getTopologyInfo(configServer, afterClusterTime) {
        const res = assert.commandWorked(configServer.getDB("config").runCommand({
            find: "shards",
            sort: {_id: 1},
            readConcern: {level: "majority", afterClusterTime: afterClusterTime}
        }));
        return res.cursor.firstBatch;
    }

    function _isTopologyChanged(configServer, oldTopologyInfo, restorePIT) {
        jsTestLog("Checking if topology has changed");

        /**
         *  7.a. After the backup cursor on the config server has been extended, read
         *       "config.shards" with readConcern {level: "majority", afterClusterTime:
         *        maxTimestamp}.
         */
        const newTopologyInfo = _getTopologyInfo(configServer, restorePIT);
        jsTestLog("New Topology Information: " + tojson(newTopologyInfo));

        /**
         *  7.b. Invalidate the backup if the result does not match the topology on which the backup
         *       was run.
         *       If "newTopologyInfo" does not match with "oldTopologyInfo", we can conservatively
         *       assume that the topology change happened between the time we record
         *       "oldTopologyInfo" and "restorePIT".
         */
        if (oldTopologyInfo.length !== newTopologyInfo.length) {
            return true;
        }
        for (let i = 0; i < numShards; i++) {
            if (oldTopologyInfo[i]._id !== newTopologyInfo[i]._id) {
                return true;
            }
        }

        /**
         *  7.c. If "newTopologyInfo" matches with "oldTopologyInfo", it is still possible a shard
         *       was added before the "restorePIT" and removed after it.  If this is the case, the
         *       topology at the "oldTopologyInfo" time is a subset of the topology at the
         *       "restorePIT" (do not match).  We again conservatively assume that any presence
         *       of "removeShard" operations between "restorePIT" and now indicates this happened.
         *       Invalidate the backup if there was a "removeShard" operation.
         */
        const oplogEntries =
            configServer.getDB("local").oplog.rs.find({"ts": {$gt: restorePIT}}).toArray();
        jsTestLog("Oplog entries after restorePIT " + restorePIT + ": " + tojson(oplogEntries));
        for (let oplog of oplogEntries) {
            if (oplog.ns === "config.shards" && oplog.op === "d") {
                jsTestLog("A 'removeShard' oplog entry has been detected: " + oplog);
                return true;
            }
        }
        return false;
    }

    function _getNodesToBackup(topologyInfo, mongos) {
        load('jstests/libs/discover_topology.js');
        const topology = DiscoverTopology.findConnectedNodes(mongos);
        jsTestLog("Topology: " + tojson(topology));
        assert.eq(topology.type, Topology.kShardedCluster);

        // Sort the shard list by shard name.
        const shards = Object.keys(topology.shards).map(function(key) {
            return {name: key, nodes: topology.shards[key].nodes};
        });
        shards.sort((a, b) => (bsonWoCompare(a.name, b.name)));
        jsTestLog("Sorted shards: " + tojson(shards));

        const nodes = [
            new Mongo(shards[0].nodes[1]),
            new Mongo(shards[1].nodes[2]),
            new Mongo(shards[2].nodes[0]),
            new Mongo(shards[3].nodes[1]),
            new Mongo(topology.configsvr.nodes[1]),
        ];
        nodes.forEach((node) => node.setSlaveOk());
        return nodes;
    }

    //////////////////////////////////////////////////////////////////////////////////////
    //////////////////////////////// Backup Specification ////////////////////////////////
    //////////////////////////////////////////////////////////////////////////////////////

    function _createBackup(st) {
        jsTestLog("Creating backup");

        /**
         *  1. Take note of the topology configuration of the entire cluster.
         */
        const startTime = Timestamp(1, 1);  // This time is definitely before the backup starts.
        const initialTopology = _getTopologyInfo(st.config1, startTime);
        const nodesToBackup = _getNodesToBackup(initialTopology, st.s);
        jsTestLog("Backing up nodes: " + tojson(nodesToBackup));

        let copyWorkers = [];
        let heartbeaters = [];
        let backupCursors = [];
        let dbpaths = [];
        let backupIds = [];
        let maxTimestamp = Timestamp();
        let stopCounter = new CountDownLatch(1);
        for (let i = 0; i < numShards + 1; i++) {
            jsTestLog("Backing up shard" + i);

            let metadata;
            /**
             *  2. Open up a $backupCursor on a node (primary or secondary) of each shard and one
             *     config server node.
             */
            backupCursors[i] = openBackupCursor(nodesToBackup[i]);
            metadata = getBackupCursorMetadata(backupCursors[i]);
            assert("checkpointTimestamp" in metadata);

            /**
             *  3. Spawn a copy worker thread to copy the data files for each node.
             */
            const copyThread =
                copyBackupCursorFiles(backupCursors[i], metadata["dbpath"], restorePaths[i], true);
            jsTestLog("Opened up backup cursor on " + nodesToBackup[i] + ": " + tojson(metadata));
            dbpaths[i] = metadata.dbpath;
            backupIds[i] = metadata.backupId;

            /**
             *  4. Get the `maxTimestamp` given all the checkpoint timestamps returned by
             *     $backupCursor.
             */
            let checkpointTimestamp = metadata.checkpointTimestamp;
            if (timestampCmp(checkpointTimestamp, maxTimestamp) > 0) {
                maxTimestamp = checkpointTimestamp;
            }
            copyWorkers.push(copyThread);
            heartbeaters.push(startHeartbeatThread(nodesToBackup[i].host,
                                                   backupCursors[i],
                                                   nodesToBackup[i].getDB("admin").getSession(),
                                                   stopCounter));
        }

        concurrentWorkWhileBackup.setup();
        concurrentWorkWhileBackup.runBeforeExtend(st.s);

        /**
         *  5. Call $backupCursorExtend on each node and copy additional files.
         */
        for (let i = 0; i < numShards + 1; i++) {
            jsTestLog("Extending backup cursor for shard" + i);
            let cursor = extendBackupCursor(nodesToBackup[i], backupIds[i], maxTimestamp);
            let thread = copyBackupCursorExtendFiles(cursor, dbpaths[i], restorePaths[i], true);
            copyWorkers.push(thread);
            cursor.close();
        }

        concurrentWorkWhileBackup.runAfterExtend(st.s);

        jsTestLog("Joining threads");

        /**
         *  6. Wait until all the copy worker threads have done their work.
         */
        copyWorkers.forEach((thread) => {
            thread.join();
        });

        stopCounter.countDown();
        heartbeaters.forEach((heartbeater) => {
            heartbeater.join();
        });

        /**
         *  7. Check if the backup must be invalidated due to a topology change.
         */
        if (_isTopologyChanged(st.config1, initialTopology, maxTimestamp)) {
            throw "Sharding topology has been changed during backup.";
        }

        /**
         *  8. Close $backupCursor on each node. (This test closes all the cursors in the end out of
         *     convenience and there is no correctness reason to postpone closing a backup cursor.
         *     The best practice in real application is to close the backup cursor on a node
         *     immediately after that node's files have been copied.)
         */
        backupCursors.forEach((cursor) => {
            cursor.close();
        });

        jsTestLog("The data of sharded cluster at timestamp " + maxTimestamp +
                  " has been successfully backed up at " + tojson(restorePaths));
        return maxTimestamp;
    }

    //////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////// Restore Specification ////////////////////////////////
    //////////////////////////////////////////////////////////////////////////////////////

    function _restoreReplicaSet(restorePath, restoredNodePort, backupPointInTime) {
        /**
         * 2. Start each shard member as a standalone on an ephemeral port,
         *      - without auth
         *      - with setParameter.ttlMonitorEnabled=false
         *      - with no sharding.clusterRole value
         *      - with setParameter.disableLogicalSessionCacheRefresh=true
         *
         * For simplicity, we restore to a single node replica set. In practice we'd want to
         * do the same procedure on each shard server in the destination cluster.
         */
        let conn = MongoRunner.runMongod({
            dbpath: restorePath,
            noCleanData: true,  // Do not delete existing data on startup.
            setParameter: {ttlMonitorEnabled: false, disableLogicalSessionCacheRefresh: true}
        });

        /**
         * 3. Remove all documents in `local.system.replset`.
         */
        const localDb = conn.getDB("local");
        const originalConfig = localDb.system.replset.findOne();
        assert.commandWorked(localDb.system.replset.remove({}));

        /**
         * 4. Drop `local.replset.oplogTruncateAfterPoint` and `local.replset.minvalid`.
         */
        localDb.replset.oplogTruncateAfterPoint.drop();
        localDb.replset.minvalid.drop();

        /**
         * 5. Insert the proper document into `local.replset.oplogTruncateAfterPoint`.
         */
        const truncateAfterPoint =
            Timestamp(backupPointInTime.getTime(), backupPointInTime.getInc());
        assert.commandWorked(localDb.replset.oplogTruncateAfterPoint.insert(
            {_id: "oplogTruncateAfterPoint", "oplogTruncateAfterPoint": truncateAfterPoint}));
        const firstOplogToRemove = localDb.oplog.rs.find({ts: {$gte: truncateAfterPoint}})
                                       .sort({ts: 1})
                                       .limit(1)
                                       .toArray()[0];
        jsTestLog(restorePath + ": Truncating all the oplog after " + truncateAfterPoint +
                  " (not inclusive), starting after oplog entry: " + tojson(firstOplogToRemove));

        /**
         * 6. Insert the minValid document.
         *    The goal is to use a timestamp that is less than all other timestamps that would
         *    appear in a running system. We use Timestamp(0, 1) because MongoDB internally
         *    converts a null timestamp, Timestamp(0, 0), to the current time.
         */
        assert.commandWorked(localDb.replset.minvalid.insert(
            {_id: ObjectId(), t: NumberLong(-1), ts: Timestamp(0, 1)}));

        /**
         * 7. Manually create a document in `local.system.replset`,
         *      - Set `members` array to the target cluster members
         *      - Set `protocolVersion: 1`
         */
        // Make a copy of the original config so we do not modify it.
        let newConfig = Object.extend({}, originalConfig, true);
        newConfig.members = [{_id: NumberInt(0), host: "localhost:" + restoredNodePort}];
        newConfig.protocolVersion = 1;
        assert.commandWorked(localDb.system.replset.insert(newConfig));

        /**
         * Steps 8+ are for shard and CSRS members only. Restoring a single replica set should
         * stop here.
         * -----------------------------------------------------------------------------------
         */

        /**
         * 8. Restart each node again as a standalone with `recoverFromOplogAsStandalone`
         *    and `takeUnstableCheckpointOnShutdown`.
         */
        MongoRunner.stopMongod(conn, {noCleanData: true});

        jsTestLog(restorePath + ": Replaying oplog to timestamp " + backupPointInTime);
        conn = MongoRunner.runMongod({
            dbpath: restorePath,
            noCleanData: true,
            setParameter:
                {recoverFromOplogAsStandalone: true, takeUnstableCheckpointOnShutdown: true}
        });
        assert.neq(conn, null);

        /**
         * 9. Once the mongod is up and accepting connections, shut down via the normal mechanism,
         *    and then restart as a standalone without the two flags above.
         */
        MongoRunner.stopMongod(conn, {noCleanData: true});
        return MongoRunner.runMongod({dbpath: restorePath, noCleanData: true});
    }

    function _restoreCSRS(restorePath, restoredCSRSPort, restoredNodePorts, backupPointInTime) {
        jsTestLog("Restoring CSRS at " + restorePath + " for port " + restoredCSRSPort);

        const conn = _restoreReplicaSet(restorePath, restoredCSRSPort, backupPointInTime);
        const configDb = conn.getDB("config");

        /**
         * 10. Remove all documents from `config.mongos`.
         *     Remove all documents from `config.lockpings`.
         */
        assert.commandWorked(configDb.mongos.remove({}));
        assert.commandWorked(configDb.lockpings.remove({}));

        /**
         * 11. For every sourceShardName document in `config.shards`:
         *      - For every document in `config.databases` with {primary: sourceShardName}:
         *          - Set primary from sourceShardName to destShardName
         *      - For every document in `config.chunks` with {shard: sourceShardName}:
         *          - Set shard from sourceShardName to destShardName
         *          - Set history to []
         *      - For every document in `config.collections` with {primary: sourceShardName}:
         *          - Set primary from sourceShardName to destShardName
         *      - For every document in `config.shards`:
         *          - Remove the document
         *          - Insert new document with:
         *              - _id: destShardName
         *              - hosts: based on the destShardRsId and the hostname:ports of the
         *                       destination cluster.
         */
        // Sort by _id so that the shard order is deterministic.
        const shards = configDb.shards.find().sort({_id: 1}).toArray();
        jsTestLog("Old config.shards: " + tojson(shards));
        const databases = configDb.databases.find().toArray();
        jsTestLog("Old config.databases: " + tojson(databases));
        const collections = configDb.collections.find().toArray();
        jsTestLog("Old config.collections: " + tojson(collections));
        const chunks = configDb.chunks.find().toArray();
        jsTestLog("Old config.chunks: " + tojson(chunks));

        assert.commandWorked(configDb.shards.remove({}));
        for (let i = 0; i < numShards; i++) {
            const sourceShardName = shards[i]._id;
            const destShardName = _shardName(i);
            assert.commandWorked(configDb.database.update({primary: sourceShardName},
                                                          {$set: {primary: destShardName}}));
            assert.commandWorked(configDb.collections.update({primary: sourceShardName},
                                                             {$set: {primary: destShardName}}));
            assert.commandWorked(configDb.chunks.update(
                {shard: sourceShardName}, {$set: {shard: destShardName, history: []}}));
            assert.commandWorked(configDb.shards.insert({
                _id: destShardName,
                host: destShardName + "/localhost:" + restoredNodePorts[i],
            }));
        }

        jsTestLog("New config.shards: " + tojson(configDb.shards.find().sort({_id: 1}).toArray()));
        jsTestLog("New config.databases: " + tojson(configDb.databases.find().toArray()));
        jsTestLog("New config.collections: " + tojson(configDb.collections.find().toArray()));
        jsTestLog("New config.chunks: " + tojson(configDb.chunks.find().toArray()));

        const clusterId = configDb.getCollection('version').findOne();
        jsTestLog("ClusterId doc: " + tojson(clusterId));

        /**
         * 12. Shut down the mongod process cleanly via a shutdown command.
         *     Start the process as a replica set/shard member on regular port.
         */
        MongoRunner.stopMongod(conn, {noCleanData: true});

        return clusterId.clusterId;
    }

    function _restoreShard(restorePath,
                           restoredNodePort,
                           backupPointInTime,
                           clusterId,
                           shardNum,
                           configsvrConnectionString) {
        jsTestLog("Restoring shard at " + restorePath + " for port " + restoredNodePort);

        const conn = _restoreReplicaSet(restorePath, restoredNodePort, backupPointInTime);

        /**
         * 10. Remove the {_id: "minOpTimeRecovery"} from the `admin.system.version` collection.
         */
        const adminDb = conn.getDB("admin");
        assert.commandWorked(adminDb.system.version.remove({_id: "minOpTimeRecovery"}));

        /**
         * 11. Update the {"_id": "shardIdentity"} in `admin.system.version`:
         * "$set": {
         *              "clusterId":                 clusterId,
         *              "shardName":                 destShardName,
         *              "configsvrConnectionString": configsvrConnectionString,
         *          }
         * ...where
         *   - clusterId:
         *      - If restoring to same source cluster:
         *          - Connect to the CSRS primary and gather the following information
         *              - clusterId: from the first document in the config.version collection
         *      - If restoring to a different destination cluster:
         *              - clusterId: generate a new ObjectId to use below (CLOUDP-22958)
         *   - configsvrConnectionString: from the destination cluster
         */
        assert.commandWorked(adminDb.system.version.update({_id: "shardIdentity"}, {
            $set: {
                clusterId: clusterId,
                shardName: _shardName(shardNum),
                configsvrConnectionString: configsvrConnectionString
            }
        }));

        /**
         * 11. Drop `config.cache.collections`.
         *     Drop `config.cache.chunks.*` collections.
         *     Drop `config.cache.databases`.
         */
        const configDb = conn.getDB("config");
        configDb.cache.collections.drop();
        configDb.cache.databases.drop();
        const res = assert.commandWorked(configDb.runCommand(
            {listCollections: 1, nameOnly: true, filter: {name: {$regex: /cache\.chunks\..*/}}}));
        const collInfos = new DBCommandCursor(configDb, res).toArray();
        collInfos.forEach(collInfo => {
            configDb[collInfo.name].drop();
        });

        /**
         * 12. Shut down the mongod process cleanly via a shutdown command.
         *     Start the process as a replica set/shard member on regular port.
         */
        MongoRunner.stopMongod(conn, {noCleanData: true});
    }

    function _restoreFromBackup(restoredNodePorts, backupPointInTime) {
        jsTestLog("Restoring from backup");

        /**
         * The following procedure is used regardless of whether or not the source cluster and the
         * destination cluster are different clusters. The source cluster is the one on which the
         * backups were taken. The destination cluster is the one where the backups are being
         * restored.
         *
         * 1. For every node in the destination cluster,
         *      1.a. Shut down the node, wait until all nodes are down.
         *      1.b. Delete the contents of the dbpath.
         *      1.c. Download and extract the contents of the snapshot.
         *
         * For simplicity, this spec-test skips this step. The backup procedure puts the backups in
         * the destination dbpath.
         */

        const clusterId = _restoreCSRS(restorePaths[configServerIdx],
                                       restoredNodePorts[configServerIdx],
                                       restoredNodePorts,
                                       backupPointInTime);

        const configsvrConnectionString =
            csrsName + "/localhost:" + restoredNodePorts[configServerIdx];
        jsTestLog("configsvrConnectionString: " + configsvrConnectionString);

        for (let i = 0; i < numShards; i++) {
            _restoreShard(restorePaths[i],
                          restoredNodePorts[i],
                          backupPointInTime,
                          clusterId,
                          i,
                          configsvrConnectionString);
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////// Runs the test ////////////////////////////////////
    //////////////////////////////////////////////////////////////////////////////////////

    this.run = function() {
        /**
         *  Setup for backup
         */
        const st = new ShardingTest({
            name: jsTestName(),
            shards: numShards,
            rs: {
                nodes:
                    // Set the secondaries to priority:0 and votes:0 to prevent the primary from
                    // stepping down.
                    [{}, {rsConfig: {priority: 0, votes: 0}}, {rsConfig: {priority: 0, votes: 0}}],
                syncdelay: 1,

                oplogSize: 1,
                setParameter: {writePeriodicNoops: true}
            }
        });

        _setupShardedCollectionForCausalWrites(st, dbName, collName);
        let writerPid = _startCausalWriterClient(st);

        jsTestLog("Resetting db path");
        resetDbpath(MongoRunner.dataPath + "forRestore/");

        /**
         *  Backup
         */
        let failureMessage;
        let backupPointInTime;
        try {
            backupPointInTime = _createBackup(st);
        } catch (e) {
            failureMessage = "Failed to backup: " + e;
            jsTestLog(failureMessage);
        } finally {
            concurrentWorkWhileBackup.teardown();
            _stopWriterClient(writerPid);
            st.stop();
        }
        if (failureMessage !== undefined) {
            return failureMessage;
        }

        /**
         *  Restore
         */
        const restoredNodePorts = allocatePorts(numShards + 1);
        _restoreFromBackup(restoredNodePorts, backupPointInTime);

        /**
         *  Check data consistency
         */
        _checkDataConsistency(restoredNodePorts);

        jsTestLog("Test succeeded");
        return "Test succeeded.";
    };
};
