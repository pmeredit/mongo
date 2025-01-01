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
 *
 * NOTE: Any changes to this file should be reviewed by someone on the cloud automation and/or
 *       backup teams.
 */
import "jstests/libs/parallelTester.js";

import {
    copyBackupCursorExtendFiles,
    copyBackupCursorFiles,
    copyBackupCursorFilesForIncremental,
    extendBackupCursor,
    getBackupCursorMetadata,
    openBackupCursor,
    startHeartbeatThread,
} from "jstests/libs/backup_utils.js";
import {DiscoverTopology, Topology} from "jstests/libs/discover_topology.js";
import {ReplSetTest} from "jstests/libs/replsettest.js";
import {ShardingTest} from "jstests/libs/shardingtest.js";
import {findChunksUtil} from "jstests/sharding/libs/find_chunks_util.js";
import {
    openIncrementalBackupCursor
} from "src/mongo/db/modules/enterprise/jstests/hot_backups/libs/incremental_backup_helpers.js";

export var ShardedBackupRestoreTest = function(concurrentWorkWhileBackup,
                                               isDirectoryPerDb = false,
                                               isWiredTigerDirectoryForIndexes = false,
                                               {configShard} = {}) {
    // When opening a backup cursor, only checkpointed data is backed up. However, the most
    // up-to-date size storer information is used. Thus the fast count may be inaccurate.
    TestData.skipEnforceFastCountOnValidate = true;

    jsTestLog("ShardedBackupRestoreTest: " + tojson({
                  isDirectoryPerDb: isDirectoryPerDb,
                  isWiredTigerDirectoryForIndexes: isWiredTigerDirectoryForIndexes
              }));

    const numShards = 4;
    const dbName = "test";
    const collNameRestored = "continuous_writes_restored";
    const collNameUnrestored = "continuous_writes_unrestored";
    let collUUIDUnrestored;

    let isTimeseries = false;

    const pathsep = _isWindows() ? "\\" : "/";
    const restorePaths = [
        MongoRunner.dataPath + "forRestore" + pathsep + "shard0",
        MongoRunner.dataPath + "forRestore" + pathsep + "shard1",
        MongoRunner.dataPath + "forRestore" + pathsep + "shard2",
        MongoRunner.dataPath + "forRestore" + pathsep + "shard3",
        MongoRunner.dataPath + "forRestore" + pathsep + "config"
    ];
    if (configShard) {
        // With a config shard, the first shard is the config server, so it doesn't need a separate
        // restore path.
        restorePaths.pop();
    }

    // The config shard is always the first shard in this test, if there is one.
    const configServerIdx = configShard ? 0 : numShards;

    function _addServerParams(options, isCSRS, isSelectiveRestore) {
        if (isSelectiveRestore) {
            Object.assign(options, {restore: ''});
        }

        if (!isCSRS) {
            if (isDirectoryPerDb) {
                Object.assign(options, {directoryperdb: ''});
            }
            if (isWiredTigerDirectoryForIndexes) {
                Object.assign(options, {wiredTigerDirectoryForIndexes: ''});
            }
        }
        return options;
    }

    function _addEncryptOptions(options, encryptOptions) {
        if (encryptOptions["enableEncryption"] != undefined) {
            Object.assign(options, encryptOptions);
        }

        return options;
    }

    const csrsName = jsTestName() + (configShard ? "-rs0" : "-configRS");
    function _shardName(shardNum) {
        if (shardNum === configServerIdx) {
            return "config";
        }
        return jsTestName() + "-rs" + shardNum;
    }

    function _replicaSetName(shardNum) {
        if (shardNum === configServerIdx) {
            return csrsName;
        }
        return _shardName(shardNum);
    }

    // Port unused by any of the nodes, to be set after we allocate ports.
    let tmpPort = -1;

    //////////////////////////////////////////////////////////////////////////////////////
    /////////// Helper functions for checking causal consistency of the backup ///////////
    //////////////////////////////////////////////////////////////////////////////////////

    function _setupShardedTimeseriesCollectionForCausalWrites(
        st, dbName, collName, collectionOptions) {
        const fullNs = dbName + "." + collName;
        const fullBucketNs = dbName + ".system.buckets." + collName;
        const metaFieldName = collectionOptions.timeseries.metaField;

        jsTestLog("Setting up sharded time-series collection " + fullNs);

        st.adminCommand({enableSharding: dbName, primaryShard: st.shard0.shardName});

        const mongosDB = st.s.getDB(dbName);
        const mongosColl = mongosDB.getCollection(collName);
        assert.commandWorked(mongosDB.createCollection(collName, collectionOptions));
        assert.commandWorked(mongosColl.createIndex({[metaFieldName]: 1}));

        st.adminCommand({shardCollection: fullNs, key: {[metaFieldName]: 1}});

        // Split the collection into 4 chunks: [MinKey, -100), [-100, 0), [0, 100), [100, MaxKey).
        st.adminCommand({split: fullBucketNs, middle: {meta: -100}});
        st.adminCommand({split: fullBucketNs, middle: {meta: 0}});
        st.adminCommand({split: fullBucketNs, middle: {meta: 100}});

        st.adminCommand({moveChunk: fullBucketNs, find: {meta: -50}, to: st.shard1.shardName});
        st.adminCommand({moveChunk: fullBucketNs, find: {meta: 50}, to: st.shard2.shardName});
        st.adminCommand({moveChunk: fullBucketNs, find: {meta: 150}, to: st.shard3.shardName});
    }

    function _setupShardedCollectionForCausalWrites(st, dbName, collName) {
        const fullNs = dbName + "." + collName;
        jsTestLog("Setting up sharded collection " + fullNs);

        st.adminCommand({enableSharding: dbName, primaryShard: st.shard0.shardName});

        st.adminCommand({shardCollection: fullNs, key: {numForPartition: 1}});

        // Split the collection into 4 chunks: [MinKey, -100), [-100, 0), [0, 100), [100, MaxKey).
        st.adminCommand({split: fullNs, middle: {numForPartition: -100}});
        st.adminCommand({split: fullNs, middle: {numForPartition: 0}});
        st.adminCommand({split: fullNs, middle: {numForPartition: 100}});

        st.adminCommand({moveChunk: fullNs, find: {numForPartition: -50}, to: st.shard1.shardName});
        st.adminCommand({moveChunk: fullNs, find: {numForPartition: 50}, to: st.shard2.shardName});
        st.adminCommand({moveChunk: fullNs, find: {numForPartition: 150}, to: st.shard3.shardName});
    }

    function _startCausalWriterClient(st, collNames, collectionOptions) {
        jsTestLog("Starting causal writer client");

        let timeField = undefined;
        if (isTimeseries) {
            timeField = collectionOptions.timeseries.timeField;
        }

        // This 1st write goes to shard 0, 2nd goes to shard 1, ..., Nth goes to shard N-1, and
        // then N+1th goes to shard 0 again, ...
        const numFallsIntoShard = [-150, -50, 50, 150];
        let docId = 0;

        // Make sure some writes are persistent on disk in order to have a meaningful checkpoint
        // to backup.
        while (docId < 100) {
            let doc = {
                shardId: docId % numShards,
                numForPartition: numFallsIntoShard[docId % numShards],
                docId: docId,
            };

            if (timeField) {
                Object.assign(doc, {[timeField]: new Date()});
            }

            for (const collName of collNames) {
                assert.commandWorked(st.getDB(dbName)[collName].insert(doc));
            }
            docId++;
        }
        for (let i = 0; i < numShards; i++) {
            const shard = st["rs" + i];
            shard.awaitReplication();
            for (let node of shard.nodes) {
                assert.commandWorked(node.getDB(dbName).adminCommand({fsync: 1}));
            }
        }

        const writerClientCmds = function(dbName, collNamesStr, numShards, timeField = undefined) {
            const session = db.getMongo().startSession({causalConsistency: true});

            let sessionColls = [];
            const collNames = collNamesStr.split(",");
            for (const collName of collNames) {
                sessionColls.push(session.getDatabase(dbName).getCollection(collName));
            }

            const numFallsIntoShard = [-150, -50, 50, 150];
            let docId = 100;

            // Run indefinitely.
            const largeStr = "a".repeat(
                1 * 1024 * 256);  // 256KB, give incremental backups more incremental work.
            while (1) {
                let doc = {
                    shardId: docId % numShards,
                    numForPartition: numFallsIntoShard[docId % numShards],
                    docId: docId,
                    str: largeStr
                };

                if (timeField) {
                    Object.assign(doc, {[timeField]: new Date()});
                }

                for (const sessionColl of sessionColls) {
                    assert.commandWorked(sessionColl.insert(doc));
                }
                docId++;
            }
        };

        let shellPath = MongoRunner.getMongoShellPath();
        return startMongoProgramNoConnect(
            shellPath,
            '--eval',
            `(${writerClientCmds})("${dbName}", "${collNames}", ${numShards}, "${timeField}")`,
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

        const coll = mongos.getDB(dbName).getCollection(collNameRestored);
        const docs = coll.find().toArray();
        const total = docs.length;
        assert.gt(total, 0, "There is no doc.");
        jsTestLog("There are " + total + " documents in total.");

        // Confirm that each document has a unique 'docId' between '0' and 'total-1'.
        const docSet = docs.map((v) => v.docId);
        for (let i = 0; i < total; i++) {
            assert(docSet.includes(i), function() {
                print("Full docSet: " + tojson(docSet));
                shards.forEach(function(shard) {
                    print("Shard " + shard.name + ": " +
                          tojson(shard.getPrimary()
                                     .getDB(dbName)
                                     .getCollection(collNameRestored)
                                     .find()
                                     .toArray()));
                });

                return "Doc " + i + " is missing";
            });
        }
        return total;
    }

    // lastDocID is the largest docID inserted in the restored oplog entries. This ensures that the
    // data reflects the point in time the user requested (if PIT restore is specified).
    function _checkDataConsistency(restoredNodePorts,
                                   lastDocID,
                                   backupBinaryVersion,
                                   isIncrementalBackup,
                                   collectionOptions,
                                   encryptOptions) {
        jsTestLog("Checking data consistency");

        let options = {
            noCleanData: true,
            dbpath: restorePaths[configServerIdx],
            port: restoredNodePorts[configServerIdx]
        };
        options = _addEncryptOptions(options, encryptOptions);
        const configRS = new ReplSetTest({name: csrsName, nodes: [options]});

        // We need to manually update this field because ReplSetTest assumes the "ports" field is
        // always set by itself.
        configRS.ports = [restoredNodePorts[configServerIdx]];

        const expectedFCV = binVersionToFCV(backupBinaryVersion);

        jsTestLog("Starting restored Config Server with data from " +
                  restorePaths[configServerIdx] + " at port " + restoredNodePorts[configServerIdx]);
        configRS.startSet({configsvr: ""});

        if (encryptOptions["enableEncryption"] != undefined) {
            assert(configRS.getPrimary()
                       .getDB('admin')
                       .runCommand({serverStatus: 1})
                       .encryptionAtRest.encryptionEnabled);
        }

        checkFCV(configRS.getPrimary().getDB('admin'), expectedFCV);

        let restoredShards = [];
        for (let i = 0; i < numShards; i++) {
            if (i === configServerIdx) {
                restoredShards[i] = configRS;
                continue;
            }

            jsTestLog("Starting restored shard" + i + " with data from " + restorePaths[i] +
                      " at port " + restoredNodePorts[i]);
            let options = _addServerParams(
                {noCleanData: true, dbpath: restorePaths[i], port: restoredNodePorts[i]},
                /*isCSRS=*/ false,
                /*isSelectiveRestore=*/ false);
            options = _addEncryptOptions(options, encryptOptions);
            restoredShards[i] = new ReplSetTest({
                name: _replicaSetName(i),
                nodes: [options],
            });
            restoredShards[i].startSet({shardsvr: ""});

            checkFCV(restoredShards[i].getPrimary().getDB('admin'), expectedFCV);
            if (encryptOptions["enableEncryption"] != undefined) {
                assert(restoredShards[i]
                           .getPrimary()
                           .getDB('admin')
                           .runCommand({serverStatus: 1})
                           .encryptionAtRest.encryptionEnabled);
            }

            const indexes = restoredShards[i]
                                .getPrimary()
                                .getDB(dbName)
                                .getCollection(collNameRestored)
                                .getIndexes();
            if (isIncrementalBackup) {
                assert.eq(3, indexes.length);
                assert.eq((isTimeseries ? {
                              [collectionOptions.timeseries.metaField]: 1,
                              [collectionOptions.timeseries.timeField]: 1
                          }
                                        : {_id: 1}),
                          indexes[0].key);
                assert.eq({numForPartition: 1}, indexes[1].key);
                assert.eq({str: 1}, indexes[2].key);
            } else {
                assert.eq(2, indexes.length);
                assert.eq((isTimeseries ? {
                              [collectionOptions.timeseries.metaField]: 1,
                              [collectionOptions.timeseries.timeField]: 1
                          }
                                        : {_id: 1}),
                          indexes[0].key);
                assert.eq({numForPartition: 1}, indexes[1].key);
            }
        }

        const mongos = MongoRunner.runMongos({configdb: configRS.getURL()});

        const totalDocs = _verifyDataIsCausallyConsistent(mongos, restoredShards);
        if (lastDocID !== undefined) {
            // 'docId's start at 0 so we subtract 1 from 'totalDocs'.
            assert.eq(totalDocs - 1, lastDocID, "PIT Restore did not restore to correct PIT");
        }

        jsTestLog("Stopping cluster after checking data consistency");
        MongoRunner.stopMongos(mongos);
        for (let i = 0; i < numShards; i++) {
            if (i === configServerIdx) {
                continue;
            }

            restoredShards[i].stopSet();
        }
        configRS.stopSet();
    }

    function _getLastTimeseriesDocID(restoreOplogEntries) {
        if (!restoreOplogEntries) {
            return undefined;
        }

        function getDocId(entry) {
            // Bucket insert format.
            if (entry.hasOwnProperty("o") && entry.o.hasOwnProperty("control") &&
                entry.o.control.hasOwnProperty("max") &&
                entry.o.control.max.hasOwnProperty("docId")) {
                return entry.o.control.max.docId;
            }

            // DocDiff format.
            if (entry.hasOwnProperty("o") && entry.o.hasOwnProperty("diff") &&
                entry.o.diff.hasOwnProperty("scontrol") &&
                entry.o.diff.scontrol.hasOwnProperty("smax") &&
                entry.o.diff.scontrol.smax.hasOwnProperty("u") &&
                entry.o.diff.scontrol.smax.u.hasOwnProperty("docId")) {
                return entry.o.diff.scontrol.smax.u.docId;
            }

            return undefined;
        }

        let lastDocIdList = [];
        let maxDocID = -1;
        for (let entryList of Object.values(restoreOplogEntries)) {
            // Starting from the end, find the first oplog entry containing docId.
            for (let entry of Array.from(entryList).reverse()) {
                let docId = getDocId(entry);
                if (docId == undefined) {
                    continue;
                }

                assert(entry, () => tojson(restoreOplogEntries));
                lastDocIdList.push(entry);
                maxDocID = Math.max(maxDocID, docId);
                break;
            }
        }

        jsTestLog("Last docID: " + maxDocID + ", last docIds: " + tojson(lastDocIdList));
        if (maxDocID == -1) {
            return undefined;
        }
        return maxDocID;
    }

    function _getLastDocID(restoreOplogEntries) {
        if (!restoreOplogEntries) {
            return undefined;
        }

        let lastDocIdList = [];
        let maxDocID = -1;
        for (let entryList of Object.values(restoreOplogEntries)) {
            // Starting from the end, find the first oplog entry containing docId.
            for (let entry of Array.from(entryList).reverse()) {
                if (!entry.hasOwnProperty("o") || !entry.o.hasOwnProperty("docId")) {
                    continue;
                }

                assert(entry, () => tojson(restoreOplogEntries));
                lastDocIdList.push(entry);
                maxDocID = Math.max(maxDocID, entry.o.docId);
                break;
            }
        }

        jsTestLog("Last docID: " + maxDocID + ", last docIds: " + tojson(lastDocIdList));
        if (maxDocID == -1) {
            return undefined;
        }
        return maxDocID;
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
         *        restorePIT}.
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
         *       was added before the "restorePIT" and removed after it.  If this is the
         *       case, the topology at the "oldTopologyInfo" time is a subset of the topology at the
         *       "restorePIT" (do not match).  We again conservatively assume that any presence of
         *       "removeShard" operations between "restorePIT" and now indicates this happened.
         *       Invalidate the backup if there was a "removeShard" operation.
         */
        const oplogEntries =
            configServer.getDB("local").oplog.rs.find({"ts": {$gt: restorePIT}}).toArray();
        jsTestLog({
            msg: "Oplog entries after restorePIT ",
            restorePIT: restorePIT,
            "oplogEntries": oplogEntries
        });
        for (let oplog of oplogEntries) {
            if ((oplog.ns === "config.$cmd" || oplog.ns === "admin.$cmd") && oplog.op === "c" &&
                oplog.o && Object.keys(oplog.o)[0] === "applyOps" &&
                Array.isArray(oplog.o.applyOps)) {
                let applyOps = oplog.o.applyOps;
                for (let appliedOp of applyOps) {
                    if (appliedOp.ns === 'config.shards' && appliedOp.op === "d") {
                        jsTestLog("A 'removeShard' oplog entry has been detected: " + appliedOp);
                        return true;
                    }
                }
            }
        }
        return false;
    }

    function _getNodesToBackup(topologyInfo, mongos) {
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
    //////////////////////// Helper functions for PIT Restores ///////////////////////////
    //////////////////////////////////////////////////////////////////////////////////////

    function _getTopOfOplogTS(conn) {
        return conn.getDB('local').oplog.rs.find().sort({$natural: -1}).limit(-1).next()["ts"];
    }

    function _getEarliestTopOfOplog(st, backupPointInTime) {
        let minTimestamp = _getTopOfOplogTS(st.configRS.getPrimary());
        jsTestLog("Top of configRS oplog: " + tojson(minTimestamp));
        for (let i = 0; i < numShards; i++) {
            const oplogTop = _getTopOfOplogTS(st["rs" + i].getPrimary());
            jsTestLog("Top of rs" + i + " oplog: " + tojson(oplogTop));
            if (timestampCmp(oplogTop, minTimestamp) < 0) {
                minTimestamp = oplogTop;
            }
        }
        jsTestLog("Minimum top of oplog is " + tojson(minTimestamp));

        assert.gte(minTimestamp, backupPointInTime);
        return minTimestamp;
    }

    function _getRestoreOplogEntriesForSet(replSet, backupPointInTime, restorePointInTime) {
        return replSet.getPrimary()
            .getDB('local')
            .oplog.rs.find({ts: {$gt: backupPointInTime, $lte: restorePointInTime}})
            .sort({$natural: 1})
            .toArray();
    }

    function _getRestoreOplogEntries(st, backupPointInTime, restorePointInTime) {
        let restoreOplogEntries = {};
        for (let i = 0; i < numShards; i++) {
            restoreOplogEntries[st["rs" + i].name] =
                _getRestoreOplogEntriesForSet(st["rs" + i], backupPointInTime, restorePointInTime);
        }
        restoreOplogEntries[st.configRS.name] =
            _getRestoreOplogEntriesForSet(st.configRS, backupPointInTime, restorePointInTime);
        return restoreOplogEntries;
    }

    /**
     * Sanity check to ensure that the oplog was not truncated (point in time restore will
     * not work if this happens). We cannot use the backupPointInTime because it is not
     * guaranteed to correspond to an oplog entry in every replica set, but checkpointTimestamp
     * will.
     */
    function _oplogTruncated(st, checkpointTimestamps) {
        let replSetsToCheck = [];
        for (let i = 0; i < numShards; i++) {
            replSetsToCheck.push(
                {replSet: st["rs" + i], checkpointTimestamp: checkpointTimestamps[i]});
        }
        replSetsToCheck.push(
            {replSet: st.configRS, checkpointTimestamp: checkpointTimestamps[configServerIdx]});

        for (let i = 0; i < replSetsToCheck.length; i++) {
            const {replSet, checkpointTimestamp} = replSetsToCheck[i];
            const oplog =
                replSet.getPrimary().getDB('local').oplog.rs.findOne({ts: checkpointTimestamp});
            if (!oplog) {
                jsTestLog(`Oplog of replica set with URL: ${replSet.getURL()} has been truncated`);
                return true;
            }
        }

        return false;
    }

    //////////////////////////////////////////////////////////////////////////////////////
    //////////////////////////////// Backup Specification ////////////////////////////////
    //////////////////////////////////////////////////////////////////////////////////////

    function _createBackup(st, isSelectiveRestore, isIncrementalBackup, backupNames) {
        jsTestLog("Creating backup with options: " +
                  tojson({selective: isSelectiveRestore, incremental: isIncrementalBackup}));

        /**
         *  1. Take note of the topology configuration of the entire cluster.
         */
        const startTime = Timestamp(1, 1);  // This time is definitely before the backup starts.
        const initialTopology = _getTopologyInfo(st.config1, startTime);
        const nodesToBackup = _getNodesToBackup(initialTopology, st.s);

        const isFullBackup =
            !isIncrementalBackup || (isIncrementalBackup && backupNames.length == 0);
        jsTestLog("Backing up nodes: " + tojson(nodesToBackup) +
                  ", is full backup: " + isFullBackup);

        let copyWorkers = [];
        let heartbeaters = [];
        let backupCursors = [];
        let dbpaths = [];
        let backupIds = [];
        let checkpointTimestamps = [];
        let maxCheckpointTimestamp = Timestamp();
        let stopCounter = new CountDownLatch(1);
        let filesCopied = [];
        let fileCopiedCallback = (fileCopied) => filesCopied.push(fileCopied);
        // With a config shard, one of the shards is the config server.
        const numNodes = configShard ? numShards : numShards + 1;
        for (let i = 0; i < numNodes; i++) {
            const shardNum = (i === configServerIdx) ? "CSRS" : ("shard" + i);
            jsTestLog("Backing up " + shardNum + " with node " + nodesToBackup[i].host);

            let metadata;
            /**
             *  2. Open up a $backupCursor on a node (primary or secondary) of each shard and one
             *     config server node.
             */
            if (isIncrementalBackup) {
                // 1 MB is the smallest block size we can use.
                let backupOptions = {incrementalBackup: true, blockSize: NumberInt(1)};
                if (!backupNames[i]) {
                    backupOptions.thisBackupName = "a";
                } else {
                    backupOptions.thisBackupName = backupNames[i] + "a";
                    backupOptions.srcBackupName = backupNames[i];

                    // Take a checkpoint against the node we're going to open a incremental
                    // $backupCursor against. This is to give an opportunity to the backup cursor to
                    // report blocks that changed between backups.
                    assert.commandWorked(nodesToBackup[i].getDB("admin").runCommand({fsync: 1}));
                }

                let ret = openIncrementalBackupCursor(nodesToBackup[i], backupOptions);
                backupCursors[i] = ret.backupCursor;
                backupNames[i] = ret.thisBackupName;
            } else {
                backupCursors[i] = openBackupCursor(nodesToBackup[i].getDB("admin"));
            }
            metadata = getBackupCursorMetadata(backupCursors[i]);
            assert("checkpointTimestamp" in metadata);

            /**
             *  3. Spawn a copy worker thread to copy the data files for each node.
             */
            const namespacesToSkip = [];
            if (isSelectiveRestore) {
                namespacesToSkip.push(dbName + "." + collNameUnrestored);
            }

            let copyThread;
            if (isFullBackup) {
                copyThread = copyBackupCursorFiles(backupCursors[i],
                                                   namespacesToSkip,
                                                   metadata["dbpath"],
                                                   restorePaths[i],
                                                   /*async=*/ true,
                                                   fileCopiedCallback);
            } else {
                copyThread = copyBackupCursorFilesForIncremental(
                    backupCursors[i], namespacesToSkip, metadata["dbpath"], restorePaths[i]);
            }

            jsTestLog("Opened up backup cursor on " + nodesToBackup[i] + ": " + tojson(metadata));
            dbpaths[i] = metadata.dbpath;
            backupIds[i] = metadata.backupId;

            /**
             *  4. Get the `maxCheckpointTimestamp` given all the checkpoint timestamps returned by
             *     $backupCursor.
             */
            let checkpointTimestamp = metadata.checkpointTimestamp;
            checkpointTimestamps.push(metadata.checkpointTimestamp);
            if (timestampCmp(checkpointTimestamp, maxCheckpointTimestamp) > 0) {
                maxCheckpointTimestamp = checkpointTimestamp;
            }
            copyWorkers.push(copyThread);
            heartbeaters.push(startHeartbeatThread(nodesToBackup[i].host,
                                                   backupCursors[i],
                                                   nodesToBackup[i].getDB("admin").getSession(),
                                                   stopCounter));
        }

        /**
         *  4.1. Ensure the initial backup completes before extending. This avoids a race where both
         *       the initial and extended set of files for the same backup are copied concurrently.
         */
        copyWorkers.forEach((thread) => { thread.join(); });
        copyWorkers = [];

        concurrentWorkWhileBackup.setup();
        concurrentWorkWhileBackup.runBeforeExtend(st.s);

        /**
         *  5. Call $backupCursorExtend on each node and copy additional files.
         */
        for (let i = 0; i < numNodes; i++) {
            jsTestLog("Extending backup cursor for shard" + i);
            let cursor = extendBackupCursor(nodesToBackup[i], backupIds[i], maxCheckpointTimestamp);
            let thread = copyBackupCursorExtendFiles(
                cursor, /*namespacesToSkip=*/[], dbpaths[i], restorePaths[i], true);
            copyWorkers.push(thread);
            backupCursors.push(cursor);
        }

        concurrentWorkWhileBackup.runAfterExtend(st.s);

        jsTestLog("Joining threads");

        /**
         *  6. Wait until all the copy worker threads have done their work.
         */
        copyWorkers.forEach((thread) => { thread.join(); });

        stopCounter.countDown();
        heartbeaters.forEach((heartbeater) => { heartbeater.join(); });

        /**
         *  7. Check if the backup must be invalidated due to a topology change.
         */
        if (_isTopologyChanged(st.config1, initialTopology, maxCheckpointTimestamp)) {
            throw "Sharding topology has been changed during backup.";
        }

        /**
         *  8. Close $backupCursor on each node. (This test closes all the cursors in the end out of
         *     convenience and there is no correctness reason to postpone closing a backup cursor.
         *     The best practice in real application is to close the backup cursor on a node
         *     immediately after that node's files have been copied.)
         *
         *     Note that cursors are closed in FILO-order so that extended cursors are closed before
         *     the backup cursor they depend on.
         */
        backupCursors.reverse().forEach((cursor) => { cursor.close(); });

        jsTestLog({
            msg: "Sharded cluster has been successfully backed up.",
            maxCheckpointTimestamp: maxCheckpointTimestamp,
            restorePaths: restorePaths,
            filesCopied: filesCopied
        });
        return {
            checkpointTimestamps,
            maxCheckpointTimestamp,
            initialTopology,
            backupNames,
            filesCopied
        };
    }

    //////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////// Restore Specification ////////////////////////////////
    //////////////////////////////////////////////////////////////////////////////////////

    function _restoreReplicaSet(restorePath,
                                setName,
                                restoredNodePort,
                                backupPointInTime,
                                restorePointInTime,
                                restoreOplogEntries,
                                isSelectiveRestore,
                                encryptOptions,
                                filesCopied) {
        const isCSRS = (setName === csrsName);

        /**
         * 2. Start each replica set member as a standalone on an ephemeral port,
         *      - without auth
         *      - with setParameter.ttlMonitorEnabled=false
         *      - with no sharding.clusterRole value
         *      - with setParameter.disableLogicalSessionCacheRefresh=true
         *      - with setParameter.allowUnsafeUntimestampedWrites=true
         *      - with setParameter.wiredTigerSkipTableLoggingChecksOnStartup=true
         * This matches what Cloud does today.
         *
         * For simplicity, we restore to a single node replica set. In practice we'd want to
         * do the same procedure on each shard server in the destination cluster.
         *
         * For selective restores, checks that collNameUnrestored was not restored on each shard
         * server.
         */
        let options = _addServerParams({
            dbpath: restorePath,
            noCleanData: true,  // Do not delete existing data on startup.
            setParameter: {
                ttlMonitorEnabled: false,
                disableLogicalSessionCacheRefresh: true,
                allowUnsafeUntimestampedWrites: true,
                wiredTigerSkipTableLoggingChecksOnStartup: true,
                wiredTigerSkipTableLoggingChecksDuringValidation: true,
            }
        },
                                       isCSRS,
                                       isSelectiveRestore);
        options = _addEncryptOptions(options, encryptOptions);

        let conn = MongoRunner.runMongod(options);

        if (!isCSRS || configShard) {
            const collList = conn.getDB(dbName)
                                 .runCommand({listCollections: 1, filter: {type: "collection"}})
                                 .cursor.firstBatch;
            collList.forEach(function(collInfo) {
                assert.neq(collNameUnrestored, collInfo.name);
            });
        }

        /**
         * 3. Remove all documents in `local.system.replset`.
         */
        const localDb = conn.getDB("local");
        const originalConfig = localDb.system.replset.findOne();
        assert.commandWorked(localDb.system.replset.remove({}));

        /**
         * 4. Drop `local.replset.oplogTruncateAfterPoint`, `local.replset.minvalid`, and
         *    `local.replset.initialSyncId`.
         */
        localDb.replset.oplogTruncateAfterPoint.drop();
        localDb.replset.minvalid.drop();
        localDb.replset.initialSyncId.drop();

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
        jsTestLog(restorePath + ": Truncating all the oplog after " + tojson(truncateAfterPoint) +
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
         * 8. Safely shut down the node.
         */
        MongoRunner.stopMongod(conn, {noCleanData: true});

        if (restorePointInTime) {
            /**
             * PIT-1. Restart node as a single node replica set on an ephemeral port.
             */
            jsTestLog(restorePath + ": Restarting node on temporary port " + tmpPort);
            options = _addServerParams({
                noCleanData: true,
                dbpath: restorePath,
                port: tmpPort,
            },
                                       isCSRS,
                                       isSelectiveRestore);
            options = _addEncryptOptions(options, encryptOptions);
            const tmpSet = new ReplSetTest({
                name: setName,
                nodes: [options],
            });
            if (isCSRS) {
                tmpSet.startSet({configsvr: ""});
            } else {
                // Do not start as a 'shardsvr' so it does not attempt to connect to the CSRS.
                tmpSet.startSet();
            }

            /**
             * PIT-2. Safely shut down the node.
             */
            tmpSet.stopSet(15, true);

            jsTestLog(restorePath + ": Restarting node to insert oplog entries up to " +
                      tojson(restorePointInTime));
            /**
             * PIT-3. Start up the member as a standalone on an ephemeral port
             *      - without auth
             *      - with setParameter.ttlMonitorEnabled=false
             *      - with no sharding.clusterRole value
             *      - with setParameter.disableLogicalSessionCacheRefresh=true
             *      - with setParameter.allowUnsafeUntimestampedWrites=true
             *      - with setParameter.wiredTigerSkipTableLoggingChecksOnStartup=true
             * This matches what Cloud does today.
             */
            options = _addServerParams({
                dbpath: restorePath,
                noCleanData: true,
                setParameter: {
                    ttlMonitorEnabled: false,
                    disableLogicalSessionCacheRefresh: true,
                    allowUnsafeUntimestampedWrites: true,
                    wiredTigerSkipTableLoggingChecksOnStartup: true,
                    wiredTigerSkipTableLoggingChecksDuringValidation: true,
                }
            },
                                       isCSRS,
                                       isSelectiveRestore);
            options = _addEncryptOptions(options, encryptOptions);
            conn = MongoRunner.runMongod(options);

            /**
             * PIT-4. Insert the desired oplog entries from the snapshot time until the desired time
             *        into the node’s `local.oplog.rs` collection
             */
            for (let entry of restoreOplogEntries) {
                assert.commandWorked(conn.getDB('local').oplog.rs.insert(entry));
            }

            /**
             * PIT-5. Safely shut down the node.
             */
            MongoRunner.stopMongod(conn, {noCleanData: true});

            /**
             * PIT-6. Replaying oplog.
             */
            jsTestLog(restorePath + ": Replaying oplog");
            options = _addServerParams({
                dbpath: restorePath,
                noCleanData: true,
                setParameter:
                    {recoverFromOplogAsStandalone: true, takeUnstableCheckpointOnShutdown: true}
            },
                                       isCSRS,
                                       isSelectiveRestore);

            options = _addEncryptOptions(options, encryptOptions);
            conn = MongoRunner.runMongod(options);
            assert.neq(conn, null);
            // Once the mongod is up and accepting connections, safely shut down the node.
            MongoRunner.stopMongod(conn, {noCleanData: true});
        }

        /**
         * Steps 9-10 is for shard and CSRS members if it's a snapshot restore, and only CSRS
         * members if it's a PIT restore.
         *
         * Restoring a single replica set should stop here.
         * -----------------------------------------------------------------------------------
         */
        if (isCSRS || !restorePointInTime) {
            /**
             * 9. Restart each node again as a standalone with `recoverFromOplogAsStandalone`
             *    and `takeUnstableCheckpointOnShutdown`.
             */
            jsTestLog(restorePath + ": Replaying oplog");
            options = _addServerParams({
                dbpath: restorePath,
                noCleanData: true,
                setParameter:
                    {recoverFromOplogAsStandalone: true, takeUnstableCheckpointOnShutdown: true}
            },
                                       isCSRS,
                                       isSelectiveRestore);

            options = _addEncryptOptions(options, encryptOptions);
            conn = MongoRunner.runMongod(options);
            assert.neq(conn, null);

            /**
             * 10. Once the mongod is up and accepting connections, safely shut down the node.
             */
            MongoRunner.stopMongod(conn, {noCleanData: true});
        }

        /**
         * 11. When performing a selective restore, the CSRS needs to perform an additional step to
         * remove any references to unrestored collections from its config collections.
         */
        if (isCSRS && isSelectiveRestore) {
            options = {
                dbpath: restorePath,
                noCleanData: true,
                restore: '',
                setParameter: {
                    ttlMonitorEnabled: false,
                    disableLogicalSessionCacheRefresh: true,
                    allowUnsafeUntimestampedWrites: true,
                    wiredTigerSkipTableLoggingChecksOnStartup: true,
                    wiredTigerSkipTableLoggingChecksDuringValidation: true,
                }
            };
            options = _addEncryptOptions(options, encryptOptions);
            conn = MongoRunner.runMongod(options);
            assert.neq(conn, null);

            // Create "local.system.collections_to_restore" and populate it with collections
            // restored on shards before running "_configsvrRunRestore".
            assert.commandWorked(
                conn.getDB("local").createCollection("system.collections_to_restore"));
            for (const fileCopied of filesCopied) {
                if (fileCopied.ns.length == 0 || fileCopied.uuid.length == 0) {
                    // Skip files not known to the catalog, such as WiredTiger metadata files.
                    continue;
                }

                assert.commandWorked(conn.getDB("local")
                                         .getCollection("system.collections_to_restore")
                                         .insert({ns: fileCopied.ns, uuid: UUID(fileCopied.uuid)}));
            }

            // Removes any references from the CSRS config collections for any collections not
            // present in "local.system.collections_to_restore".
            assert.commandWorked(conn.getDB("admin").runCommand({_configsvrRunRestore: 1}));

            // Remove "local.system.collections_to_restore" after running "_configsvrRunRestore".
            assert(conn.getDB("local").getCollection("system.collections_to_restore").drop());

            MongoRunner.stopMongod(conn, {noCleanData: true});
        }

        /**
         * 12. Start each replica set member as a standalone on an ephemeral port,
         *      - without auth
         *      - with setParameter.ttlMonitorEnabled=false
         *      - with no sharding.clusterRole value
         *      - with setParameter.disableLogicalSessionCacheRefresh=true
         *      - with setParameter.allowUnsafeUntimestampedWrites=true
         *      - with setParameter.wiredTigerSkipTableLoggingChecksOnStartup=true
         * This matches what Cloud does today.
         */
        options = _addServerParams({
            dbpath: restorePath,
            noCleanData: true,
            setParameter: {
                ttlMonitorEnabled: false,
                disableLogicalSessionCacheRefresh: true,
                allowUnsafeUntimestampedWrites: true,
                wiredTigerSkipTableLoggingChecksOnStartup: true,
                wiredTigerSkipTableLoggingChecksDuringValidation: true,
            }
        },
                                   isCSRS,
                                   isSelectiveRestore);
        options = _addEncryptOptions(options, encryptOptions);
        return MongoRunner.runMongod(options);
    }

    function _restoreCSRS(restorePath,
                          restoredCSRSPort,
                          restoredNodePorts,
                          backupPointInTime,
                          restorePointInTime,
                          restoreOplogEntries,
                          isSelectiveRestore,
                          filesCopied,
                          encryptOptions) {
        jsTestLog({
            msg: "Restoring CSRS.",
            restorePath: restorePath,
            port: restoredCSRSPort,
            restorePIT: restorePointInTime,
            backupPointInTime: backupPointInTime,
            isSelectiveRestore: isSelectiveRestore
        });

        const configsvrConnectionString = csrsName + "/localhost:" + restoredCSRSPort;
        jsTestLog("configsvrConnectionString: " + configsvrConnectionString);

        const conn = _restoreReplicaSet(restorePath,
                                        csrsName,
                                        restoredCSRSPort,
                                        backupPointInTime,
                                        restorePointInTime,
                                        restoreOplogEntries,
                                        isSelectiveRestore,
                                        encryptOptions,
                                        filesCopied);
        const configDb = conn.getDB("config");

        /**
         * 13. Remove all documents from `config.mongos`.
         *     Remove all documents from `config.lockpings`.
         */
        assert.commandWorked(configDb.mongos.remove({}));
        assert.commandWorked(configDb.lockpings.remove({}));

        /**
         * 14. For every sourceShardName document in `config.shards`:
         *      - For every document in `config.databases` with {primary: sourceShardName}:
         *          - Set primary from sourceShardName to destShardName
         *      - For every document in `config.chunks` with {shard: sourceShardName}:
         *          - Set shard from sourceShardName to destShardName
         *          - Set history to []
         *      - For every document in `config.collections` with {primary: sourceShardName}:
         *          - Set primary from sourceShardName to destShardName
         *      - For every document in `config.shards`:
         *          - Stash document
         *          - Remove the document
         *          - Insert document with modified:
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
            const destReplSetName = _replicaSetName(i);
            assert.commandWorked(configDb.database.update({primary: sourceShardName},
                                                          {$set: {primary: destShardName}}));
            assert.commandWorked(configDb.collections.update({primary: sourceShardName},
                                                             {$set: {primary: destShardName}}));
            assert.commandWorked(configDb.chunks.update(
                {shard: sourceShardName},
                {$set: {shard: destShardName, history: []}, $unset: {onCurrentShardSince: ""}}));

            shards[i]._id = destShardName;
            shards[i].host = destReplSetName + "/localhost:" + restoredNodePorts[i];
            assert.commandWorked(configDb.shards.insert(shards[i]));
        }

        jsTestLog("New config.shards: " + tojson(configDb.shards.find().sort({_id: 1}).toArray()));
        jsTestLog("New config.databases: " + tojson(configDb.databases.find().toArray()));
        jsTestLog("New config.collections: " + tojson(configDb.collections.find().toArray()));
        jsTestLog("New config.chunks: " + tojson(configDb.chunks.find().toArray()));

        const clusterId = configDb.getCollection('version').findOne();
        jsTestLog("ClusterId doc: " + tojson(clusterId));

        /**
         * 15. For selective restores, verify that none of the config collections contain references
         * to the unrestored collection.
         */
        if (isSelectiveRestore) {
            // These checks should be kept in sync with any changes made to the
            // '_configsvrRunRestore' command.
            jsTestLog("Verifying that the config collections have no references of: " +
                      tojson({nss: collNameUnrestored, uuid: collUUIDUnrestored}));

            assert.eq(0, configDb.chunks.find({uuid: collUUIDUnrestored}).itcount());
            assert.eq(0,
                      configDb.collections.find({_id: collNameUnrestored, uuid: collUUIDUnrestored})
                          .itcount());
            assert.eq(0, configDb.locks.find({_id: collNameUnrestored}).itcount());
            assert.eq(0,
                      configDb.migrationCoordinators
                          .find({nss: collNameUnrestored, collectionUuid: collUUIDUnrestored})
                          .itcount());
            assert.eq(0, configDb.tags.find({ns: collNameUnrestored}).itcount());
            assert.eq(0,
                      configDb.rangeDeletions
                          .find({nss: collNameUnrestored, collectionUuid: collUUIDUnrestored})
                          .itcount());
            assert.eq(0,
                      configDb.getCollection("system.sharding_ddl_coordinators")
                          .find({"_id.namespace": collNameUnrestored})
                          .itcount());
        }

        if (configShard) {
            // Restore shard components for config shard.
            _restoreShardInner(conn, clusterId.clusterId, 0, configsvrConnectionString);
        }

        /**
         * 16. Shut down the mongod process cleanly via a shutdown command.
         *     Start the process as a replica set/shard member on regular port.
         */
        MongoRunner.stopMongod(conn, {noCleanData: true});

        return clusterId.clusterId;
    }

    function _restoreShard(restorePath,
                           restoredNodePort,
                           backupPointInTime,
                           restorePointInTime,
                           restoreOplogEntries,
                           clusterId,
                           shardNum,
                           configsvrConnectionString,
                           isSelectiveRestore,
                           encryptOptions) {
        jsTestLog("Restoring shard at " + restorePath + " for port " + restoredNodePort);

        const conn = _restoreReplicaSet(restorePath,
                                        _replicaSetName(shardNum),
                                        restoredNodePort,
                                        backupPointInTime,
                                        restorePointInTime,
                                        restoreOplogEntries,
                                        isSelectiveRestore,
                                        encryptOptions);

        _restoreShardInner(conn, clusterId, shardNum, configsvrConnectionString);

        /**
         * 16. Shut down the mongod process cleanly via a shutdown command.
         *     Start the process as a replica set/shard member on regular port.
         */
        MongoRunner.stopMongod(conn, {noCleanData: true});
    }

    function _restoreShardInner(conn, clusterId, shardNum, configsvrConnectionString) {
        /**
         * 13. Remove the {_id: "minOpTimeRecovery"} from the `admin.system.version` collection.
         */
        const adminDb = conn.getDB("admin");
        assert.commandWorked(adminDb.system.version.remove({_id: "minOpTimeRecovery"}));

        /**
         * 14. Update the {"_id": "shardIdentity"} in `admin.system.version`:
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
         * 15. Drop `config.cache.collections`.
         *     Drop `config.cache.chunks.*` collections.
         *     Drop `config.cache.databases`.
         */
        const configDb = conn.getDB("config");
        configDb.cache.collections.drop();
        configDb.cache.databases.drop();
        const res = assert.commandWorked(configDb.runCommand(
            {listCollections: 1, nameOnly: true, filter: {name: {$regex: /cache\.chunks\..*/}}}));
        const collInfos = new DBCommandCursor(configDb, res).toArray();
        collInfos.forEach(collInfo => { configDb[collInfo.name].drop(); });
    }

    function _restoreFromBackup(restoredNodePorts,
                                backupPointInTime,
                                restorePointInTime,
                                restoreOplogEntries,
                                isSelectiveRestore,
                                filesCopied,
                                encryptOptions) {
        jsTestLog({
            msg: "Restoring from backup.",
            "Backup PIT": backupPointInTime,
            "Restore PIT": restorePointInTime,
            "Selective Restore": isSelectiveRestore
        });

        /**
         * The following procedure is used regardless of whether or not the source cluster
         * and the destination cluster are different clusters. The source cluster is the
         * one on which the backups were taken. The destination cluster is the one where
         * the backups are being restored.
         *
         * 1. For every node in the destination cluster,
         *      1.a. Shut down the node, wait until all nodes are down.
         *      1.b. Delete the contents of the dbpath.
         *      1.c. Download and extract the contents of the snapshot.
         *
         * For simplicity, this spec-test skips this step. The backup procedure puts the
         * backups in the destination dbpath.
         */

        const csrsRestoreOplogEntries =
            restoreOplogEntries ? restoreOplogEntries[csrsName] : undefined;
        const clusterId = _restoreCSRS(restorePaths[configServerIdx],
                                       restoredNodePorts[configServerIdx],
                                       restoredNodePorts,
                                       backupPointInTime,
                                       restorePointInTime,
                                       csrsRestoreOplogEntries,
                                       isSelectiveRestore,
                                       filesCopied,
                                       encryptOptions);

        const configsvrConnectionString =
            csrsName + "/localhost:" + restoredNodePorts[configServerIdx];
        jsTestLog("configsvrConnectionString: " + configsvrConnectionString);

        for (let i = 0; i < numShards; i++) {
            if (i === configServerIdx) {
                continue;
            }

            const shardRestoreOplogEntries =
                restoreOplogEntries ? restoreOplogEntries[_shardName(i)] : undefined;
            _restoreShard(restorePaths[i],
                          restoredNodePorts[i],
                          backupPointInTime,
                          restorePointInTime,
                          shardRestoreOplogEntries,
                          clusterId,
                          i,
                          configsvrConnectionString,
                          isSelectiveRestore,
                          encryptOptions);
        }
    }

    //////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////// Runs the test ////////////////////////////////////
    //////////////////////////////////////////////////////////////////////////////////////

    // If 'isPitRestore' is specified, does a PIT restore to an arbitrary PIT and checks that the
    // data is consistent.
    this.run = function({
        isPitRestore = false,
        isSelectiveRestore = false,
        isIncrementalBackup = false,
        collectionOptions = {},
        backupBinaryVersion = "latest",
        enableEncryption = undefined,
        encryptionKeyFile = "",
        encryptionCipherMode = "",
    } = {}) {
        const oplogSize = 1024;

        /**
         *  Setup for backup
         */
        let rsOptions = _addServerParams(
            {syncdelay: 1, oplogSize: oplogSize, setParameter: {writePeriodicNoops: true}},
            /*isCSRS=*/ false,
            /*isSelectiveRestore=*/ false);
        let configOptions = {
            binVersion: backupBinaryVersion,
            setParameter: {writePeriodicNoops: true, periodicNoopIntervalSecs: 1},
            syncdelay: 1,
            oplogSize: oplogSize,
        };

        // Check if encryption is enabled, if not pass an empty set to the options.
        let encryptOptions =
            _addEncryptOptions({}, {enableEncryption, encryptionKeyFile, encryptionCipherMode});

        configOptions = _addEncryptOptions(configOptions, encryptOptions);
        rsOptions = _addEncryptOptions(rsOptions, encryptOptions);
        const st = new ShardingTest({
            name: jsTestName(),
            shards: numShards,
            configShard: configShard,
            rs: rsOptions,
            other: {
                mongosOptions: {binVersion: backupBinaryVersion},
                configOptions: configOptions,
                rsOptions: {binVersion: backupBinaryVersion},
            },
        });
        let collNames = [collNameRestored];
        if (isSelectiveRestore) {
            collNames.push(collNameUnrestored);
        }

        isTimeseries = collectionOptions && collectionOptions.hasOwnProperty("timeseries");

        for (const collName of collNames) {
            if (isTimeseries) {
                _setupShardedTimeseriesCollectionForCausalWrites(
                    st, dbName, collName, collectionOptions);
            } else {
                _setupShardedCollectionForCausalWrites(st, dbName, collName);
            }
        }

        if (isSelectiveRestore) {
            // Store the UUID of the collection that will not be restored. This will be used during
            // the CSRS restore to make sure no config collections reference the collection.
            const ns = isTimeseries ? "system.buckets." + collNameUnrestored : collNameUnrestored;
            collUUIDUnrestored =
                st.shard0.getDB(dbName)
                    .runCommand({listCollections: 1, filter: {name: ns, type: "collection"}})
                    .cursor.firstBatch[0]
                    .info.uuid;
        }

        let writerPid = _startCausalWriterClient(st, collNames, collectionOptions);

        jsTestLog("Resetting db path");
        resetDbpath(MongoRunner.dataPath + "forRestore" + pathsep);

        /**
         *  Backup
         */
        let failureMessage;
        let backupPointInTime;
        let restorePointInTime;
        let restoreOplogEntries;
        let lastDocID;
        let filesCopied;
        let backupNames = [];

        const kNumBackups = isIncrementalBackup ? 5 : 1;
        try {
            for (let i = 1; i <= kNumBackups; i++) {
                jsTestLog("Taking backup " + i + " of " + kNumBackups);

                const result =
                    _createBackup(st, isSelectiveRestore, isIncrementalBackup, backupNames);
                const initialTopology = result.initialTopology;
                const checkpointTimestamps = result.checkpointTimestamps;
                backupPointInTime = result.maxCheckpointTimestamp;

                if (i == 1) {
                    // Keep track of files copied from the initial backup only. This test does not
                    // create any additional collections/indexes after this point, except for
                    // incremental backups which creates an additional index.
                    filesCopied = result.filesCopied;

                    // After the initial backup, create an index. The next incremental backup will
                    // be responsible for backing it up.
                    if (isIncrementalBackup) {
                        assert.commandWorked(
                            st.s.getDB(dbName).getCollection(collNameRestored).createIndex({
                                str: 1
                            }));
                    }
                }

                backupNames = result.backupNames;

                if (isPitRestore) {
                    // The common reason for a retry is for the config server to generate an oplog
                    // entry after the test client's inserts have begun. The noop writer ensures
                    // this will occur.
                    assert.soonNoExcept(function() {
                        jsTestLog("Attempting to prepare for PIT restore");
                        restorePointInTime = _getEarliestTopOfOplog(st, backupPointInTime);
                        restoreOplogEntries =
                            _getRestoreOplogEntries(st, backupPointInTime, restorePointInTime);
                        lastDocID = isTimeseries ? _getLastTimeseriesDocID(restoreOplogEntries)
                                                 : _getLastDocID(restoreOplogEntries);

                        return lastDocID !== undefined;
                    }, "PIT restore prepare failed", ReplSetTest.kDefaultTimeoutMS, 1000);

                    if (_oplogTruncated(st, checkpointTimestamps)) {
                        throw new Error(
                            "Oplog has been truncated while getting PIT restore oplog entries.");
                    }

                    if (_isTopologyChanged(st.config1, initialTopology, restorePointInTime)) {
                        throw "Sharding topology has been changed while getting PIT restore oplog " +
                            "entries.";
                    }
                }
            }
        } catch (e) {
            failureMessage = "Failed to backup: " + e;
            jsTestLog(failureMessage + "\n\n" + e.stack);
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
        // With a config shard, one of the shards is the config server.
        const numNodes = configShard ? numShards : numShards + 1;
        const restoredNodePorts = allocatePorts(numNodes);
        tmpPort = Math.max(...restoredNodePorts) + 1;

        _restoreFromBackup(restoredNodePorts,
                           backupPointInTime,
                           restorePointInTime,
                           restoreOplogEntries,
                           isSelectiveRestore,
                           filesCopied,
                           encryptOptions);

        /**
         *  Check data consistency
         */
        _checkDataConsistency(restoredNodePorts,
                              lastDocID,
                              backupBinaryVersion,
                              isIncrementalBackup,
                              collectionOptions,
                              encryptOptions);

        return "Test succeeded.";
    };
};

export var NoopWorker = function() {
    this.setup = function() {};

    this.runBeforeExtend = function(mongos) {};

    this.runAfterExtend = function(mongos) {};

    this.teardown = function() {};
};

export var ChunkMigrator = function() {
    this.setup = function() {};

    this.runBeforeExtend = function(mongos) {
        let shardsInfo = mongos.getDB("config").shards.find().sort({_id: 1}).toArray();
        jsTestLog("Shards Info: " + tojson(shardsInfo));
        let chunksInfo = findChunksUtil.findChunksByNs(mongos.getDB('config'),
                                                       "test.continuous_writes_restored");
        jsTestLog("Chunks Info before migrations: " + tojson(chunksInfo));
        jsTestLog("Migrate the first chunk [MinKey, -100) from shard 0 to shard 2");
        assert.commandWorked(mongos.adminCommand({
            moveChunk: "test.continuous_writes_restored",
            find: {numForPartition: -100000},
            to: shardsInfo[2]._id
        }));
        chunksInfo = mongos.getDB("config")
                         .chunks.find({ns: "test.continuous_writes_restored"})
                         .sort({_id: 1})
                         .toArray();
        jsTestLog("Chunks Info after migrations: " + tojson(chunksInfo));
    };

    this.runAfterExtend = function(mongos) {};

    this.teardown = function() {};
};

export var AddShardWorker = function() {
    this.setup = function() {
        jsTestLog("Starting the extra shard replica set");
        this._rst = new ReplSetTest({nodes: 1});
        this._rst.startSet({shardsvr: ""});
        this._rst.initiate();
    };

    this.runBeforeExtend = function(mongos) {
        jsTestLog("Adding the extra shard to sharded cluster");
        assert.commandWorked(mongos.adminCommand({addshard: this._rst.getURL()}));
    };

    this.runAfterExtend = function(mongos) {};

    this.teardown = function() {
        jsTestLog("Stopping the extra shard replica set");
        this._rst.stopSet();
    };
};

export var AddRemoveShardWorker = function() {
    this.setup = function() {
        jsTestLog("Starting the extra shard replica set");
        this._rst = new ReplSetTest({name: "extraShard", nodes: 1});
        this._rst.startSet({shardsvr: ""});
        this._rst.initiate();
    };

    this.runBeforeExtend = function(mongos) {
        jsTestLog("Adding the extra shard to sharded cluster");
        assert.commandWorked(mongos.adminCommand({addshard: this._rst.getURL()}));
    };

    this.runAfterExtend = function(mongos) {
        jsTestLog("Removing the extra shard to sharded cluster");
        assert.soon(() => {
            let res = assert.commandWorked(mongos.adminCommand({removeShard: "extraShard"}));
            jsTestLog({"removeShard response": res});
            return res.msg == "removeshard completed successfully";
        });
    };

    this.teardown = function() {
        jsTestLog("Stopping the extra shard replica set");
        this._rst.stopSet();
    };
};
