/**
 * This class implements helpers for testing the magic restore process. It wraps a ReplSetTest
 * object and maintains the state of the backup cursor, handles writing objects to named pipes and
 * running the restore process. It exposes some of this state so that individual tests can make
 * specific assertions as needed.
 */
import * as backupUtils from "jstests/libs/backup_utils.js";

export class MagicRestoreUtils {
    constructor({rst, pipeDir, insertHigherTermOplogEntry}) {
        this.rst = rst;
        this.pipeDir = pipeDir;

        this.backupSource = this._selectBackupSource();
        // Data files are backed up from the source into 'backupDbPath'. We'll copy these data files
        // into separate dbpaths for each node, ending with 'restore_{nodeId}'.
        this.backupDbPath = pipeDir + "/backup";
        this.restoreDbPaths = [];
        this.rst.nodes.forEach((node) => {
            const restoreDbPath = pipeDir + "/restore_" + this.rst.getNodeId(node);
            this.restoreDbPaths.push(restoreDbPath);
        });

        // isPit is set when we receive the restoreConfiguration.
        this.isPit = false;
        this.insertHigherTermOplogEntry = insertHigherTermOplogEntry || false;
        // Default high term value.
        this.restoreToHigherTermThan = 100;

        // The replica set config will be the same across nodes in a cluster.
        this.expectedConfig = this.rst.getPrimary().adminCommand({replSetGetConfig: 1}).config;
        // These fields are set during the restore process.
        this.backupCursor = undefined;
        this.backupId = undefined;
        this.checkpointTimestamp = undefined;
        this.pointInTimeTimestamp = undefined;
    }

    /**
     * Helper function that selects the node to use for data files. For single-node sets we'll use
     * the primary, but for multi-node sets we'll use the first secondary. In production, we often
     * retrieve the backup from a secondary node to reduce load on the primary.
     */
    _selectBackupSource() {
        let backupSource;
        if (this.rst.nodes.length === 1) {
            backupSource = this.rst.getPrimary();
            jsTestLog(`Selecting primary ${backupSource.host} as backup source.`);
            return backupSource;
        }
        jsTestLog(`Selecting secondary ${backupSource.host} as backup source.`);
        backupSource = this.rst.getSecondary();
        return backupSource;
    }

    /**
     * Helper function that returns the checkpoint timestamp from the backup cursor. Used in tests
     * that need this timestamp to make assertions about data before and after the backup time.
     */
    getCheckpointTimestamp() {
        return this.checkpointTimestamp;
    }

    /**
     * Helper function that returns the dbpath for the backup. Used to start a regular node after
     * magic restore completes. Parameterizes the dbpath to allow for multi-node clusters.
     */
    getBackupDbPath() {
        return MagicRestoreUtils.parameterizeDbpath(this.restoreDbPaths[0]);
    }

    /**
     * Helper function that returns the expected config after the restore.
     */
    getExpectedConfig() {
        return this.expectedConfig;
    }

    /**
     * Takes a checkpoint and opens the backup cursor on the source. backupCursorOpts is an optional
     * parameter which will be passed to the openBackupCursor call if provided. This function
     * returns the backup cursor metadata object.
     */
    takeCheckpointAndOpenBackup(backupCursorOpts = {}) {
        // Take the initial checkpoint.
        assert.commandWorked(this.backupSource.adminCommand({fsync: 1}));

        // Open a backup cursor on the checkpoint.
        this.backupCursor =
            backupUtils.openBackupCursor(this.backupSource.getDB("admin"), backupCursorOpts);
        // Print the backup metadata document.
        assert(this.backupCursor.hasNext());
        const {metadata} = this.backupCursor.next();
        jsTestLog("Backup cursor metadata document: " + tojson(metadata));
        this.backupId = metadata.backupId;
        this.checkpointTimestamp = metadata.checkpointTimestamp;
        return metadata;
    }

    /**
     * Copies data files from the source dbpath to the backup dbpath.
     */
    copyFiles() {
        resetDbpath(this.backupDbPath);
        // TODO(SERVER-13455): Replace `journal/` with the configurable journal path.
        mkdir(this.backupDbPath + "/journal");
        while (this.backupCursor.hasNext()) {
            const doc = this.backupCursor.next();
            jsTestLog("Copying for backup: " + tojson(doc));
            backupUtils.copyFileHelper({filename: doc.filename, fileSize: doc.fileSize},
                                       this.backupSource.dbpath,
                                       this.backupDbPath);
        }
    }

    /**
     * Copies data files from the source dbpath to the backup dbpath, and then closes the backup
     * cursor. Copies the data files from the backup path to each node's restore db path.
     */
    copyFilesAndCloseBackup() {
        this.copyFiles();
        this.backupCursor.close();
        this.restoreDbPaths.forEach((restoreDbPath) => {
            resetDbpath(restoreDbPath);
            MagicRestoreUtils.copyBackupFilesToDir(this.backupDbPath, restoreDbPath);
        });
    }

    /**
     * Extends the backup cursor, copies the extend files and closes the backup cursor.
     */
    extendAndCloseBackup(mongo, maxCheckpointTs) {
        backupUtils.extendBackupCursor(mongo, this.backupId, maxCheckpointTs);
        backupUtils.copyBackupCursorExtendFiles(this.backupCursor,
                                                [] /*namespacesToSkip*/,
                                                this.backupSource.dbpath,
                                                this.backupDbPath,
                                                false /*async*/);
        this.backupCursor.close();
    }

    // Copies backup cursor data files from directory to another. Makes the destination directory if
    // needed. Used to copy one set of backup files to multiple nodes.
    static copyBackupFilesToDir(source, dest) {
        if (!fileExists(dest)) {
            assert(mkdir(dest).created);
        }
        jsTestLog(`Copying data files from source path ${source} to destination path ${dest}`);
        copyDir(source, dest);
    }

    /**
     * Helper function that generates the magic restore named pipe path for testing. 'pipeDir'
     * is the directory in the filesystem in which we create the named pipe.
     */
    static _generateMagicRestorePipePath(pipeDir) {
        const pipeName = "magic_restore_named_pipe";
        const pipePath = `${pipeDir}/tmp/${pipeName}`;
        if (!fileExists(pipeDir + "/tmp/")) {
            assert(mkdir(pipeDir + "/tmp/").created);
        }
        return {pipeName, pipePath};
    }

    /**
     * Helper function that writes an array of JavaScript objects into a named pipe. 'objs' will be
     * serialized into BSON and written into the named pipe path generated by
     * '_generateMagicRestorePipePath'. The function is static as it is used in passthrough testing
     * as well.
     */
    static writeObjsToMagicRestorePipe(pipeDir, objs, persistPipe = false) {
        const {pipeName, pipePath} = MagicRestoreUtils._generateMagicRestorePipePath(pipeDir);
        _writeTestPipeObjects(pipeName, objs.length, objs, pipeDir + "/tmp/", persistPipe);
        // Creating the named pipe is async, so we should wait until the file exists.
        assert.soon(() => fileExists(pipePath));
    }

    /**
     * Helper function that starts a magic restore node on the 'backupDbPath'. Waits for the process
     * to exit cleanly. The function is static as it is used in passthrough testing as well.
     */
    static runMagicRestoreNode(pipeDir, backupDbPath, options = {}) {
        const {pipePath} = MagicRestoreUtils._generateMagicRestorePipePath(pipeDir);
        // Magic restore will exit the mongod process cleanly. 'runMongod' may acquire a connection
        // to mongod before it exits, and so we wait for the process to exit in the 'assert.soon'
        // below. If mongod exits before we acquire a connection, 'conn' will be null. In this case,
        // if mongod exits with non-zero exit code, the runner will throw a StopError.
        const conn = MongoRunner.runMongod({
            dbpath: backupDbPath,
            noCleanData: true,
            magicRestore: "",
            env: {namedPipeInput: pipePath},
            ...options
        });
        if (conn) {
            assert.soon(() => {
                const res = checkProgram(conn.pid);
                return !res.alive && res.exitCode == MongoRunner.EXIT_CLEAN;
            }, "Expected magic restore to exit mongod cleanly");
        }
    }

    /**
     * Avoids nested loops (some including the config servers and some without) in the test itself.
     */
    static getAllNodes(numShards, numNodes) {
        const result = [];
        for (let rsIndex = 0; rsIndex < numShards; rsIndex++) {
            for (let nodeIndex = 0; nodeIndex < numNodes; nodeIndex++) {
                result.push([rsIndex, nodeIndex]);
            }
        }
        return result;
    }

    /**
     * Helper function that lists databases on the given ShardingTest, calls dbHash for each and
     * returns those as a dbName-to-shardIdx-to-nodeIdx-to-dbHash dictionary.
     */
    static getDbHashes(st, numShards, numNodes, expectedDBCount) {
        const dbHashes = {};
        const res = assert.commandWorked(st.s.adminCommand({"listDatabases": 1, "nameOnly": true}));
        assert.eq(res["databases"].length, expectedDBCount);

        const allNodes = this.getAllNodes(numShards + 1, numNodes);
        for (let dbEntry of res["databases"]) {
            const dbName = dbEntry["name"];
            dbHashes[dbName] = {};
            for (const [rsIndex, nodeIndex] of allNodes) {
                const node = rsIndex < numShards ? st["rs" + rsIndex].nodes[nodeIndex]
                                                 : st.configRS.nodes[nodeIndex];
                const dbHash = node.getDB(dbName).runCommand({dbHash: 1});
                if (dbHashes[dbName][rsIndex] == undefined) {
                    dbHashes[dbName][rsIndex] = {};
                }
                dbHashes[dbName][rsIndex][nodeIndex] = dbHash;
            }
        }

        return dbHashes;
    }

    /**
     * Helper function that compares outputs of dbHash on the given replicaSets against the output
     * of a previous getDbHashes call. The config server replica set can be passed in replicaSets
     * too. Collection names in excludedCollections are excluded from the comparison.
     */
    static checkDbHashes(dbHashes, replicaSets, excludedCollections, numShards, numNodes) {
        for (const [rsIndex, nodeIndex] of this.getAllNodes(numShards + 1, numNodes)) {
            for (const dbName in dbHashes) {
                if (dbHashes[dbName][rsIndex] != undefined) {
                    let dbhash =
                        replicaSets[rsIndex].nodes[nodeIndex].getDB(dbName).runCommand({dbHash: 1});
                    for (let collectionName in dbhash["collections"]) {
                        if (excludedCollections.includes(collectionName)) {
                            continue;
                        }
                        assert.eq(
                            dbhash["collections"][collectionName],
                            dbHashes[dbName][rsIndex][nodeIndex]["collections"][collectionName],
                            `Comparing ${collectionName} in ${dbName} on replicaSets[${
                                rsIndex}], node ${nodeIndex} failed`);
                    }
                }
            }
        }
    }

    /**
     * Replaces the trailing "_0" from the backupPath of the first node of a replica set by "_$node"
     * so startSet uses the correct dbpath for each node in the replica set.
     */
    static parameterizeDbpath(backupPath) {
        return backupPath.slice(0, -1) + "$node";
    }

    /**
     * Retrieves all oplog entries that occurred after the checkpoint timestamp on the source node.
     * Returns an object with the timestamp of the last oplog entry, as well as the oplog
     * entry array
     */
    getEntriesAfterBackup(sourceNode) {
        let oplog = sourceNode.getDB("local").getCollection('oplog.rs');
        const entriesAfterBackup =
            oplog.find({ts: {$gt: this.checkpointTimestamp}}).sort({ts: 1}).toArray();
        return {
            lastOplogEntryTs: entriesAfterBackup[entriesAfterBackup.length - 1].ts,
            entriesAfterBackup
        };
    }

    /**
     * A helper function that makes multiple assertions on the restore node. Helpful to avoid
     * needing to make each individual assertion in each test.
     */
    postRestoreChecks({
        node,
        dbName,
        collName,
        expectedOplogCountForNs,
        opFilter,
        expectedNumDocsSnapshot,
        rolesCollUuid,
        userCollUuid,
        logPath,
        shardLastOplogEntryTs
    }) {
        node.setSecondaryOk();
        const restoredConfig =
            assert.commandWorked(node.adminCommand({replSetGetConfig: 1})).config;
        this._assertConfigIsCorrect(this.expectedConfig, restoredConfig);
        this.assertOplogCountForNamespace(
            node, {ns: dbName + "." + collName, op: opFilter}, expectedOplogCountForNs);
        this._assertMinValidIsCorrect(node);
        this._assertStableCheckpointIsCorrectAfterRestore(node, shardLastOplogEntryTs);
        this._assertCannotDoSnapshotRead(
            node, expectedNumDocsSnapshot /* expectedNumDocsSnapshot */, dbName, collName);
        if (rolesCollUuid && userCollUuid) {
            assert.eq(rolesCollUuid, this.getCollUuid(node, "admin", "system.roles"));
            assert.eq(userCollUuid, this.getCollUuid(node, "admin", "system.users"));
        }
        if (logPath) {
            this._checkRestoreSpecificLogs(logPath);
        }
    }

    /**
     * Performs a find on the oplog for the given name space and asserts that the expected number of
     * entries exists. Optionally takes an op type to filter.
     */
    assertOplogCountForNamespace(node, findObj, expectedNumEntries) {
        const entries =
            node.getDB("local").getCollection('oplog.rs').find(findObj).sort({ts: -1}).toArray();
        assert.eq(entries.length, expectedNumEntries);
    }

    /**
     * Adds the 'restoreToHigherTermThan' field to the restore configuration if this instance is
     * testing the higher term no-op behavior.
     */
    appendRestoreToHigherTermThanIfNeeded(restoreConfiguration) {
        if (this.insertHigherTermOplogEntry) {
            restoreConfiguration.restoreToHigherTermThan = NumberLong(this.restoreToHigherTermThan);
        }
        return restoreConfiguration;
    }

    /**
     * Combines writing objects to the named pipe and running magic restore.
     */
    writeObjsAndRunMagicRestore(restoreConfiguration, entriesAfterBackup, options) {
        this.pointInTimeTimestamp = restoreConfiguration.pointInTimeTimestamp;
        if (this.pointInTimeTimestamp) {
            assert(entriesAfterBackup.length > 0);
            this.isPit = true;
        }
        jsTestLog("Restore configuration: " + tojson(restoreConfiguration));

        // Restore each node in serial.
        this.rst.nodes.forEach((node, idx) => {
            jsTestLog(`Restoring node ${node.host}`);
            MagicRestoreUtils.writeObjsToMagicRestorePipe(
                this.pipeDir, [restoreConfiguration, ...entriesAfterBackup]);
            MagicRestoreUtils.runMagicRestoreNode(this.pipeDir, this.restoreDbPaths[idx], options);
        });
    }

    /**
     * Asserts that two config arguments are equal. If we are testing higher term behavior in the
     * test, modifies the expected term on the source config.
     */
    _assertConfigIsCorrect(srcConfig, dstConfig) {
        // If we passed in a value for the 'restoreToHigherTermThan' field in the restore config, a
        // no-op oplog entry was inserted in the oplog with that term value + 100. On startup, the
        // replica set node sets its term to this value. A new election occurred when the replica
        // set restarted, so we must also increment the term by 1 regardless of if we passed in a
        // higher term value.
        const expectedTerm = this.insertHigherTermOplogEntry ? this.restoreToHigherTermThan + 101
                                                             : srcConfig.term + 1;
        // Make a copy to not modify the object passed by the caller.
        srcConfig = Object.assign({}, srcConfig);
        srcConfig.term = expectedTerm;
        assert.eq(srcConfig, dstConfig);
    }

    /**
     * Asserts that the stable checkpoint timestamp on the restored node is as expected. For a
     * non-PIT restore, the timestamp should be equal to the checkpoint timestamp from the backup
     * cursor. For a PIT restore, it should be equal to the top of the oplog. For any restore that
     * inserts a no-op oplog entry with a higher term, the stable checkpoint timestamp should be
     * equal to the timestamp of that entry.
     */
    _assertStableCheckpointIsCorrectAfterRestore(restoreNode, lastOplogEntryTs = undefined) {
        let lastStableCheckpointTs =
            this.isPit ? this.pointInTimeTimestamp : this.checkpointTimestamp;

        // For a PIT restore on a sharded cluster, the lastStableCheckpointTs of a given shard might
        // be behind the global pointInTimeTimestamp. This allows the caller to specify
        // lastOplogEntryTs instead.
        if (lastOplogEntryTs != undefined) {
            lastStableCheckpointTs = lastOplogEntryTs;
        }

        if (this.insertHigherTermOplogEntry) {
            const oplog = restoreNode.getDB("local").getCollection('oplog.rs');
            const incrementTermEntry =
                oplog.findOne({op: "n", "o.msg": "restore incrementing term"});
            assert(incrementTermEntry);
            assert.eq(incrementTermEntry.t, this.restoreToHigherTermThan + 100);
            // If we've inserted a no-op oplog entry with a higher term during magic restore, we'll
            // have updated the stable timestamp.
            lastStableCheckpointTs = incrementTermEntry.ts;
        }

        // Ensure that the last stable checkpoint is as expected. As the timestamp is greater than
        // 0, this means the magic restore took a stable checkpoint on shutdown.
        const {lastStableRecoveryTimestamp} =
            assert.commandWorked(restoreNode.adminCommand({replSetGetStatus: 1}));
        assert(timestampCmp(lastStableRecoveryTimestamp, lastStableCheckpointTs) == 0,
               `timestampCmp(${tojson(lastStableRecoveryTimestamp)}, ${
                   tojson(lastStableCheckpointTs)}) is ${
                   timestampCmp(lastStableRecoveryTimestamp, lastStableCheckpointTs)}, not 0`);
    }

    /**
     * Assert that the minvalid document has been set correctly in magic restore.
     */
    _assertMinValidIsCorrect(restoreNode) {
        const minValid = restoreNode.getCollection('local.replset.minvalid').findOne();
        assert.eq(minValid,
                  {_id: ObjectId("000000000000000000000000"), t: -1, ts: Timestamp(0, 1)});
    }

    /**
     * Assert that a restored node cannot complete a snapshot read at a timestamp earlier than the
     * last stable checkpoint timestamp.
     */
    _assertCannotDoSnapshotRead(restoreNode, expectedNumDocsSnapshot, db = "db", coll = "coll") {
        const {lastStableRecoveryTimestamp} =
            assert.commandWorked(restoreNode.adminCommand({replSetGetStatus: 1}));
        // A restored node will not preserve any history. The oldest timestamp should be set to the
        // stable timestamp at the end of a non-PIT restore.
        let res = restoreNode.getDB(db).runCommand({
            find: coll,
            readConcern: {
                level: "snapshot",
                atClusterTime: Timestamp(lastStableRecoveryTimestamp.getTime() - 1,
                                         lastStableRecoveryTimestamp.getInc())
            }
        });
        assert.commandFailedWithCode(res, ErrorCodes.SnapshotTooOld);

        // A snapshot read at the last stable timestamp should succeed.
        res = restoreNode.getDB(db).runCommand({
            find: coll,
            readConcern: {level: "snapshot", atClusterTime: lastStableRecoveryTimestamp}
        });
        assert.commandWorked(res);
        assert.eq(res.cursor.firstBatch.length, expectedNumDocsSnapshot);
    }

    /**
     * Get the UUID for a given collection.
     */
    getCollUuid(node, dbName, collName) {
        return node.getDB(dbName).getCollectionInfos({name: collName})[0].info.uuid;
    }

    /**
     * Checks the log file specified at 'logpath', and ensures that it contains logs with the
     * 'RESTORE' component. The function also ensures the first and last log lines are as expected.
     */
    _checkRestoreSpecificLogs(logpath) {
        // When splitting the logs on new lines, the last element will be an empty string, so we
        // should filter it out.
        let logs = cat(logpath)
                       .split("\n")
                       .filter(line => line.trim() !== "")
                       .map(line => JSON.parse(line))
                       .filter(json => json.c === 'RESTORE');
        assert.eq(logs[0].msg, "Beginning magic restore");
        assert.eq(logs[logs.length - 1].msg, "Finished magic restore");
    }
}
