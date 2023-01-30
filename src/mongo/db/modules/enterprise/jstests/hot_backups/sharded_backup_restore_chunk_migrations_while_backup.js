/**
 * Migrate a chunk while the backup is in progress. The data copied should still
 * be causally consistent.
 *
 * @tags: [
 *   requires_persistence,
 *   requires_wiredtiger,
 * ]
 */

(function() {
"use strict";
load("src/mongo/db/modules/enterprise/jstests/hot_backups/libs/sharded_backup_restore.js");
load("jstests/sharding/libs/find_chunks_util.js");

let ChunkMigrator = function() {
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

let msg = new ShardedBackupRestoreTest(new ChunkMigrator()).run();
assert.eq(msg, "Test succeeded.");
}());
