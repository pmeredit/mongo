/**
 * Add a shard while the backup is in progress. In this case, topology changes should be detected
 * and the backup will be invalidated.
 *
 * @tags: [requires_wiredtiger,
 *         requires_journaling,
 *         requires_persistence]
 */
load("src/mongo/db/modules/enterprise/jstests/hot_backups/sharded_backup_restore.js");

(function() {
    "use strict";

    let AddShardWorker = function() {
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

    let msg = new ShardedBackupRestoreTest(new AddShardWorker()).run();
    assert.neq(-1, msg.indexOf("Sharding topology has been changed during backup."));
}());
