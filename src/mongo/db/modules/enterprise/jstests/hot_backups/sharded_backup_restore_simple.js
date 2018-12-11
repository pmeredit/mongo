/**
 * A simple case of sharded backup/restore.
 *
 * @tags: [requires_wiredtiger,
 *         requires_journaling,
 *         requires_persistence,
 *         requires_majority_read_concern]
 */
load("src/mongo/db/modules/enterprise/jstests/hot_backups/sharded_backup_restore.js");

(function() {
    "use strict";

    let NoopWorker = function() {
        this.setup = function() {};

        this.runBeforeExtend = function(mongos) {};

        this.runAfterExtend = function(mongos) {};

        this.teardown = function() {};
    };

    let msg = new ShardedBackupRestoreTest(new NoopWorker()).run();
    assert.eq(msg, "Test succeeded.");
}());
