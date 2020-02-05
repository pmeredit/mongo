/**
 * A simple case of sharded backup/restore.
 *
 * @tags: [requires_wiredtiger,
 *         requires_journaling,
 *         requires_persistence]
 */

(function() {
"use strict";
load("src/mongo/db/modules/enterprise/jstests/hot_backups/libs/sharded_backup_restore.js");

let NoopWorker = function() {
    this.setup = function() {};

    this.runBeforeExtend = function(mongos) {};

    this.runAfterExtend = function(mongos) {};

    this.teardown = function() {};
};

let msg = new ShardedBackupRestoreTest(new NoopWorker()).run();
assert.eq(msg, "Test succeeded.");
}());
