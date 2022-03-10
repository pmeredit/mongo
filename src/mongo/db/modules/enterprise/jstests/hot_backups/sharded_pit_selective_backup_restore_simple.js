/**
 * A simple case of sharded PIT selective backup/restore.
 *
 * @tags: [
 *   requires_journaling,
 *   requires_persistence,
 *   requires_wiredtiger,
 * ]
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

let msg = new ShardedBackupRestoreTest(new NoopWorker())
              .run({isPitRestore: true, isSelectiveRestore: true, backupBinaryVersion: "latest"});
assert.eq(msg, "Test succeeded.");
}());
