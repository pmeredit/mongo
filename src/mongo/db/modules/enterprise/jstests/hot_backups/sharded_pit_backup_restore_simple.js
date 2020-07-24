/**
 * A simple case of sharded PIT backup/restore.
 *
 * @tags: [requires_wiredtiger,
 *         requires_journaling,
 *         requires_persistence]
 */

(function() {
"use strict";
load("src/mongo/db/modules/enterprise/jstests/hot_backups/libs/sharded_backup_restore.js");

// TODO (SERVER-49862): Re-enable fast count validation if possible.
TestData.skipEnforceFastCountOnValidate = true;

let NoopWorker = function() {
    this.setup = function() {};

    this.runBeforeExtend = function(mongos) {};

    this.runAfterExtend = function(mongos) {};

    this.teardown = function() {};
};

let msg = new ShardedBackupRestoreTest(new NoopWorker())
              .run({isPitRestore: true, isLastLTSBackup: false});
assert.eq(msg, "Test succeeded.");
}());
