/**
 * A simple case of sharded PIT backup/restore.
 *
 * @tags: [
 *   requires_persistence,
 *   requires_wiredtiger,
 * ]
 */

(function() {
"use strict";
load("src/mongo/db/modules/enterprise/jstests/hot_backups/libs/sharded_backup_restore.js");

let msg = new ShardedBackupRestoreTest(new NoopWorker(), {
              configShard: true
          }).run({isPitRestore: true, isSelectiveRestore: false, backupBinaryVersion: "latest"});
assert.eq(msg, "Test succeeded.");
}());
