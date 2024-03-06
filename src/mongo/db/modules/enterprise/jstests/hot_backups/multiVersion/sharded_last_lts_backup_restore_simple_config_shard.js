/**
 * A simple case of sharded snapshot backup/restore where the backup was taken with a 'last-lts'
 * binary version.
 *
 * @tags: [requires_wiredtiger,
 *         requires_persistence]
 */

(function() {
"use strict";
load("src/mongo/db/modules/enterprise/jstests/hot_backups/libs/sharded_backup_restore.js");

let msg = new ShardedBackupRestoreTest(new NoopWorker(), {
              configShard: true
          }).run({isPitRestore: false, isSelectiveRestore: false, backupBinaryVersion: "last-lts"});
assert.eq(msg, "Test succeeded.");
}());
