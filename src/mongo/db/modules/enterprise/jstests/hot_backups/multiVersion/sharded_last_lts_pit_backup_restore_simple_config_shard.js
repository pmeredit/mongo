/**
 * A simple case of sharded PIT backup/restore where the backup was taken with a 'last-lts'
 * binary version.
 *
 * @tags: [backport_required_multiversion,
 *         requires_wiredtiger,
 *         requires_persistence]
 */

import {
    NoopWorker,
    ShardedBackupRestoreTest
} from "src/mongo/db/modules/enterprise/jstests/hot_backups/libs/sharded_backup_restore.js";

let msg = new ShardedBackupRestoreTest(new NoopWorker(), {
              configShard: true
          }).run({isPitRestore: true, isSelectiveRestore: false, backupBinaryVersion: "last-lts"});
assert.eq(msg, "Test succeeded.");