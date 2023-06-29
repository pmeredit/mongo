/**
 * A simple case of sharded snapshot selective backup/restore.
 *
 * @tags: [
 *   requires_persistence,
 *   requires_wiredtiger,
 * ]
 */

import {
    NoopWorker,
    ShardedBackupRestoreTest
} from "src/mongo/db/modules/enterprise/jstests/hot_backups/libs/sharded_backup_restore.js";

let msg = new ShardedBackupRestoreTest(new NoopWorker())
              .run({isPitRestore: false, isSelectiveRestore: true, backupBinaryVersion: "latest"});
assert.eq(msg, "Test succeeded.");