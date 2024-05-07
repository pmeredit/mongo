/**
 * A simple case of sharded PIT selective backup/restore.
 *
 * @tags: [
 *   requires_persistence,
 *   requires_wiredtiger,
 * ]
 */

// TODO(SERVER-90072): Does not currently work on Windows.
if (_isWindows()) {
    print("Skipping test on windows");
    quit();
}

import {
    NoopWorker,
    ShardedBackupRestoreTest
} from "src/mongo/db/modules/enterprise/jstests/hot_backups/libs/sharded_backup_restore.js";

let msg = new ShardedBackupRestoreTest(new NoopWorker(), {
              configShard: true
          }).run({isPitRestore: true, isSelectiveRestore: true, backupBinaryVersion: "latest"});
assert.eq(msg, "Test succeeded.");
