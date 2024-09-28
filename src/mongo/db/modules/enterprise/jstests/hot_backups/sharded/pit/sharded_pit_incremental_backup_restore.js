/**
 * A simple case of sharded PIT incremental backup/restore.
 *
 * @tags: [
 *   requires_persistence,
 *   requires_wiredtiger,
 *   resource_intensive,
 * ]
 */

if (_isWindows()) {
    print("Skipping test on windows");
    quit();
}

import {
    NoopWorker,
    ShardedBackupRestoreTest
} from "src/mongo/db/modules/enterprise/jstests/hot_backups/libs/sharded_backup_restore.js";

let msg = new ShardedBackupRestoreTest(new NoopWorker()).run({
    isPitRestore: true,
    isSelectiveRestore: false,
    isIncrementalBackup: true,
    backupBinaryVersion: "latest"
});
assert.eq(msg, "Test succeeded.");
