/**
 * Testing more simple cases of sharded snapshot backup/restore with server parameter
 * isWiredTigerDirectoryForIndexes=true.
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

let msg =
    new ShardedBackupRestoreTest(new NoopWorker(),
                                 /*isDirectoryPerDb=*/ false, /*isWiredTigerDirectoryForIndexes=*/
                                 true)
        .run({isPitRestore: false, isSelectiveRestore: false, backupBinaryVersion: "latest"});
assert.eq(msg, "Test succeeded.");

msg = new ShardedBackupRestoreTest(new NoopWorker(),
                                   /*isDirectoryPerDb=*/ true, /*isWiredTigerDirectoryForIndexes=*/
                                   true)
          .run({isPitRestore: false, isSelectiveRestore: false, backupBinaryVersion: "latest"});
assert.eq(msg, "Test succeeded.");