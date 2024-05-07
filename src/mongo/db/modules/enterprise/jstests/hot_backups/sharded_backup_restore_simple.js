/**
 * A simple case of sharded snapshot backup/restore.
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
    new ShardedBackupRestoreTest(
        new NoopWorker(), /*isDirectoryPerDb=*/ false, /*isWiredTigerDirectoryForIndexes=*/ false)
        .run({isPitRestore: false, isSelectiveRestore: false, backupBinaryVersion: "latest"});
assert.eq(msg, "Test succeeded.");

msg = new ShardedBackupRestoreTest(
          new NoopWorker(), /*isDirectoryPerDb=*/ true, /*isWiredTigerDirectoryForIndexes=*/ false)
          .run({isPitRestore: false, isSelectiveRestore: false, backupBinaryVersion: "latest"});
assert.eq(msg, "Test succeeded.");

// TODO(SERVER-89919): Re-enable these test cases in separate files. Currently run into test
// infra/evergreen limits.

// msg = new ShardedBackupRestoreTest(
//           new NoopWorker(), /*isDirectoryPerDb=*/ false, /*isWiredTigerDirectoryForIndexes=*/
//           true) .run({isPitRestore: false, isSelectiveRestore: false, backupBinaryVersion:
//           "latest"});
// assert.eq(msg, "Test succeeded.");

// msg = new ShardedBackupRestoreTest(
//           new NoopWorker(), /*isDirectoryPerDb=*/ true, /*isWiredTigerDirectoryForIndexes=*/
//           true) .run({isPitRestore: false, isSelectiveRestore: false, backupBinaryVersion:
//           "latest"});
// assert.eq(msg, "Test succeeded.");
