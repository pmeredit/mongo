/**
 * A simple case of sharded snapshot selective backup/restore for time-series collections.
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

let msg =
    new ShardedBackupRestoreTest(
        new NoopWorker(), /*isDirectoryPerDb=*/ true, /*isWiredTigerDirectoryForIndexes=*/ true)
        .run({
            isPitRestore: true,
            isSelectiveRestore: true,
            collectionOptions: {timeseries: {timeField: "time", metaField: "numForPartition"}},
            backupBinaryVersion: "latest"
        });
assert.eq(msg, "Test succeeded.");

// TODO(SERVER-89919): Re-enable these test cases in separate files.
// msg = new ShardedBackupRestoreTest(
//           new NoopWorker(), /*isDirectoryPerDb=*/ true, /*isWiredTigerDirectoryForIndexes=*/
//           false) .run({
//               isPitRestore: true,
//               isSelectiveRestore: true,
//               collectionOptions: {timeseries: {timeField: "time", metaField: "numForPartition"}},
//               backupBinaryVersion: "latest"
//           });
// assert.eq(msg, "Test succeeded.");

// msg = new ShardedBackupRestoreTest(
//           new NoopWorker(), /*isDirectoryPerDb=*/ false, /*isWiredTigerDirectoryForIndexes=*/
//           true) .run({
//               isPitRestore: true,
//               isSelectiveRestore: true,
//               collectionOptions: {timeseries: {timeField: "time", metaField: "numForPartition"}},
//               backupBinaryVersion: "latest"
//           });
// assert.eq(msg, "Test succeeded.");
