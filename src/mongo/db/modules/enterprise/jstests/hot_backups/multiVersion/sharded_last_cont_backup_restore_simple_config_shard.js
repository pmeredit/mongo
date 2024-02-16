/**
 * A simple case of sharded snapshot backup/restore where the backup was taken with a
 * 'last-continuous' binary version.
 *
 * @tags: [requires_wiredtiger,
 *         requires_persistence]
 */

import {
    NoopWorker,
    ShardedBackupRestoreTest
} from "src/mongo/db/modules/enterprise/jstests/hot_backups/libs/sharded_backup_restore.js";

// TODO(SERVER-86715): Re-enable after SERVER-84548 in 7.3.
// let msg = new ShardedBackupRestoreTest(new NoopWorker(), {configShard: true}).run({
//     isPitRestore: false,
//     isSelectiveRestore: false,
//     backupBinaryVersion: "last-continuous"
// });
// assert.eq(msg, "Test succeeded.");