/**
 * Migrate a chunk while the backup is in progress. The data copied should still
 * be causally consistent.
 *
 * @tags: [
 *   requires_persistence,
 *   requires_wiredtiger,
 * ]
 */

import {
    ChunkMigrator,
    ShardedBackupRestoreTest
} from "src/mongo/db/modules/enterprise/jstests/hot_backups/libs/sharded_backup_restore.js";

let msg = new ShardedBackupRestoreTest(new ChunkMigrator()).run();
assert.eq(msg, "Test succeeded.");