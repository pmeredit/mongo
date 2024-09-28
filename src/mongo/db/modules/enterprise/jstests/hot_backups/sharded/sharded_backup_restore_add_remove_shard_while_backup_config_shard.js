/**
 * Add a shard and then remove it while the backup is in progress. In this case, topology changes
 * should be detected and the backup will be invalidated.
 *
 * @tags: [
 *   requires_persistence,
 *   requires_wiredtiger,
 * ]
 */
import {
    AddRemoveShardWorker,
    ShardedBackupRestoreTest
} from "src/mongo/db/modules/enterprise/jstests/hot_backups/libs/sharded_backup_restore.js";

let msg = new ShardedBackupRestoreTest(new AddRemoveShardWorker(), {configShard: true}).run();
assert.neq(-1, msg.indexOf("Sharding topology has been changed during backup."));
