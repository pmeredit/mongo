/**
 * A simple case of sharded incremental backup/restore with encryption, directoryPerDb and
 * wiredTigerDirectoryForIndexes options enabled.
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

import {
    platformSupportsGCM
} from "src/mongo/db/modules/enterprise/jstests/encryptdb/libs/helpers.js";

if (!platformSupportsGCM) {
    print("Skipping test as platform does not support GCM");
    quit();
}
const assetsPath = "src/mongo/db/modules/enterprise/jstests/encryptdb/libs/";
const key = assetsPath + "ekf";
run("chmod", "600", key);

let msg = new ShardedBackupRestoreTest(new NoopWorker(),
                                       /*isDirectoryPerDb=*/ true,
                                       /*isWiredTigerDirectoryForIndexes=*/ false)
              .run({
                  isPitRestore: false,
                  isSelectiveRestore: false,
                  isIncrementalBackup: true,
                  backupBinaryVersion: "latest",
                  enableEncryption: "",
                  encryptionKeyFile: key,
                  encryptionCipherMode: "AES256-GCM"
              });
assert.eq(msg, "Test succeeded.");
