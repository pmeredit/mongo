/**
 * Testing simple cases of sharded snapshot backup/restore with server parameter
 * isWiredTigerDirectoryForIndexes=false and encryption enabled.
 *
 * @tags: [
 *   requires_persistence,
 *   requires_wiredtiger,
 * ]
 */
if (_isWindows()) {
    print("Skipping test on windows");
    quit();
}

import {
    platformSupportsGCM
} from "src/mongo/db/modules/enterprise/jstests/encryptdb/libs/helpers.js";
import {
    NoopWorker,
    ShardedBackupRestoreTest
} from "src/mongo/db/modules/enterprise/jstests/hot_backups/libs/sharded_backup_restore.js";

if (!platformSupportsGCM) {
    print("Skipping test as platform does not support GCM");
    quit();
}
const assetsPath = "src/mongo/db/modules/enterprise/jstests/encryptdb/libs/";
const key = assetsPath + "ekf";
run("chmod", "600", key);

let msg =
    new ShardedBackupRestoreTest(
        new NoopWorker(), /*isDirectoryPerDb=*/ false, /*isWiredTigerDirectoryForIndexes=*/ false)
        .run({
            isPitRestore: false,
            isSelectiveRestore: false,
            backupBinaryVersion: "latest",
            enableEncryption: "",
            encryptionKeyFile: key,
            encryptionCipherMode: "AES256-GCM"
        });
assert.eq(msg, "Test succeeded.");

msg = new ShardedBackupRestoreTest(
          new NoopWorker(), /*isDirectoryPerDb=*/ true, /*isWiredTigerDirectoryForIndexes=*/ false)
          .run({
              isPitRestore: false,
              isSelectiveRestore: false,
              backupBinaryVersion: "latest",
              enableEncryption: "",
              encryptionKeyFile: key,
              encryptionCipherMode: "AES256-GCM"
          });
assert.eq(msg, "Test succeeded.");
