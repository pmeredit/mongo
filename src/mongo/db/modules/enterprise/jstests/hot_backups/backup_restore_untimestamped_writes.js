/**
 * Writes to keys in a WiredTiger table when running in a replica set are timestamped. When
 * restoring from a backup, part of the restore procedure starts up a standalone mongod on the data
 * files. Standalone mode performs untimestamped writes. This test ensures we don't trip any
 * WiredTiger assertion about performing untimestamped writes to a key that was previously
 * timestamped.
 *
 * @tags: [
 *     requires_wiredtiger,
 *     requires_persistence,
 *     requires_replication,
 *     # TODO (SERVER-85457) createUser command is not supported by gRPC.
 *     grpc_incompatible,
 * ]
 */
// Windows doesn't guarantee synchronous file operations.
if (_isWindows()) {
    print("Skipping test on windows");
    quit();
}

// Restoring from a backup is considered an unclean shutdown and fast-count can be inaccurate.
TestData.skipEnforceFastCountOnValidate = true;

import {
    kSeparator,
    openIncrementalBackupCursor,
    consumeBackupCursor,
    endBackup
} from "src/mongo/db/modules/enterprise/jstests/hot_backups/libs/incremental_backup_helpers.js";
import {ReplSetTest} from "jstests/libs/replsettest.js";

const rst = new ReplSetTest({nodes: 1});
rst.startSet();
rst.initiate();

const primary = rst.getPrimary();

const backupPath = primary.dbpath + kSeparator + "backup";
resetDbpath(backupPath);
mkdir(backupPath + kSeparator + "journal");

const dbName = "test";
const collName = jsTestName();

let db = primary.getDB(dbName);
let coll = db.getCollection(collName);

// Perform a timestamped write to a key.
assert.commandWorked(coll.insert({x: 1}));

// Additionally, to cover the scenario in HELP-45192, create a user.
primary.getDB("admin").createUser(
    {user: "admin", pwd: "password", roles: [{role: "root", db: "admin"}]});

// Wait for stable timestamp to advance before taking a checkpoint.
assert.commandWorked(
    primary.adminCommand({setDefaultRWConcern: 1, defaultWriteConcern: {w: 1, j: true}}));

// Take a checkpoint.
assert.commandWorked(db.adminCommand({fsync: 1}));

// Take a backup.
let ret = openIncrementalBackupCursor(primary, {});
let backupCursor = ret.backupCursor;
consumeBackupCursor(backupCursor, backupPath);
endBackup(backupCursor);

assert.commandWorked(db.adminCommand({configureFailPoint: "pauseCheckpointThread", mode: "off"}));
rst.stopSet(/*signal=*/ null, /*forRestart=*/ true);

// Restore in standalone mode and perform an untimestamped write to a key that was previously
// timestamped.
let restoreConn = MongoRunner.runMongod({
    dbpath: backupPath,
    noCleanData: true,
    setParameter:
        {wiredTigerSkipTableLoggingChecksOnStartup: true, allowUnsafeUntimestampedWrites: true}
});
assert(restoreConn);

db = restoreConn.getDB(dbName);
coll = db.getCollection(collName);

assert.commandWorked(coll.update({x: 1}, {x: 2}));
assert.commandWorked(coll.remove({}));

// HELP-45192 experienced a fatal assertion when updating a user due to this being an untimestamped
// write to a key previously timestamped.
restoreConn.getDB("admin").updateUser("admin",
                                      {pwd: "password", roles: [{role: "root", db: "admin"}]});

// Check the logs to make sure we got an indication of the possible consequences of these
// untimestamped writes.
checkLog.containsJson(restoreConn, 7692300);

// Skip running validation prior to shutting down. This test makes use of the
// wiredTigerSkipTableLoggingChecksOnStartup parameter, which is not compatible with the validate
// command.
MongoRunner.stopMongod(restoreConn, null, {skipValidation: true});
