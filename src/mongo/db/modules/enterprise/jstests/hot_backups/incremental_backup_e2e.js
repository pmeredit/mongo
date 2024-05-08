/**
 * Tests end-to-end WiredTiger incremental backups.
 *
 * While concurrent FSM workloads are running in the background on the primary node, several
 * incremental backups are taken. The backups are verified for correctness by running full
 * validations to verify the on-disk tables integrity and correctness.
 *
 * @tags: [requires_wiredtiger, requires_persistence, requires_replication]
 */
// Windows doesn't guarantee synchronous file operations.
if (_isWindows()) {
    print("Skipping test on windows");
    quit();
}

TestData.skipEnforceFastCountOnValidate = true;

import {validateCollections} from "jstests/hooks/validate_collections.js";
import {
    kSeparator,
    openIncrementalBackupCursor,
    consumeBackupCursor,
    endBackup,
    copyDataFiles,
    startFSMClient,
    stopFSMClient,
} from "src/mongo/db/modules/enterprise/jstests/hot_backups/libs/incremental_backup_helpers.js";

const rst = new ReplSetTest({
    nodes: 1,
    // Fast checkpoints.
    nodeOptions: {
        syncdelay: 1,
        setParameter: {logComponentVerbosity: tojson({storage: {wt: {wtBackup: 2}}})}
    }
});

rst.startSet();
rst.initiate();

const primary = rst.getPrimary();

// Run the FSM workload on the primary throughout the duration of this test.
const fsmPid = startFSMClient(primary.host);

// Use a random block size between 1MB to 16MB. We avoid using any larger block sizes as we aren't
// backing up large datasets and would like to have multiple blocks reported for files.
const blockSize = NumberInt(Math.floor(Math.random() * 16) + 1);

const backupPath = primary.dbpath + kSeparator + "incBackup";
resetDbpath(backupPath);
mkdir(backupPath + kSeparator + "journal");

// An initial full backup is needed that will be used as a basis for future incremental backups.
let thisBackupName = "initialIncBackup-0";
let ret = openIncrementalBackupCursor(
    primary, {incrementalBackup: true, thisBackupName: thisBackupName, blockSize: blockSize});
let backupCursor = ret.backupCursor;
thisBackupName = ret.thisBackupName;

consumeBackupCursor(backupCursor, backupPath);
endBackup(backupCursor);

// With the FSM workload running in the background generating new data, take several incremental
// backups.
const kNumBackups = 10;
for (let backupNum = 0; backupNum < kNumBackups; backupNum++) {
    const nextBackupName = "incBackup-" + backupNum.toString() + "-0";

    jsTestLog({
        backupNum: backupNum.toString(),
        thisBackupName: nextBackupName,
        srcBackupName: thisBackupName,
        blockSize: blockSize
    });

    ret = openIncrementalBackupCursor(primary, {
        incrementalBackup: true,
        thisBackupName: nextBackupName,
        srcBackupName: thisBackupName,
        blockSize: blockSize
    });
    backupCursor = ret.backupCursor;
    thisBackupName = ret.thisBackupName;

    consumeBackupCursor(backupCursor, backupPath);
    endBackup(backupCursor);

    // Make a copy of the backed up data files and start up a binary on them.
    const thisBackupPath = primary.dbpath + kSeparator + thisBackupName;
    resetDbpath(thisBackupPath);
    mkdir(thisBackupPath + kSeparator + "journal");
    copyDataFiles(backupPath, thisBackupPath);

    // Verify the correctness of the backed up data.
    let thisBackupConn = MongoRunner.runMongod({dbpath: thisBackupPath, noCleanData: true});
    assert(thisBackupConn);

    let res = assert.commandWorked(thisBackupConn.adminCommand({listDatabases: 1}));
    const dbNames = res.databases.map(dbInfo => dbInfo.name);
    for (let i = 0; i < dbNames.length; ++i) {
        const dbName = dbNames[i];
        assert.commandWorked(validateCollections(thisBackupConn.getDB(dbName), {full: true}));
    }

    MongoRunner.stopMongod(thisBackupConn);
    resetDbpath(thisBackupPath);
}

stopFSMClient(fsmPid);
rst.stopSet();
