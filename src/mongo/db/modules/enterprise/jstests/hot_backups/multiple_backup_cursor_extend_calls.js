/**
 * Open a backupCursor and verify 'extendBackupCursor' calls result in new log files.
 * If new documents are added to the primaryDB and a 'extendBackupCursor' call is made, we expect
 * the extended backup cursor to create a new log file. Additionally, we maintain a record of the
 * log files and their checkSum (using md5sumFile) to ensure that subsequent 'extendBackupCursor'
 * calls do not change previous log files.
 *
 * @tags: [
 *   requires_persistence,
 *   requires_wiredtiger,
 * ]
 */
import {extendBackupCursor, openBackupCursor} from "jstests/libs/backup_utils.js";
import {ReplSetTest} from "jstests/libs/replsettest.js";

const dbName = "test";
const collName = "coll";

function insertDoc(db, doc) {
    const res = assert.commandWorked(db.runCommand({insert: collName, documents: [doc]}));
    assert(res.hasOwnProperty("operationTime"), tojson(res));
    return res.operationTime;
}

// Expect to read a new log file from 'extendBackupCursor' calls and old backup log files to
// remain unchanged. After extending the backupCursor, we will have an extra log file. We save
// the log file in 'backupLogFileCheckSums' along with the checkSum of the file to ensure that
// future calls to extendBackupCursor leave the logs unchanged.
function assertBackupCursorExtends(rst, backupId, extendTo, backupLogFileCheckSums) {
    jsTestLog("Extending cursor: " + tojson(backupId) + " to timestamp: " + tojson(extendTo));
    const extendCursor = extendBackupCursor(rst.getPrimary(), backupId, extendTo);
    const extendedFiles = extendCursor.toArray();

    let newLogFileCount = 0;
    for (let j in extendedFiles) {
        const extendFile = extendedFiles[j];
        if (!backupLogFileCheckSums[extendFile.filename]) {
            const sumFile = md5sumFile(extendFile.filename);
            backupLogFileCheckSums[extendFile.filename] = sumFile;
            newLogFileCount += 1;
        } else {
            assert.eq(backupLogFileCheckSums[extendFile.filename], md5sumFile(extendFile.filename));
        }
    }
    assert(newLogFileCount > 0);
    extendCursor.close();
}

function assertUpdatedLogsAfterMultipleExtendCalls() {
    const rst = new ReplSetTest({name: "test", nodes: 3});
    rst.startSet();
    rst.initiateWithHighElectionTimeout();
    const primaryDB = rst.getPrimary().getDB(dbName);

    const cursor = openBackupCursor(rst.getPrimary().getDB("admin"));
    const firstBatch = cursor.next();
    const metadata = firstBatch["metadata"];

    const backupId = metadata.backupId;
    let clusterTime = rst.status().$clusterTime.clusterTime;

    // We insert new documents and call 'extendBackupCursor' multiple times and expect to read
    // an extra log file from the cursor and leave the older log files unchanged.
    const backupLogFileCheckSums = {};
    clusterTime = insertDoc(primaryDB, {a: 1});
    assertBackupCursorExtends(rst, backupId, clusterTime, backupLogFileCheckSums);

    clusterTime = insertDoc(primaryDB, {b: 1});
    assertBackupCursorExtends(rst, backupId, clusterTime, backupLogFileCheckSums);

    clusterTime = insertDoc(primaryDB, {c: 1});
    assertBackupCursorExtends(rst, backupId, clusterTime, backupLogFileCheckSums);

    cursor.close();
    rst.stopSet();
}

assertUpdatedLogsAfterMultipleExtendCalls();