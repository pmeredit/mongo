/**
 * Test the basic operation of a `$backupCursorExtend` aggregation stage.
 * @tags: [requires_wiredtiger,
 *         requires_persistence]
 */

import {getBackupCursorDB} from "jstests/libs/backup_utils.js";
import {openBackupCursor} from "jstests/libs/backup_utils.js";

const backupIdNotExist = UUID();
const binData = BinData(0, "AAAAAAAAAAAAAAAAAAAAAAAAAAAA");
const extendTo = Timestamp(100, 1);
const nullTimestamp = Timestamp();

(function assertBackupCursorExtendOnlyWorksInReplSetMode() {
    const conn = MongoRunner.runMongod();
    const db = getBackupCursorDB(conn);
    const aggBackupCursor = openBackupCursor(db);
    const backupId = aggBackupCursor.next().metadata.backupId;
    assert.commandFailedWithCode(db.runCommand({
        aggregate: 1,
        pipeline: [{$backupCursorExtend: {backupId: backupId, timestamp: extendTo}}],
        cursor: {}
    }),
                                 51016);
    MongoRunner.stopMongod(conn);
})();

let rst = new ReplSetTest({
    name: "aggBackupCursor",
    nodes: 1,
    nodeOptions: {wiredTigerEngineConfigString: "log=(file_max=100K)"}
});
rst.startSet();
rst.initiate();
const db = getBackupCursorDB(rst.getPrimary());

function assertFailedToParse(parameters) {
    assert.commandFailedWithCode(
        db.runCommand({aggregate: 1, pipeline: [{$backupCursorExtend: parameters}], cursor: {}}),
        ErrorCodes.FailedToParse);
}

function assertBackupIdNotFound(parameters) {
    assert.commandFailedWithCode(
        db.runCommand({aggregate: 1, pipeline: [{$backupCursorExtend: parameters}], cursor: {}}),
        51011);
}

function assertSuccessfulExtend(parameters) {
    assert.commandWorked(
        db.runCommand({aggregate: 1, pipeline: [{$backupCursorExtend: parameters}], cursor: {}}));
}

function insertDoc(db, collName, doc) {
    let res = assert.commandWorked(db.runCommand({insert: collName, documents: [doc]}));
    assert(res.hasOwnProperty("operationTime"), tojson(res));
    return res.operationTime;
}

function assertAdditionalJournalFiles(parameters) {
    let res = db.aggregate([{$backupCursorExtend: parameters}]);
    assert(res.hasNext());
    let files = res.toArray();
    jsTestLog("Additional files that need to be copied:");
    for (let i in files) {
        jsTestLog(files[i]);
    }
}

// 1. Extend without a running backup cursor.
assertBackupIdNotFound({backupId: backupIdNotExist, timestamp: extendTo});

// 2. Extend with invalid parameters.
const aggBackupCursor = openBackupCursor(db);
const backupId = aggBackupCursor.next().metadata.backupId;

assertFailedToParse({timestamp: extendTo});                                 // Without backupId.
assertFailedToParse({backupId: 1, timestamp: extendTo});                    // Wrong type backupId.
assertFailedToParse({backupId: binData, timestamp: extendTo});              // Wrong type backupId.
assertBackupIdNotFound({backupId: backupIdNotExist, timestamp: extendTo});  // Invalid backupId.

assertFailedToParse({backupId: backupId});                            // Without timestamp.
assertFailedToParse({backupId: backupId, timestamp: 1});              // Wrong type timestamp.
assertFailedToParse({backupId: backupId, timestamp: nullTimestamp});  // Invalid timestamp.

// With extraneous unknown parameter.
assertFailedToParse({backupId: backupId, timestamp: extendTo, extraParam: 1});

// 3. Successful extend.
assertSuccessfulExtend({backupId: backupId, timestamp: extendTo});
assertSuccessfulExtend({backupId: backupId, timestamp: extendTo});  // Extend again.

// 4. Cannot specify it in view pipeline.
assert.commandFailedWithCode(
    db.createView("a", "b", [{$backupCursorExtend: {backupId: backupId, timestamp: extendTo}}]),
    ErrorCodes.InvalidNamespace);

// 5. Do some writes and verify it returns additional log files.
const collName = "test";
let clusterTime;
jsTestLog(
    "Inserting some documents so that $backupCursorExtend will return additional journal files that need to be copied.");
for (let i = 0; i < 10000; i++) {
    clusterTime = insertDoc(db, collName, {x: i});
}
assertAdditionalJournalFiles({backupId: backupId, timestamp: clusterTime});

// Expected usage is for the tailable $backupCursor to be explicitly killed by the client.
aggBackupCursor.close();
rst.stopSet();