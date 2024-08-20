/**
 * All the diffs for a file should be reported consecutively,
 * without the diffs for any other files reported in between.
 *
 * The backup blocks should be returned in the same order as WT returns them.
 *
 * @tags: [
 *  requires_wiredtiger,
 *  requires_replication,
 *  requires_persistence,
 * ]
 */
import {openBackupCursor} from "jstests/libs/backup_utils.js";
import {ReplSetTest} from "jstests/libs/replsettest.js";

const rst = new ReplSetTest({nodes: 1});
rst.startSet();
rst.initiate();

let primary = rst.getPrimary();
let primaryDB = primary.getDB("test");

assert.commandWorked(primaryDB.createCollection("abc"));
let primaryColl = primaryDB.getCollection("abc");

assert.commandWorked(
    primary.adminCommand({configureFailPoint: 'pauseCheckpointThread', mode: 'alwaysOn'}));

let populateData = function(coll) {
    let generateData = function(minLen, maxLen) {
        maxLen = maxLen || minLen;
        var len = Math.floor(Math.random() * (maxLen - minLen)) + minLen;
        var buf = new Array(len).fill().map(() => Math.round(Math.random() * 255));
        return buf;
    };

    const minLen = 0.25 * 1024 * 1024;  // 0.25MB.
    const maxLen = 1 * 1024 * 1024;     // 1MB.
    for (let i = 0; i < 25; i++) {
        assert.commandWorked(coll.insert({x: generateData(minLen, maxLen)}));
    }
};

// Initial data.
populateData(primaryColl);

// Checkpoint
assert.commandWorked(primaryDB.adminCommand({fsync: 1}));

// Initial, full backup.
let backupCursor =
    openBackupCursor(primary.getDB("admin"),
                     {incrementalBackup: true, thisBackupName: "a", blockSize: NumberInt(2)});

while (backupCursor.hasNext()) {
    // Just consume it.
    backupCursor.next();
}

backupCursor.close();

// Add more data.
populateData(primaryColl);

// Another checkpoint
assert.commandWorked(primaryDB.adminCommand({fsync: 1}));

backupCursor = openBackupCursor(
    primary.getDB("admin"),
    {incrementalBackup: true, thisBackupName: "b", srcBackupName: "a", blockSize: NumberInt(2)});

// Skip the metadata document.
assert(backupCursor.hasNext());
backupCursor.next();

let seenFiles = {};

let lastSeenFile = undefined;
let lastSeenOffset = 0;

while (backupCursor.hasNext()) {
    // Make sure not out of order.
    let fileToCopy = backupCursor.next();
    jsTestLog(fileToCopy);

    if (lastSeenFile == undefined) {
        // first file
        lastSeenFile = fileToCopy.filename;
        lastSeenOffset = fileToCopy.offset;
    } else if (lastSeenFile != fileToCopy.filename) {
        // new file
        seenFiles[lastSeenFile] = 1;

        lastSeenFile = fileToCopy.filename;
        lastSeenOffset = fileToCopy.offset;
    } else {
        // Same file. Make sure offset is ascending.
        // Checks blocks within a batch are ordered properly.
        assert.gt(fileToCopy.offset, lastSeenOffset);
        lastSeenOffset = fileToCopy.offset;
    }

    // If we're copy a new file, we shouldn't see an already copied file again.
    // Checks ordering at the boundaries between batches.
    // Failure means batch pre-fetch has caused backup blocks of different files to be
    // served interleaved.
    assert(!seenFiles.hasOwnProperty(fileToCopy.filename),
           "Backup blocks of different files should not be interleaved");
}

backupCursor.close();

assert.commandWorked(
    primary.adminCommand({configureFailPoint: 'pauseCheckpointThread', mode: 'off'}));
rst.stopSet();