/**
 * Characterizes the ordering and format of the documents returned by the backup cursor.
 *
 * The first document will contain the metadata about the backup.
 * The remainder of the documents will have one of the formats listed below.
 *
 * For non-incremental backups, for incremental backups where the file had no changed blocks and
 * for the first full backup to be used as a basis for future incremental backups:
 * {
 *     filename: String,
 *     fileSize: Number,
 * }
 *
 * For incremental backups where the file had changed blocks:
 * {
 *     filename: String,
 *     fileSize: Number,
 *     offset: Number,
 *     length: Number
 * }
 * If the file had multiple changed blocks, then there will be one document per changed block.
 *
 * @tags: [requires_persistence, requires_replication]
 */
(function() {
'use strict';

const rst = new ReplSetTest({
    nodes: [
        {},
        {
            // Disallow elections on secondary.
            rsConfig: {
                priority: 0,
                votes: 0,
            },
        },
    ]
});
rst.startSet();
rst.initiate();

let primary = rst.getPrimary();
let primaryDB = primary.getDB("test");

let x = 'x'.repeat(5 * 1024 * 1024);
let y = 'y'.repeat(5 * 1024 * 1024);
let z = 'z'.repeat(5 * 1024 * 1024);

// Opening backup cursors can race with taking a checkpoint, so disable checkpoints.
assert.commandWorked(
    primary.adminCommand({configureFailPoint: 'pauseCheckpointThread', mode: 'alwaysOn'}));

try {
    jsTest.log("Testing non-incremental backup document format.");
    let backupCursor =
        primary.getDB("admin").aggregate([{$backupCursor: {incrementalBackup: false}}]);

    let isFirstDoc = true;
    while (backupCursor.hasNext()) {
        let doc = backupCursor.next();

        if (isFirstDoc) {
            assert.eq(true, doc.hasOwnProperty("metadata"));
            assert.eq(false, doc.hasOwnProperty("filename"));
            assert.eq(false, doc.hasOwnProperty("fileSize"));
            assert.eq(false, doc.hasOwnProperty("offset"));
            assert.eq(false, doc.hasOwnProperty("length"));
            isFirstDoc = false;
        } else {
            assert.eq(false, doc.hasOwnProperty("metadata"));
            assert.eq(true, doc.hasOwnProperty("filename"));
            assert.eq(true, doc.hasOwnProperty("fileSize"));
            assert.eq(false, doc.hasOwnProperty("offset"));
            assert.eq(false, doc.hasOwnProperty("length"));
        }
    }

    backupCursor.close();

    // Take the first full incremental backup to be used as a basis for future incremental backups.
    jsTest.log("Testing incremental full backup document format.");
    backupCursor = primary.getDB("admin").aggregate([
        {$backupCursor: {incrementalBackup: true, thisBackupName: "foo", blockSize: NumberInt(16)}}
    ]);

    isFirstDoc = true;
    while (backupCursor.hasNext()) {
        let doc = backupCursor.next();

        if (isFirstDoc) {
            assert.eq(true, doc.hasOwnProperty("metadata"));
            assert.eq(false, doc.hasOwnProperty("filename"));
            assert.eq(false, doc.hasOwnProperty("fileSize"));
            assert.eq(false, doc.hasOwnProperty("offset"));
            assert.eq(false, doc.hasOwnProperty("length"));
            isFirstDoc = false;
        } else {
            assert.eq(false, doc.hasOwnProperty("metadata"));
            assert.eq(true, doc.hasOwnProperty("filename"));
            assert.eq(true, doc.hasOwnProperty("fileSize"));
            assert.eq(false, doc.hasOwnProperty("offset"));
            assert.eq(false, doc.hasOwnProperty("length"));
        }
    }

    backupCursor.close();

    // Insert documents to create changes which incremental backup will make us copy.
    for (let i = 0; i < 25; i++) {
        primaryDB.getCollection("test").insert({x: x, y: y, z: z});
    }

    assert.commandWorked(primaryDB.adminCommand({fsync: 1}));

    jsTest.log("Testing incremental backup document format.");
    backupCursor = primary.getDB("admin").aggregate(
        [{$backupCursor: {incrementalBackup: true, thisBackupName: "bar", srcBackupName: "foo"}}]);

    isFirstDoc = true;
    let hasMatchingDoc = false;
    while (backupCursor.hasNext()) {
        let doc = backupCursor.next();

        if (isFirstDoc) {
            assert.eq(true, doc.hasOwnProperty("metadata"));
            assert.eq(false, doc.hasOwnProperty("filename"));
            assert.eq(false, doc.hasOwnProperty("fileSize"));
            assert.eq(false, doc.hasOwnProperty("offset"));
            assert.eq(false, doc.hasOwnProperty("length"));
            isFirstDoc = false;
        } else {
            assert.eq(false, doc.hasOwnProperty("metadata"));
            assert.eq(true, doc.hasOwnProperty("filename"));
            assert.eq(true, doc.hasOwnProperty("fileSize"));

            // We only inserted documents into one collection, so at least one WiredTiger file
            // should have incremental changes to report, but not all files list will have these
            // properties.
            if (doc.hasOwnProperty("offset") && doc.hasOwnProperty("length")) {
                hasMatchingDoc = true;

                // If the fileSize is the same as the length, then WT is asking us to take a full
                // backup of this file.
                if (Number(doc.fileSize) != Number(doc.length)) {
                    // The length must be strictly less than or equal to 16MB, which was the
                    // specified block size during the full backup.
                    assert.lte(Number(doc.length), 16 * 1024 * 1024);
                    assert.gte(Number(doc.offset), 0);
                } else {
                    assert.eq(Number(doc.offset), 0);
                }
            }
        }
    }

    assert.eq(true, hasMatchingDoc);

    backupCursor.close();
} finally {
    assert.commandWorked(
        primary.adminCommand({configureFailPoint: 'pauseCheckpointThread', mode: 'off'}));
    rst.stopSet();
}
}());
