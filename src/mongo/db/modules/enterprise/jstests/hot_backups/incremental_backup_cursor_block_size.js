/**
 * Tests that the block size can be set when creating the first full backup to be used as a basis
 * for incremental backups. Incremental backups cannot change the block size once it has been set.
 *
 * @tags: [
 *   requires_persistence,
 *   requires_replication,
 * ]
 */
import {openBackupCursor} from "jstests/libs/backup_utils.js";

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
// This makes testing quicker and more predictable. In production, a poorly interleaved checkpoint
// will return an error, requiring retry.
assert.commandWorked(
    primary.adminCommand({configureFailPoint: 'pauseCheckpointThread', mode: 'alwaysOn'}));

try {
    jsTest.log("Taking a full backup for incremental purposes.");
    let backupCursor =
        openBackupCursor(primary.getDB("admin"),
                         {incrementalBackup: true, thisBackupName: "foo", blockSize: NumberInt(1)});

    while (backupCursor.hasNext()) {
        backupCursor.next();
    }

    backupCursor.close();

    // Insert documents to create changes which incremental backup will make us copy.
    for (let i = 0; i < 25; i++) {
        primaryDB.getCollection("test").insert({x: x, y: y, z: z});
    }

    assert.commandWorked(primaryDB.adminCommand({fsync: 1}));

    jsTest.log("Taking an incremental backup on the previous full backup.");
    // The blockSize is ignored, it only gets set during the full backup.
    backupCursor = openBackupCursor(primary.getDB("admin"), {
        incrementalBackup: true,
        thisBackupName: "bar",
        srcBackupName: "foo",
        blockSize: NumberInt(2048)
    });

    while (backupCursor.hasNext()) {
        let doc = backupCursor.next();

        if (doc.hasOwnProperty("length") && doc.hasOwnProperty("offset")) {
            // If the fileSize is the same as the length, then WT is asking us to take a full
            // backup of this file.
            if (Number(doc.fileSize) != Number(doc.length)) {
                // The length must be strictly less than or equal to 1MB.
                assert.lte(Number(doc["length"]), 1024 * 1024);
                assert.gte(Number(doc["offset"]), 0);
            } else {
                assert.eq(Number(doc.offset), 0);
            }
        }
    }

    backupCursor.close();
} finally {
    assert.commandWorked(
        primary.adminCommand({configureFailPoint: 'pauseCheckpointThread', mode: 'off'}));
    rst.stopSet();
}
