/**
 * Characterizes the ordering and format of the documents returned by the backup cursor.
 *
 * The first document will contain the metadata about the backup.
 * The remainder of the documents will have one of the formats listed below.
 *
 * For non-incremental backups, incremental backups where the file had no changed blocks, and the
 * first full backup to be used as a basis for future incremental backups:
 * {
 *     filename: String,
 *     fileSize: Number,
 *     required: bool
 * }
 *
 * For incremental backups where the file had changed blocks:
 * {
 *     filename: String,
 *     fileSize: Number,
 *     offset: Number,
 *     length: Number,
 *     required: bool
 * }
 * If the file had multiple changed blocks, then there will be one document per changed block.
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

const validateBackupCursorFields = function(doc) {
    const kSeparator = _isWindows() ? "\\" : "/";

    const filename = doc.filename.substring(doc.filename.lastIndexOf(kSeparator) + 1);
    const required = doc.required;
    const ns = doc.ns;
    const uuid = doc.uuid;

    jsTest.log("Field Validation: Filename: [" + filename + "] Namespace: [" + ns +
               "] Required: [" + required + "]");

    // Verify that the correct WiredTiger files are marked as required.
    if (filename == "WiredTiger" || filename == "WiredTiger.backup" ||
        filename == "WiredTigerHS.wt" || filename.startsWith("WiredTigerLog.")) {
        assert.eq(true, required);
        assert.eq("", ns);    // Empty
        assert.eq("", uuid);  // Empty
        return;
    }

    // Verify that the correct MongoDB files are marked as required.
    if (filename == "_mdb_catalog.wt" || filename == "sizeStorer.wt") {
        assert.eq(true, required);
        assert.eq("", ns);    // Empty
        assert.eq("", uuid);  // Empty
        return;
    }

    if (filename.startsWith("collection-") || filename.startsWith("index-")) {
        assert.neq("", ns);
        assert.neq("", uuid);

        // Verify time-series collection has internal "system.buckets." string removed
        if (ns.includes("timeseries-test")) {
            assert.eq("test.timeseries-test", ns);
            assert.neq("test.system.buckets.timeseries-test", ns);
            assert.eq(false, required);
            return;
        } else if (ns.startsWith("local") || ns.startsWith("admin") || ns.startsWith("config") ||
                   ns.includes("test.system.views")) {
            // Verify that internal database and system.views files are marked required.
            assert.eq(true, required);
            return;
        }
    } else {
        assert.eq("", ns);    // Empty
        assert.eq("", uuid);  // Empty
    }

    // Everything else shouldn't be marked as required.
    jsTest.log("Fallthrough");
    assert.eq(false, required);
};

// Opening backup cursors can race with taking a checkpoint, so disable checkpoints.
// This makes testing quicker and more predictable. In production, a poorly interleaved checkpoint
// will return an error, requiring retry.
assert.commandWorked(
    primary.adminCommand({configureFailPoint: 'pauseCheckpointThread', mode: 'alwaysOn'}));

// Take the initial checkpoint.
assert.commandWorked(primary.adminCommand({fsync: 1}));

try {
    jsTest.log("Testing non-incremental backup document format.");
    let backupCursor = openBackupCursor(primary.getDB("admin"), {incrementalBackup: false});

    let isFirstDoc = true;
    while (backupCursor.hasNext()) {
        let doc = backupCursor.next();

        if (isFirstDoc) {
            assert.eq(true, doc.hasOwnProperty("metadata"));
            assert.eq(false, doc.metadata.disableIncrementalBackup);
            assert.eq(false, doc.metadata.incrementalBackup);
            assert.eq(16, doc.metadata.blockSize);
            assert.eq(false, doc.metadata.hasOwnProperty("thisBackupName"));
            assert.eq(false, doc.metadata.hasOwnProperty("srcBackupName"));

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
            assert.eq(true, doc.hasOwnProperty("required"));
            assert.eq(true, doc.hasOwnProperty("ns"));
            validateBackupCursorFields(doc);
        }
    }

    backupCursor.close();

    // Take the first full incremental backup to be used as a basis for future incremental backups.
    jsTest.log("Testing incremental full backup document format.");
    backupCursor = openBackupCursor(
        primary.getDB("admin"),
        {incrementalBackup: true, thisBackupName: "foo", blockSize: NumberInt(16)});

    isFirstDoc = true;
    while (backupCursor.hasNext()) {
        let doc = backupCursor.next();

        if (isFirstDoc) {
            assert.eq(true, doc.hasOwnProperty("metadata"));
            assert.eq(false, doc.metadata.disableIncrementalBackup);
            assert.eq(true, doc.metadata.incrementalBackup);
            assert.eq(16, doc.metadata.blockSize);
            assert.eq(true, doc.metadata.hasOwnProperty("thisBackupName"));
            assert.eq("foo", doc.metadata.thisBackupName);
            assert.eq(false, doc.metadata.hasOwnProperty("srcBackupName"));

            assert.eq(false, doc.hasOwnProperty("filename"));
            assert.eq(false, doc.hasOwnProperty("fileSize"));
            assert.eq(false, doc.hasOwnProperty("offset"));
            assert.eq(false, doc.hasOwnProperty("length"));
            isFirstDoc = false;
        } else {
            assert.eq(false, doc.hasOwnProperty("metadata"));
            assert.eq(true, doc.hasOwnProperty("filename"));
            assert.eq(true, doc.hasOwnProperty("fileSize"));
            assert.eq(true, doc.hasOwnProperty("offset"));
            assert.eq(true, doc.hasOwnProperty("length"));
            assert.eq(true, doc.hasOwnProperty("required"));
            assert.eq(true, doc.hasOwnProperty("ns"));
            validateBackupCursorFields(doc);
        }
    }

    backupCursor.close();

    // Insert documents to create changes which incremental backup will make us copy.
    for (let i = 0; i < 25; i++) {
        primaryDB.getCollection("test").insert({x: x, y: y, z: z});
    }

    // Create time-series collection with some documents to test namespace behavior
    // in the backup cursor
    assert.commandWorked(
        primaryDB.createCollection("timeseries-test", {timeseries: {timeField: "tm"}}));
    for (let i = 0; i < 10; i++) {
        primaryDB.getCollection("timeseries-test").insert({tm: ISODate(), x: i});
    }

    assert.commandWorked(primaryDB.adminCommand({fsync: 1}));

    jsTest.log("Testing incremental backup document format.");
    backupCursor =
        openBackupCursor(primary.getDB("admin"),
                         {incrementalBackup: true, thisBackupName: "bar", srcBackupName: "foo"});

    isFirstDoc = true;
    let hasMatchingDoc = false;
    while (backupCursor.hasNext()) {
        let doc = backupCursor.next();

        if (isFirstDoc) {
            assert.eq(true, doc.hasOwnProperty("metadata"));
            assert.eq(false, doc.metadata.disableIncrementalBackup);
            assert.eq(true, doc.metadata.incrementalBackup);
            assert.eq(16, doc.metadata.blockSize);
            assert.eq(true, doc.metadata.hasOwnProperty("thisBackupName"));
            assert.eq("bar", doc.metadata.thisBackupName);
            assert.eq(true, doc.metadata.hasOwnProperty("srcBackupName"));
            assert.eq("foo", doc.metadata.srcBackupName);

            assert.eq(false, doc.hasOwnProperty("filename"));
            assert.eq(false, doc.hasOwnProperty("fileSize"));
            assert.eq(false, doc.hasOwnProperty("offset"));
            assert.eq(false, doc.hasOwnProperty("length"));
            isFirstDoc = false;
        } else {
            assert.eq(false, doc.hasOwnProperty("metadata"));
            assert.eq(true, doc.hasOwnProperty("filename"));
            assert.eq(true, doc.hasOwnProperty("fileSize"));
            assert.eq(true, doc.hasOwnProperty("required"));
            assert.eq(true, doc.hasOwnProperty("ns"));
            validateBackupCursorFields(doc);

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
