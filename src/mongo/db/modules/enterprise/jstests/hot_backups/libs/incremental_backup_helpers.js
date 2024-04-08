// Helper library for incremental backups.
import {getBackupCursorDB} from "jstests/libs/backup_utils.js";
import {getPython3Binary} from "jstests/libs/python.js";

export const kSeparator = _isWindows() ? "\\" : "/";

/**
 * Opens a new backup cursor on the given connection with 'options'. Returns the backup name and
 * cursor.
 */
export const beginBackup = function(conn, options) {
    let ret = {
        thisBackupName: options.thisBackupName,
    };

    const backupCursorDB = getBackupCursorDB(conn);
    // Opening a backup cursor can race with taking a checkpoint, resulting in a transient error.
    // Retry until it succeeds.
    let backupCursor;
    while (true) {
        try {
            backupCursor = backupCursorDB.aggregate([{$backupCursor: options}]);
            break;
        } catch (ex) {
            jsTestLog({"Failed to open the backup cursor, retrying.": ex});

            // We need to update 'thisBackupName' after a failed attempt to open a backup cursor as
            // WiredTiger denylists that identifier from being used again immediately. Due to the
            // unique format of the thisBackupName, appending a character will be enough here.
            if (options.thisBackupName) {
                options.thisBackupName += "0";
                ret.thisBackupName = options.thisBackupName;
            }
        }
    }

    assert(backupCursor);
    ret.backupCursor = backupCursor;

    // The first document is always the backup metadata document, so the backup cursor should never
    // be empty.
    assert.eq(true, backupCursor.hasNext());
    return ret;
};

/**
 * Consumes the contents of the open backup cursor and backs up the data to 'dest'. Takes full file
 * copies when the 'offset' and 'length' fields are missing from the backup cursor. Otherwise this
 * performs partial file copies using the 'offset' and 'length' information.
 */
export const consumeBackupCursor = function(backupCursor, dest) {
    // The first document is always the backup metadata document.
    assert.eq(true, backupCursor.hasNext());
    jsTestLog("Backup metadata document: " + tojson(backupCursor.next()));

    // We keep track of the files seen by the backup cursor. Files from past incremental backups not
    // included by the current backup cursor are considered unneeded and should be removed.
    let filesSeen = [];

    while (backupCursor.hasNext()) {
        let backupItem = backupCursor.next();
        jsTestLog("Backing up: " + tojson(backupItem));

        assert(backupItem.hasOwnProperty("filename"));
        assert(backupItem.hasOwnProperty("fileSize"));

        const filename =
            backupItem.filename.substring(backupItem.filename.lastIndexOf(kSeparator) + 1);
        filesSeen.push(filename);

        const destFilePath = _constructFilePath(backupItem.filename, dest);

        // If the file hasn't had an initial backup yet, then a full file copy is needed.
        if (!pathExists(destFilePath)) {
            jsTestLog(`Copying new file for backup from ${backupItem.filename} to ${destFilePath}`);
            copyFile(backupItem.filename, destFilePath);
            continue;
        }

        if (!backupItem.hasOwnProperty("offset") && !backupItem.hasOwnProperty("length")) {
            // Documents missing the 'offset' and 'length' fields indicate that the file remained
            // unchanged between backups but still has to be kept around.
            jsTestLog(
                `File ${backupItem.filename} is unchanged since the last incremental backup.`);
            continue;
        }

        assert(backupItem.hasOwnProperty("offset"));
        assert(backupItem.hasOwnProperty("length"));

        // Files that aren't tables will be full copies. These can be journal files or metadata
        // files.
        if (_requiresFullCopy(backupItem.filename)) {
            if (pathExists(destFilePath)) {
                // Remove the old backup of the file. For journal files specifically, if a
                // checkpoint didn't take place between two incremental backups, then the backup
                // cursor can specify journal files we've already copied at an earlier time. We
                // should remove these old journal files so that we can copy them over again in the
                // event that their contents have changed over time.
                jsTestLog(`Removing existing file ${
                    destFilePath} in preparation of copying a newer version of it`);
                removeFile(destFilePath);
            }

            jsTestLog(`Copying file from ${backupItem.filename} to ${destFilePath}`);
            copyFile(backupItem.filename, destFilePath);
            continue;
        }

        // The file from the backup cursor already exists in the destination, but has changed bytes
        // since the last incremental backup took place. Copies 'length' bytes starting at 'offset'.
        jsTestLog(`Copying ${backupItem.length} bytes from file ${backupItem.filename} to ${
            destFilePath} starting at offset ${backupItem.offset}`);
        _copyFileRange(backupItem.filename,
                       destFilePath,
                       NumberLong(backupItem.offset),
                       NumberLong(backupItem.length));
    }

    _removeStaleBackupFiles(dest, filesSeen);
};

/**
 * Verifies that the backup cursor was fully consumed and closes it.
 */
export const endBackup = function(backupCursor) {
    assert.eq(false, backupCursor.hasNext());
    backupCursor.close();
};

/**
 * Copies all the data and journal files from 'src' to 'dest'.
 */
export const copyDataFiles = function(src, dest) {
    ls(src).forEach((file) => {
        if (file.endsWith("/")) {
            return;
        }

        let fileName = file.substring(file.lastIndexOf(kSeparator) + 1);
        copyFile(file, dest + kSeparator + fileName);
    });

    ls(src + kSeparator + "journal").forEach((file) => {
        let fileName = file.substring(file.lastIndexOf(kSeparator) + 1);
        copyFile(file, dest + kSeparator + "journal" + kSeparator + fileName);
    });
};

/**
 * Returns true if the file in 'path' is a journal file.
 */
export const _isJournalFile = function(path) {
    return path.includes(kSeparator + "journal" + kSeparator);
};

/**
 * Returns true if the file in 'path' should be copied fully instead of incrementally.
 */
export const _requiresFullCopy = function(path) {
    const filename = path.substring(path.lastIndexOf(kSeparator) + 1);
    if (filename.startsWith("WiredTigerLog")) {
        // Journal files.
        return true;
    } else if (filename == "WiredTigerHS.wt") {
        // WiredTiger history store.
        return true;
    } else if (filename == "WiredTiger" || filename == "WiredTiger.backup" ||
               filename == "WiredTiger.turtle" || filename == "WiredTiger.wt") {
        // WiredTiger metadata.
        return true;
    }

    return false;
};

/**
 * Extracts the filename from 'src' and constructs a path for it in 'dest'. Returns the new path for
 * the file.
 */
export const _constructFilePath = function(src, dest) {
    let lastChar = dest[dest.length - 1];
    if (lastChar != kSeparator) {
        dest += kSeparator;
    }

    if (_isJournalFile(src)) {
        dest += "journal" + kSeparator;
    }

    let fileName = src.substring(src.lastIndexOf(kSeparator) + 1);
    return dest + fileName;
};

/**
 * Removes the files from 'dest' that aren't present in 'filesSeen'. Any files not specified by the
 * backup cursor are no longer needed and should be removed.
 */
export const _removeStaleBackupFiles = function(dest, filesSeen) {
    const dataFiles = ls(dest);
    const journalFiles = ls(dest + kSeparator + "journal");
    const files = dataFiles.concat(journalFiles);

    for (const fileIdx in files) {
        const filename = files[fileIdx].substring(files[fileIdx].lastIndexOf(kSeparator) + 1);
        if (filename.length == 0) {
            // This is a directory.
            continue;
        }

        if (!filesSeen.includes(filename)) {
            jsTestLog(`Removing stale backup file ${files[fileIdx]}`);
            removeFile(files[fileIdx]);
        }
    }
};

/**
 * Starts a client that will run a FSM workload and returns the clients PID.
 */
export const startFSMClient = function(host) {
    // Launch FSM client.
    const suite = 'concurrency_replication_for_backup_restore';
    const resmokeCmd = getPython3Binary() +
        ' buildscripts/resmoke.py run --shuffle --continueOnFailure' +
        ' --repeat=99999 --internalParam=is_inner_level --mongo=' +
        MongoRunner.getMongoShellPath() + ' --shellConnString=mongodb://' + host +
        ' --suites=' + suite;

    // Returns the pid of the FSM test client so it can be terminated without waiting for its
    // execution to finish.
    return _startMongoProgram({args: resmokeCmd.split(' ')});
};

/**
 * Stops the client running the FSM workload by its PID.
 */
export const stopFSMClient = function(fsmPid) {
    const fsmStatus = checkProgram(fsmPid);
    assert(fsmStatus.alive,
           jsTest.name() + ' FSM client was not running at end of test and exited with code: ' +
               fsmStatus.exitCode);

    const kSIGINT = 2;
    const exitCode = stopMongoProgramByPid(fsmPid, kSIGINT);
    if (!_isWindows()) {
        // The mongo shell calls TerminateProcess() on Windows rather than more gracefully
        // interrupting resmoke.py test execution.

        // resmoke.py may exit cleanly on SIGINT, returning 130 if the suite tests were running and
        // returning SIGINT otherwise. It may also exit uncleanly, in which case
        // stopMongoProgramByPid returns -SIGINT. See SERVER-67390 and SERVER-72449.
        assert(exitCode == 130 || exitCode == -kSIGINT || exitCode == kSIGINT,
               'expected resmoke.py to exit due to being interrupted, but exited with code: ' +
                   exitCode);
    }
};
