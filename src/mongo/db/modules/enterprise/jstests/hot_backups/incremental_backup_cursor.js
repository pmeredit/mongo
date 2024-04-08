/**
 * Validates the input for incremental backup requests using the aggregate $backupCursor stage.
 *  - $backupCursor accepts as input 'incrementalBackup: <false/true>'.
 *    - For backwards compatibility, the default is false.
 *    - The first full backup meant for incremental must pass true.
 *    - All incremental backups must pass true.
 *  - $backupCursor accepts as input 'thisBackupName: <string>'.
 *    - This input should only exist iff 'incrementalBackup: true'.
 *    - Errors if the name is not unique for the recent history of provided names.
 *  - $backupCursor accepts as input 'srcBackupName: <string>'.
 *    - This input must not exist when 'incrementalBackup: false'.
 *    - This input may be present when 'incrementalBackup: true'.
 *    - Errors if the name is not in the recent history of incremental backups.
 *
 * Tests the functionality of incremental backup cursors:
 * - Taking a full checkpoint with 'incrementalBackup: false' will remove any previous
 *   incremental backups.
 * - If we have incremental backups 'foo' and 'bar', then taking another incremental backup on:
 *   - 'foo' will remove the incremental backup for 'bar'.
 *   - 'bar' will remove the incremental backup for 'foo'.
 * - Cannot take an incremental backup on 'foo' when 'foo' has not finished yet.
 *
 * @tags: [
 *   requires_persistence,
 *   requires_replication,
 * ]
 */
import {getBackupCursorDB, openBackupCursor} from "jstests/libs/backup_utils.js";

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

var primary = rst.getPrimary();
const backupCursorDB = getBackupCursorDB(primary);
let backupCursor;

// Opening backup cursors can race with taking a checkpoint, so disable them.
assert.commandWorked(
    primary.adminCommand({configureFailPoint: 'pauseCheckpointThread', mode: 'alwaysOn'}));

try {
    // Ensure $backupCursor is backwards compatible with incremental backup.
    assert.throws(() => {
        // 'incrementalBackup' must be a boolean.
        backupCursorDB.aggregate([{$backupCursor: {incrementalBackup: {}}}]);
    });
    assert.doesNotThrow(() => {
        backupCursor = openBackupCursor(backupCursorDB, {incrementalBackup: false});
        backupCursor.close();
    });

    // Test input validation for incremental backups using $backupCursor.
    assert.throws(() => {
        // 'incrementalBackup' is false and has 'thisBackupName'.
        backupCursorDB.aggregate(
            [{$backupCursor: {incrementalBackup: false, thisBackupName: 'foo'}}]);
    });
    assert.throws(() => {
        // 'incrementalBackup' is false and has 'srcBackupName'.
        backupCursorDB.aggregate(
            [{$backupCursor: {incrementalBackup: false, srcBackupName: 'foo'}}]);
    });
    assert.throws(() => {
        // Missing 'thisBackupName'.
        backupCursorDB.aggregate([{$backupCursor: {incrementalBackup: true}}]);
    });
    assert.throws(() => {
        // 'thisBackupName' is empty.
        backupCursorDB.aggregate([{$backupCursor: {incrementalBackup: true, thisBackupName: ''}}]);
    });
    assert.throws(() => {
        // 'thisBackupName' is not a string.
        backupCursorDB.aggregate([{$backupCursor: {incrementalBackup: true, thisBackupName: 1.0}}]);
    });
    assert.throws(() => {
        // 'srcBackupName' is not a string.
        backupCursorDB.aggregate(
            [{$backupCursor: {incrementalBackup: true, thisBackupName: 'foo', srcBackupName: {}}}]);
    });
    assert.throws(() => {
        // Cannot specify both 'incrementalBackup' and 'disableIncrementalBackup' as true.
        backupCursorDB.aggregate(
            [{$backupCursor: {incrementalBackup: true, disableIncrementalBackup: true}}]);
    });
    assert.throws(() => {
        // Cannot specify 'thisBackupName' when 'disableIncrementalBackup' is true.
        backupCursorDB.aggregate(
            [{$backupCursor: {disableIncrementalBackup: true, thisBackupName: 'foo'}}]);
    });
    assert.throws(() => {
        // Cannot specify 'srcBackupName' when 'disableIncrementalBackup' is true.
        backupCursorDB.aggregate(
            [{$backupCursor: {disableIncrementalBackup: true, srcBackupName: 'foo'}}]);
    });

    // Test that users can forcefully disable incremental backups.
    assert.doesNotThrow(() => {
        backupCursor = openBackupCursor(backupCursorDB, {disableIncrementalBackup: true});

        assert.eq(true, backupCursor.hasNext());
        assert.eq("Close the cursor to release all incremental information and resources.",
                  backupCursor.next().metadata.message);
        assert.eq(false, backupCursor.hasNext());
        backupCursor.close();
    });

    // Test that incremental backup information is not removed when taking a normal full backup.
    assert.doesNotThrow(() => {
        backupCursor =
            openBackupCursor(backupCursorDB, {incrementalBackup: true, thisBackupName: 'foo'});
        backupCursor.close();
        backupCursor = openBackupCursor(
            backupCursorDB, {incrementalBackup: true, thisBackupName: 'bar', srcBackupName: 'foo'});
        backupCursor.close();

        backupCursor = openBackupCursor(backupCursorDB, {incrementalBackup: false});
        backupCursor.close();

        backupCursor = openBackupCursor(
            backupCursorDB, {incrementalBackup: true, thisBackupName: 'baz', srcBackupName: 'bar'});
        backupCursor.close();
    });

    // Test that incremental backup information is removed when taking a full backup that is the
    // basis for future incremental backups.
    assert.doesNotThrow(() => {
        backupCursor =
            openBackupCursor(backupCursorDB, {incrementalBackup: true, thisBackupName: 'new'});
        backupCursor.close();
    });

    // Test that incremental backup options can only be provided when incremental backup is enabled.
    assert.throws(() => {
        backupCursorDB.aggregate([
            {$backupCursor: {incrementalBackup: false, thisBackupName: 'bad', srcBackupName: 'bar'}}
        ]);
    });

    // Test that incremental backup options can only be provided when incremental backup is enabled.
    assert.throws(() => {
        backupCursorDB.aggregate([
            {$backupCursor: {incrementalBackup: false, thisBackupName: 'bad', srcBackupName: 'baz'}}
        ]);
    });

    // The first full backup meant for incremental must pass {incrementalBackup: true}.
    assert.throws(() => {
        backupCursorDB.aggregate(
            [{$backupCursor: {incrementalBackup: false, thisBackupName: 'foo'}}]);
    });
    assert.throws(() => {
        backupCursorDB.aggregate([
            {$backupCursor: {incrementalBackup: true, thisBackupName: 'bar', srcBackupName: 'foo'}}
        ]);
    });

    // Perform an incremental backup for A and for B from A.
    assert.doesNotThrow(() => {
        backupCursor =
            openBackupCursor(backupCursorDB, {incrementalBackup: true, thisBackupName: 'A'});
        backupCursor.close();
        backupCursor = openBackupCursor(
            backupCursorDB, {incrementalBackup: true, thisBackupName: 'B', srcBackupName: 'A'});
        backupCursor.close();
    });

    // Now, if we create another incremental backup on 'B', then we can no longer make incremental
    // backups on 'A'.
    assert.doesNotThrow(() => {
        backupCursor = openBackupCursor(
            backupCursorDB, {incrementalBackup: true, thisBackupName: 'C', srcBackupName: 'B'});
        backupCursor.close();
    });
    assert.throws(() => {
        backupCursorDB.aggregate([
            {$backupCursor: {incrementalBackup: true, thisBackupName: 'bad', srcBackupName: 'A'}}
        ]);
    });

    // It's also possible to make an incremental backup on 'B' instead of 'C', which will
    // prevent further incremental backups on 'C'.
    assert.doesNotThrow(() => {
        backupCursor = openBackupCursor(
            backupCursorDB, {incrementalBackup: true, thisBackupName: 'D', srcBackupName: 'B'});
        backupCursor.close();
    });
    assert.throws(() => {
        backupCursorDB.aggregate([
            {$backupCursor: {incrementalBackup: true, thisBackupName: 'bad', srcBackupName: 'E'}}
        ]);
    });

    // Cannot create an incremental backup on a non-recent srcBackupName.
    assert.throws(() => {
        backupCursorDB.aggregate([
            {$backupCursor: {incrementalBackup: true, thisBackupName: 'bad', srcBackupName: 'aaa'}}
        ]);
    });

    // Cannot create an incremental backup with a non-unique name for recent incremental backups.
    assert.throws(() => {
        backupCursorDB.aggregate(
            [{$backupCursor: {incrementalBackup: true, thisBackupName: 'B', srcBackupName: 'B'}}]);
    });
    assert.throws(() => {
        backupCursorDB.aggregate(
            [{$backupCursor: {incrementalBackup: true, thisBackupName: 'D', srcBackupName: 'D'}}]);
    });
} finally {
    assert.commandWorked(
        primary.adminCommand({configureFailPoint: 'pauseCheckpointThread', mode: 'off'}));
}
rst.stopSet();