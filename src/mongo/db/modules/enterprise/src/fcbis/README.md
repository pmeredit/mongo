# File Copy Based Initial Sync

File Copy Based Initial Sync (FCBIS) is an initial sync method implemented by
`FileCopyBasedInitialSyncer`. This initial sync method is available only in MongoDB
Enterprise Server. Unlike logical initial sync, which retrieves the
data from databases and collections using ordinary ("logical") database operations, file copy based
initial sync retrieves the data on a filesystem level, by taking a [**File System
Backup**](https://github.com/mongodb/mongo/blob/master/src/mongo/db/catalog/README.md#file-system-backups)
of the sync source, and replacing the syncing node's files with that set of files.

The effect of a File Copy Based Initial Sync is essentially the same as taking a backup of
the source node using the hot backup capability, then restoring it on syncing node.

At a high level, file copy based initial sync works as follows:

1. [Select the sync source](#selecting-and-validating-a-sync-source)
2. [Open a backup cursor to the sync source](#copying-the-files-from-the-sync-source).
3. [Retrieve the backup files from the sync source using the `BackupFileCloner`](#cloning-the-files).
4. [Optionally open](#copying-the-files-from-the-sync-source) an [extended backup cursor](https://github.com/mongodb/mongo/blob/master/src/mongo/db/catalog/README.md#extending-the-backup-cursor-on-each-node-sharded-cluster-only).
5. [Retrieve the extended backup files (WiredTiger log files) from the sync source using the `BackupFileCloner`](#cloning-the-files).
6. [Repeat steps 4 and 5 until the backup is nearly up to date or a specific number of cycles have run](#copying-the-files-from-the-sync-source).
7. [Open a **local** backup cursor and obtain a list of files from that. The files in this list will be deleted before moving the files we cloned to their final location.](#getting-the-list-of-files-to-delete).
8. [Switch storage to be pointing to the set of downloaded files](#cleaning-up-the-downloaded-files).
9. [Do some cleanup of the 'local' database that is created from downloaded files](#cleaning-up-the-downloaded-files).
10. [Switch storage to a dummy location](#moving-the-downloaded-files-to-the-dbpath).
11. [Delete the list of files obtained from the local backup cursor](#moving-the-downloaded-files-to-the-dbpath).
12. [Move the files from the download location to the normal `dbpath`](#moving-the-downloaded-files-to-the-dbpath).
13. [Switch storage back to the normal `dbpath`](#completing-the-file-copy-based-initial-sync).
14. [Reconstruct prepared transactions and other ephemera, set timestamps properly, and exit](#completing-the-file-copy-based-initial-sync).

The main entry point to the FCBIS logic is [`startup`](https://github.com/10gen/mongo-enterprise-modules/blob/r6.2.1/src/fcbis/file_copy_based_initial_syncer.cpp#L162).

Before selecting a sync source, FCBIS will create an oplog if none exists. This oplog will not be
used for anything; it merely allows re-use of some replication routines which expect one to exist.
Unlike logical initial sync, FCBIS does not set the initial sync flag. Since FCBIS does not write
anything to the oplog until it is complete, the `lastAppliedOpTime` will remain null. This will make
initial sync restart if the node is restarted even without the initial sync flag. We also make sure
the node is running WiredTiger; if not, FCBIS fails.

## Selecting and validating a sync source.

FCBIS selects a sync source in [`_selectAndValidateSyncSource`](https://github.com/10gen/mongo-enterprise-modules/blob/r6.2.1/src/fcbis/file_copy_based_initial_syncer.cpp#L336). This uses the same [Sync Source
Selection](https://github.com/mongodb/mongo/blob/master/src/mongo/db/repl/README.md#sync-source-selection) that logical initial sync and `BackgroundSync` use, so `initialSyncReadPreference` and
chaining are respected. Then it adds additional criteria:

- The sync source must be a primary or secondary.

- The sync source's wire version must be less than or equal to the syncing node's wire version,
  and greater than or equal to the first version to support FCBIS.

- The sync source must be running WiredTiger, with the same values for `directoryForIndexes` and
  `directoryPerDB` as the syncing node. That is, the arrangement of files on the sync source and
  the syncing node must be the same.

- The sync source must have the same `encryptionAtRest.encryptionEnabled` setting as the syncing
  node.

If these checks fail, the sync source is denylisted with the `SyncSourceResolver`, and another
attempt is made, up to `numInitialSyncConnectAttempts` attempts. If no sync source is found, FCBIS
exits with a code of `InvalidSyncSource`.

## Copying the files from the sync source

We start copying files in [`_startSyncingFiles`](https://github.com/10gen/mongo-enterprise-modules/blob/r6.2.1/src/fcbis/file_copy_based_initial_syncer.cpp#L1053). Before copying the files, we delete the directory `dbpath/.initialsync`, which is where we will be storing the files from the sync source.

We then run a loop which continues until the `lastAppliedOpTime` on the sync source is not too far
ahead of the last optime we retrieved from the sync source, or we have run through the loop
`fileBasedInitialSyncMaxCyclesWithoutProgress` times. The amount of lag we allow is controlled by
`fileBasedInitialSyncMaxLagSec`.

Within the loop, we call [`_cloneFromSyncSourceCursor`](https://github.com/10gen/mongo-enterprise-modules/blob/r6.2.1/src/fcbis/file_copy_based_initial_syncer.cpp#L994). The first time through, this method attempts
to open a backup cursor on the sync source and read all the documents from it. The first document in a backup cursor is metadata related to the backup as a whole, including the last valid opTime in the backup's oplog. The remaining documents in the backup cursor are metadata (including filenames) for the backup files, one document per backup file. More information on backup cursors is available in the [Execution Internals Guide](https://github.com/mongodb/mongo/blob/master/src/mongo/db/catalog/README.md#how-to-take-a-backup).

If reading the backup cursor fails because the source already has a backup cursor or does not
support backup cursors, we denylist the sync source and fail this initial sync attempt. Otherwise,
we set up a task to keep the backup cursor alive and move on to cloning the actual files. The
second and subsequent times through the loop, we open an extended backup cursor on the sync source,
read the metadata from that, and clone those files. An extended backup cursor contains only
metadata for WiredTiger log files which have been added since the original backup was made.
Downloading those log files will result in the oplog of the backup being extended during WiredTiger
journal replay; when we apply the oplog later on, this will result in the syncing node being more
up-to-date.

If we are already within `fileBasedInitialSyncMaxLagSec` after the first time through the loop, we
will not open an extended backup cursor at all. There is one small difference if this happens: if
the sync source happened to roll back after the backup, the syncing node will be unable to
recover and will fail with an `UnrecoverableRollbackError`. Extended backup cursors always back up
no later than the commit point of the set, and so if one is used, we will never see a rollback to
earlier than the `lastAppliedOpTime` on the sync source. This is a very small window; if a rollback
happens during the backup, the backup cursor will be killed and initial sync will make another
attempt if it hasn't run out of attempts.

Once we have caught up -- that is, after cloning the files, the lastAppliedOpTime of the sync source
is within `fileBasedInitialSyncMaxLagSec` of the top of the oplog (the latest entry in the oplog) in
the backup files we just finished cloning -- or exhausted our number of cycles, we stop the task
which refreshes the backup cursor, and kill the backup cursor on the sync source. The rest of FCBIS does not need the sync source; everything is local.

### Cloning the files

To clone the files in [`_cloneFiles`](https://github.com/10gen/mongo-enterprise-modules/blob/r6.2.1/src/fcbis/file_copy_based_initial_syncer.cpp#L1172),
we use the `BackupFileCloner` class. This class opens a "$\_backupFile"
aggregation, on a separate cursor, using our "exhaust" mechanism, to actually retrieve the contents
of the files from the sync source. The files are stored within the `dbpath/.initialsync` directory
with the same directory structure as they had on the sync source, e.g. `dbpath/log/000.1` on the
sync source is stored as `dbpath/.initialsync/log/000.1` on the syncing node. The two `dbpath`
values may be different; we get the sync source dbpath from the backup cursor metadata.

## Getting the list of files to delete

Since we want the files we just downloaded to take the place of the (mostly empty) files that the
syncing node started up with, in [`_getListOfOldFilesToBeDeleted`](https://github.com/10gen/mongo-enterprise-modules/blob/r6.2.1/src/fcbis/file_copy_based_initial_syncer.cpp#L1467) we open a backup cursor on the
current node. This gives us a list of files that WiredTiger is using. We record that list to
be deleted later; we cannot delete it immediately because the files are in use. It is possible
that additional WiredTiger log files will be written between enumerating the backup cursor
and shutting down; we handle that by deleting all log files.

## Cleaning up the downloaded files.

In [`_prepareStorageDirectoriesForMovingPhase`](https://github.com/10gen/mongo-enterprise-modules/blob/r6.2.1/src/fcbis/file_copy_based_initial_syncer.cpp#L1391), after getting the list of files to delete, we
take the global lock, retrieve the current on-disk replica set configuration as well as the
current last vote document, and switch the storage
engine to use the new set of files we just downloaded. These files need a little modification
before we can use them in normal operation; we call `_cleanUpLocalCollectionsAfterSync` to do this.

In [`_cleanUpLocalCollectionsAfterSync`](https://github.com/10gen/mongo-enterprise-modules/blob/r6.2.1/src/fcbis/file_copy_based_initial_syncer.cpp#L610), we do the following:

- Set the oplogTruncateAfterPoint to the last opTime we retrieved from the sync source, or if we did
  not extend the backup cursor, to the oplogEnd from the backup metadata. In either case it is
  possible the backup has oplog entries after this point, but they may be after oplog holes and thus
  invalid.

- Clear and reset the initialSyncId, which is an unique identifier for this node. Together with
  the rollbackId it allows **logical** initial sync to determine if a node it was syncing from
  is safe to continue syncing from.

- If the node has not participated in any elections yet, as in the usual case, we clear the last vote
  document. If the node participated in an election, we carry over the last vote from that election
  (the last vote document we read earlier).

- Replace the config with the one we read earlier. Because both the read and write were done
  within the same global X critical section, we can be assured that no config change was written
  between the read and write, so the `ReplicationCoordinator`s idea of the on-disk config is
  still correct.

After `_cleanUpLocalCollectionsAfterSync`, we release the global lock and call
[`_replicationStartupRecovery`](https://github.com/10gen/mongo-enterprise-modules/blob/r6.2.1/src/fcbis/file_copy_based_initial_syncer.cpp#L1360). This replays the oplog and sets the stable timestamp to the
oplogTruncateAfterPoint, which will also be the top of the oplog (the latest entry in the oplog)
after recovery. At this point, once a checkpoint is taken, the files on disk will be correct for
use in normal operation.

## Moving the downloaded files to the dbpath

The files in `dbpath/.initialsync` must be moved to the dbpath for initial sync to be complete and
for the server to be able to restart and see the new files. To do this, we again take the global X
lock. We switch storage to an empty directory in the dbpath called ".initialsync/.dummy". In doing
so we take a stable snapshot of the files in .initialsync directory (because we set the stable
timestamp earlier). We must switch to this empty directory because we can neither safely move nor
delete files which are in use, so we cannot have storage open with the original files or the
downloaded ones.

While we still have global X, we use `InitialSyncFileMover` to delete all the old files that we
retrieved earlier with `_getListOfOldFilesToBeDeleted`. This will also delete all WiredTiger log
files. Before we delete the old files, we write a "marker" file to the .initialSync directory,
containing the list of files to delete. If we crash while deleting the old files, then on restart
we will read this marker file and use it to continue deleting those files.

Once the files are deleted, in [`_startMovingNewStorageFilesPhase`](https://github.com/10gen/mongo-enterprise-modules/blob/r6.2.1/src/fcbis/file_copy_based_initial_syncer.cpp#L1275),
we write a marker file with the set of files to be moved, which is just
the entire contents of the .initialsync directory other than .dummy and any WiredTigerPreplog and
WiredTigerTmpLog files. Then we delete the delete marker. Then we actually move the files. If
there are still filename collisions despite our deleting the old files, we rename the old file and
log a warning. If we crash while moving the old files, on restart we read the marker file and use
it to continue moving the files.

The marker files are necessary because once we start moving files around in the `dbpath`, then until
we are done we can no longer depend on being able to have storage start up. This also means we
cannot open a local backup cursor to retrieve the list of files to delete, because that depends on
having storage running. So before storage starts, we attempt to recover this situation depending on
which files exist:

| Files/Directories in `dbpath`         | Meaning                                                                | Action                                                                                              |
| ------------------------------------- | ---------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------- |
| .initialsync directory does not exist | No file copy based initial sync was in progress.                       | None                                                                                                |
| .initialsync directory exists         | A file copy based initial sync was in progress.                        | Process according to marker files                                                                   |
| Only delete marker exists             | Crashed while deleting files                                           | Continue deleting files, then move .initialsync files to `dbpath` and delete .initialsync directory |
| Only move marker exists               | Crashed while moving files                                             | Continue moving files from .initialsync to `dbpath` and delete .initialsync directory               |
| Both markers exist                    | Crashed after deleting but before moving files                         | Same as if only move marker exists                                                                  |
| No marker exists                      | Crashed before deleting files; initial sync may not have been complete | Delete .initialsync directory. Initial sync will restart from the beginning.                        |

The marker files are always written atomically by writing them first to a temporary file, then
calling fsync() and using rename() to rename them to their final name.

## Completing the File Copy Based Initial Sync

Once the files are moved, we switch storage one more time, back to the original location, which
now has the downloaded files. We use the `InitialSyncFileMover` to delete the move marker and
the entire .initialsync directory; a restart of the server at or after this point will not
involve any more FCBIS work. Then we release the global lock, and retrieve the last applied
OpTime and WallTime from the top of the oplog (the latest entry in the oplog) in
[`_updateLastAppliedOptime`](https://github.com/10gen/mongo-enterprise-modules/blob/r6.2.1/src/fcbis/file_copy_based_initial_syncer.cpp#L211).

The initial sync attempt is now considered successful, and we call [`_finishCallback`](https://github.com/10gen/mongo-enterprise-modules/blob/r6.2.1/src/fcbis/file_copy_based_initial_syncer.cpp#L1841). This acts
similarly to the end of logical initial sync.

From, `_finishCallback`, we call `_updateStorageTimestampsAfterInitialSync,` where:

- We set the oplog visibility timestamp to the lastAppliedOpTime timestamp.
- We set the initialDataTimestamp to the lastAppliedOpTime timestamp.
- We reconstruct prepared transactions so the node can participate in 2-phase commit.

Finally,

- We call the callback `ReplicationCoordinator` passed us.
- We record our final statistics in `_markInitialSyncCompleted`.

## Resumability of File Copy Based Initial Sync

Like [Logical Initial Sync](https://github.com/mongodb/mongo/blob/master/src/mongo/db/repl/README.md#logical-initial-sync), File Copy Based Initial Sync is resumable on
network failure, provided the backup cursor is still available. On a network error we will retry
until `initialSyncTransientErrorRetryPeriodSeconds`, or the MongoDB cursor timeout
returned by `getCursorTimeoutMillis()`, has elapsed, whichever is shorter. The default cursor
timeout is 10 minutes, significantly shorter than the default
`initialSyncTransientErrorRetryPeriodSeconds` of one day. Once the backup cursor has timed out,
it is no longer possible to retrieve the backup files and they may actually be deleted on the
sync source.

Anything that causes the backup cursor to be killed will abort the initial sync attempt. This
includes rollback and also (unlike [Logical Initial Sync](https://github.com/mongodb/mongo/blob/master/src/mongo/db/repl/README.md#logical-initial-sync)) restart of the
sync source. This is again because the backup files cannot be retrieved once the backup cursor
is closed for any reason.
