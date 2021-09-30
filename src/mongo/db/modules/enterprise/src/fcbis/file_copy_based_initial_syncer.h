/**
 *    Copyright (C) 2021 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include <memory>

#include "fcbis/backup_file_cloner.h"
#include "fcbis/initial_sync_file_mover.h"
#include "mongo/client/dbclient_connection.h"
#include "mongo/db/repl/data_replicator_external_state.h"
#include "mongo/db/repl/initial_sync_shared_data.h"
#include "mongo/db/repl/initial_syncer_interface.h"
#include "mongo/db/repl/repl_server_parameters_gen.h"
#include "mongo/db/repl/replication_process.h"
#include "mongo/db/repl/storage_interface.h"
#include "mongo/executor/scoped_task_executor.h"
#include "mongo/executor/task_executor.h"
#include "mongo/util/concurrency/thread_pool.h"
#include "mongo/util/future_util.h"

namespace mongo {
namespace repl {

constexpr int kFileCopyBasedInitialSyncMaxCursorFetchAttempts = 10;
constexpr int kFileCopyBasedInitialSyncKeepBackupCursorAliveIntervalInMin = 5;

/**
 * The FileCopyBasedInitialSyncer performs initial sync through a file-copy based method rather than
 * a logical initial sync method.
 */
class FileCopyBasedInitialSyncer final
    : public std::enable_shared_from_this<FileCopyBasedInitialSyncer>,
      public InitialSyncerInterface {
    FileCopyBasedInitialSyncer(const FileCopyBasedInitialSyncer&) = delete;
    FileCopyBasedInitialSyncer& operator=(const FileCopyBasedInitialSyncer&) = delete;

public:
    using BackupFileMetadataCollection = std::vector<BSONObj>;

    FileCopyBasedInitialSyncer(
        InitialSyncerInterface::Options opts,
        std::unique_ptr<DataReplicatorExternalState> dataReplicatorExternalState,
        ThreadPool* writerPool,
        StorageInterface* storage,
        ReplicationProcess* replicationProcess,
        const OnCompletionFn& onCompletion);

    ~FileCopyBasedInitialSyncer() final;

    Status startup(OperationContext* opCtx, std::uint32_t maxAttempts) noexcept final;

    Status shutdown() final;

    void join() final;

    BSONObj getInitialSyncProgress() const final;

    void cancelCurrentAttempt() final;

    // State transitions:
    // PreStart --> Running --> ShuttingDown --> Complete
    // It is possible to skip intermediate states. For example, calling shutdown() when the data
    // replicator has not started will transition from PreStart directly to Complete.
    enum class State { kPreStart, kRunning, kShuttingDown, kComplete };

    /**
     * Returns true if an initial sync is currently running or in the process of shutting down.
     */
    bool isActive() const;

    /**
     *
     * Overrides how the initial syncer creates the client.
     *
     * For testing only
     */
    void setCreateClientFn_forTest(const CreateClientFn& createClientFn);

    /**
     * Return the status of _startInitialSyncAttemptFuture if ready and ErrorCodes::IllegalOperation
     * if initial sync is still in progress.
     * For testing only.
     */
    Status getStartInitialSyncAttemptFutureStatus_forTest();

    /**
     * Waits for _startInitialSyncAttemptFuture to be ready and returns the status when it is done.
     * For testing only.
     */
    Status waitForStartInitialSyncAttemptFutureStatus_forTest();

    /**
     * Return _syncSource.
     * For testing only.
     */
    HostAndPort getSyncSource_forTest();

    /**
     * Returns list of files fetched from the backupCursor.
     * For testing only.
     */
    const BackupFileMetadataCollection& getBackupCursorFiles_ForTest() {
        return *_syncingFilesState.backupCursorFiles;
    }

    /**
     * Returns list of files fetched from extended the backupCursor.
     * For testing only.
     */
    const StringSet getExtendedBackupCursorFiles_ForTest() {
        return _syncingFilesState.extendedCursorFiles;
    }

    static std::string getPathRelativeTo_forTest(StringData path, StringData basePath) {
        return _getPathRelativeTo(path, basePath);
    }

    void setOldStorageFilesToBeDeleted_ForTest(const std::vector<std::string>& oldFiles) {
        _syncingFilesState.oldStorageFilesToBeDeleted = oldFiles;
    }

private:
    /**
     * Open connection to the sync source for BackupFileCloners.
     */
    Status _connect(WithLock);

    /**
     * Start an initial sync attempt.
     */
    ExecutorFuture<OpTimeAndWallTime> _startInitialSyncAttempt(
        WithLock lock,
        std::shared_ptr<executor::TaskExecutor> executor,
        OperationContext* opCtx,
        const CancellationToken& token);

    /**
     * Select a sync source and validate that it can be used for file copy based initial sync.
     */
    ExecutorFuture<HostAndPort> _selectAndValidateSyncSource(
        WithLock lock,
        std::shared_ptr<executor::TaskExecutor> executor,
        OperationContext* opCtx,
        const CancellationToken& token);

    /**
     * Obtains a valid sync source from the sync source selector.
     * Returns error if a sync source cannot be found.
     */
    StatusWith<HostAndPort> _chooseSyncSource(WithLock);

    /**
     * Returns true if we are still processing initial sync tasks (_state is either Running or
     * Shutdown).
     */
    bool _isActive(WithLock) const;

    /**
     * Cancels all outstanding work.
     * Used by shutdown().
     */
    void _cancelRemainingWork(WithLock);

    /**
     * Invokes completion callback and transitions state to State::kComplete.
     */
    void _finishCallback(StatusWith<OpTimeAndWallTime> lastApplied);

    /**
     * Starts syncing files from the sync source.
     *  1- Get the list of the files to be cloned from the cursors on the sync source.
     *  2- Clone the files and put them in '.initialsync' directory.
     */
    ExecutorFuture<void> _startSyncingFiles(std::shared_ptr<executor::TaskExecutor> executor,
                                            const CancellationToken& token);

    /**
     * Starts deleting old storage files phase.
     *  1- Get list of files to be deleted from local cursor.
     *  2- Grab global lock.
     *  3- Switch storage engines.
     *  4- Clean up local DBs.
     *  5- Delete the list of the files.
     */
    ExecutorFuture<void> _startDeletingOldStorageFilesPhase();

    /**
     * Gets the list of the old storage files that will be deleted in dbpath.
     */
    ExecutorFuture<void> _getListOfOldFilesToBeDeleted();

    /**
     * Starts moving new synced storage files from initial sync directory to dbpath and then removes
     * the initial sync directory.
     */
    ExecutorFuture<void> _startMovingNewStorageFilesPhase();

    /**
     * Opens the right cursor on the sync source and gets the files to be cloned.
     */
    ExecutorFuture<void> _cloneFromSyncSourceCursor();

    /**
     * Opens a backup cursor on the sync source and gets the files to be cloned.
     */
    ExecutorFuture<void> _openBackupCursor(
        std::shared_ptr<BackupFileMetadataCollection> returnedFiles);

    /**
     * Extends the backup cursor on the sync source and gets the files to be cloned.
     */
    ExecutorFuture<void> _extendBackupCursor(
        std::shared_ptr<BackupFileMetadataCollection> returnedFiles);
    /**
     * Uses the BackupFileCloner to copy the files from the sync source.
     */
    ExecutorFuture<void> _cloneFiles(std::shared_ptr<BackupFileMetadataCollection> filesToClone);

    /**
     * Extracts the 'fieldName' from the BSON obj, and raises exception if it doesn't exist.
     */
    BSONElement _getBSONField(const BSONObj& obj,
                              const std::string& fieldName,
                              const std::string& objName);

    /**
     * Gets the 'lastAppliedOpTime' from the sync source node by sending 'replSetGetStatus' command.
     */
    ExecutorFuture<mongo::Timestamp> _getLastAppliedOpTimeFromSyncSource(
        std::shared_ptr<executor::TaskExecutor> executor, const CancellationToken& token);

    // Keep issuing 'getMore' command to keep the backupCursor alive.
    void _keepBackupCursorAlive();

    // Kills the backupCursor if it exists, and cancels backupCursorKeepAliveCancellation.
    // Safe to be called multiple times.
    void _killBackupCursor();

    /**
     * Computes a boost::filesystem::path generic-style relative path (always uses slashes)
     * from a base path and a relative path.
     */
    static std::string _getPathRelativeTo(StringData path, StringData basePath);

    /**
     * Hang in async way if the fail point is enabled.
     */
    ExecutorFuture<void> _hangAsyncIfFailPointEnabled(
        StringData failPoint,
        std::shared_ptr<executor::TaskExecutor> executor,
        const CancellationToken& token);

    //
    // All member variables are labeled with one of the following codes indicating the
    // synchronization rules for accessing them.
    //
    // (R)  Read-only in concurrent operation; no synchronization required.
    // (S)  Self-synchronizing; access in any way from any context.
    // (M)  Reads and writes guarded by _mutex
    // (X)  Reads and writes must be performed in a future chain run on _exec
    // (MX) Must hold _mutex and be in a future chain run on _exec to write; must either hold
    //      _mutex or be in a future chain run on _exec to read.

    mutable Mutex _mutex = MONGO_MAKE_LATCH("FileCopyBasedInitialSyncer::_mutex");  // (S)
    const InitialSyncerInterface::Options _opts;                                    // (R)
    std::unique_ptr<DataReplicatorExternalState> _dataReplicatorExternalState;      // (R)

    // This cannot be shutdown before shutting down FileCopyBasedInitialSyncer itself, since
    // shutdown() (and ~FileCopyBasedInitialSyncer) relies on running cleanup work on _exec.
    std::shared_ptr<executor::TaskExecutor> _exec;  // (R)
    HostAndPort _syncSource;                        // (M)
    std::unique_ptr<DBClientConnection> _client;    // (M)
    ThreadPool* _writerPool;                        // (R)
    StorageInterface* _storage;                     // (R)
    ReplicationProcess* _replicationProcess;        // (S)
    OpTimeAndWallTime _lastApplied;                 // (MX)


    // This is invoked with the final status of the initial sync. If startup() fails, this callback
    // is never invoked. The caller gets the last applied optime when the initial sync completes
    // successfully or an error status.
    // '_onCompletion' is cleared on completion (in _finishCallback()) in order to release any
    // resources that might be held by the callback function object.
    OnCompletionFn _onCompletion;  // (M)

    OpTime _lastFetched;  // (MX)

    // CancellationSource used on stepdown/shutdown to cancel work in all running instances of a
    // PrimaryOnlyService.
    CancellationSource _source;

    // Used to create the DBClientConnection for the cloners
    CreateClientFn _createClientFn;

    // Used to signal changes in _state.
    mutable stdx::condition_variable _stateCondition;

    // Current initial syncer state. See comments for State enum class for details.
    State _state = State::kPreStart;  // (M)

    // The initial sync attempt has been canceled
    bool _attemptCanceled = false;  // (X)

    // Used to keep track of which initial sync attempt we are on.
    std::uint32_t _initialSyncAttempt{0};      // (MX)
    std::uint32_t _initialSyncMaxAttempts{0};  // (MX)

    // Used to keep track of which choose sync source attempt we are on.
    std::uint32_t _chooseSyncSourceAttempt{0};      // (MX)
    std::uint32_t _chooseSyncSourceMaxAttempts{0};  // (MX)

    // Data for retry and error logic used by the cloners.
    std::unique_ptr<InitialSyncSharedData> _sharedData;  // (S)

    // The outage duration allowed before an initial sync fails.  Calculated as
    // the minimum of the replication parameter 'initialSyncTransientErrorRetryPeriodSeconds' and
    // the WiredTiger session timeout (usually 5 minutes).
    Seconds _allowedOutageDuration;  // (X)

    // ExecutorFuture that is resolved when the initial sync has completed with success or failure.
    boost::optional<ExecutorFuture<void>> _startInitialSyncAttemptFuture;

    // Holds the state for _startSyncingFiles async job.
    struct SyncingFilesState {
        SyncingFilesState() = default;

        void reset();

        // Extended cursor sends all log files created since the backupCursor's
        // checkpointTimestamp till the extendTo timestamp, so we need to get the
        // difference between the files returned by the consecutive backupCursorExtend to
        // clone only the new log files added since the previous backupCursorExtend.
        BackupFileMetadataCollection getNewFilesToClone(
            const BackupFileMetadataCollection& backupCursorExtendFiles, WithLock lk);

        // The backupCursor used for initialSync, we should always have only one backupCursor opened
        // on the sync source.
        boost::optional<mongo::UUID> backupId;
        CursorId backupCursorId;
        std::string backupCursorCollection;
        CancellationSource backupCursorKeepAliveCancellation;
        boost::optional<ExecutorFuture<void>> backupCursorKeepAliveFuture;

        // The last timestamp that the syncing node has caught up to.
        mongo::Timestamp lastSyncedOpTime;
        // The last appliedOp timestamp that the syncing node knows about the sync source node.
        mongo::Timestamp lastAppliedOpTimeOnSyncSrc;
        // Holds the file metadata returned by the backupCursor.
        std::shared_ptr<BackupFileMetadataCollection> backupCursorFiles;
        // Holds the files returned by extending the backupCursor.
        StringSet extendedCursorFiles;
        int fileBasedInitialSyncCycle = 1;

        // The dbpath on the remote host, used for computing relative paths.
        std::string remoteDbpath;                                    // (X)
        std::unique_ptr<BackupFileCloner> currentBackupFileCloner;   // (X)
        std::vector<BackupFileCloner::Stats> backupFileClonerStats;  // (X)
        std::vector<std::string> filesRelativePathsToBeMoved;

        // The syncing file executor.
        std::shared_ptr<executor::TaskExecutor> executor;

        // The syncing file cancellation token.
        CancellationToken token = CancellationToken::uncancelable();

        // List of relative paths to the old storage files in dbbath that will be deleted.
        std::vector<std::string> oldStorageFilesToBeDeleted;
        std::unique_ptr<InitialSyncFileMover> currentFileMover;
    } _syncingFilesState;
};

}  // namespace repl
}  // namespace mongo
