/**
 *    Copyright (C) 2021-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <memory>

#include "fcbis/backup_file_cloner.h"
#include "fcbis/initial_sync_file_mover.h"
#include "mongo/client/dbclient_connection.h"
#include "mongo/db/repl/data_replicator_external_state.h"
#include "mongo/db/repl/initial_sync_shared_data.h"
#include "mongo/db/repl/initial_syncer.h"
#include "mongo/db/repl/initial_syncer_interface.h"
#include "mongo/db/repl/repl_server_parameters_gen.h"
#include "mongo/db/repl/replication_process.h"
#include "mongo/db/repl/storage_interface.h"
#include "mongo/db/service_context.h"
#include "mongo/db/startup_recovery.h"
#include "mongo/executor/scoped_task_executor.h"
#include "mongo/executor/task_executor.h"
#include "mongo/util/concurrency/thread_pool.h"
#include "mongo/util/future_util.h"
#include "mongo/util/timer.h"
#include "mongo/watchdog/watchdog.h"

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

    std::string getInitialSyncMethod() const final;

    // Because, unlike logical initial sync, we replace the local database, we do not allow it
    // to be accessed during the initial sync.
    bool allowLocalDbAccess() const final {
        return false;
    }

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
    const BackupFileMetadataCollection& getBackupCursorFiles_forTest() {
        return *_syncingFilesState.backupCursorFiles;
    }

    /**
     * Returns list of files fetched from extended the backupCursor.
     * For testing only.
     */
    StringSet getExtendedBackupCursorFiles_forTest() {
        return _syncingFilesState.extendedCursorFiles;
    }

    static std::string getPathRelativeTo_forTest(StringData path, StringData basePath) {
        return _getPathRelativeTo(path, basePath);
    }

    void setOldStorageFilesToBeDeleted_forTest(const std::vector<std::string>& oldFiles) {
        _syncingFilesState.oldStorageFilesToBeDeleted = oldFiles;
    }

    Status _cleanUpLocalCollectionsAfterSync_forTest(OperationContext* opCtx,
                                                     StatusWith<BSONObj> swCurrConfig,
                                                     StatusWith<LastVote> swLastVote) {
        return _cleanUpLocalCollectionsAfterSync(opCtx, swCurrConfig, swLastVote);
    }

    void setLastSyncedOpTime_forTest(const mongo::Timestamp ts) {
        _syncingFilesState.lastSyncedStableTimestamp = ts;
    }

    /**
     * Returns list of storage files fetched from the local backupCursor.
     * For testing only.
     */
    const std::vector<std::string>& getOldStorageFilesToBeDeleted_forTest() {
        return _syncingFilesState.oldStorageFilesToBeDeleted;
    }

    struct InitialSyncAttemptInfo {
        int durationMillis;
        Status status;
        HostAndPort syncSource;
        // int rollBackId will not be provided, though it is for logical initial sync
        int operationsRetried;
        int totalTimeUnreachableMillis;

        std::string toString() const;
        BSONObj toBSON() const;
        void append(BSONObjBuilder* builder) const;
    };
    struct Stats {
        Date_t initialSyncStart;
        Date_t initialSyncEnd;
        std::vector<InitialSyncer::InitialSyncAttemptInfo> initialSyncAttemptInfos;
        unsigned long totalFileSize{0};
        unsigned long totalExtendedFileSize{0};
        unsigned long copiedFileSize{0};
        unsigned long extensionDataSize{0};
        // Timer for timing how long each initial sync attempt takes.
        Timer attemptTimer;

        std::string toString() const;
        BSONObj toBSON() const;
        void append(BSONObjBuilder* builder) const;
        void reset() {
            totalFileSize = 0;
            totalExtendedFileSize = 0;
            copiedFileSize = 0;
            extensionDataSize = 0;
            attemptTimer.reset();
        }
    };


private:
    /**
     * Open connection to the sync source for BackupFileCloners.
     */
    Status _connect(WithLock);

    /**
     * Create the oplog if it does not exist.
     */
    void _createOplogIfNeeded(OperationContext* opCtx);

    /**
     * Start an initial sync attempt.
     */
    ExecutorFuture<OpTimeAndWallTime> _startInitialSyncAttempt(
        WithLock lock,
        std::shared_ptr<executor::TaskExecutor> executor,
        const CancellationToken& token);

    /**
     * Select a sync source and validate that it can be used for file copy based initial sync.
     */
    ExecutorFuture<HostAndPort> _selectAndValidateSyncSource(
        WithLock lock,
        std::shared_ptr<executor::TaskExecutor> executor,
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
     * Returns true if the initial syncer has received a shutdown request (_state is ShuttingDown).
     */
    bool _isShuttingDown() const;
    bool _isShuttingDown_inlock() const;

    /**
     * Invokes completion callback and transitions state to State::kComplete.
     */
    void _finishCallback(StatusWith<OpTimeAndWallTime> lastApplied);

    /**
     * Mark initial sync stats as completed.
     */
    void _markInitialSyncCompleted(WithLock, const StatusWith<OpTimeAndWallTime>& lastApplied);

    /**
     * Starts syncing files from the sync source.
     *  1- Get the list of the files to be cloned from the cursors on the sync source.
     *  2- Clone the files and put them in '.initialsync' directory.
     */
    ExecutorFuture<void> _startSyncingFiles(std::shared_ptr<executor::TaskExecutor> executor,
                                            const CancellationToken& token);

    /**
     * Prepares the storage directories (dbpath & .initialsync) for moving phase.
     *  1- Get list of files to be deleted from dbpath using local cursor.
     *  2- Grab the global lock.
     *  3- Switch storage engine to '.initialsync'.
     *  4- Clean up local DBs in '.initialsync'.
     *  5- Switch storage engine to '.initialsync/.dummy' temporarily to be able to move files.
     *  5- Delete the list of the files from dbpath.
     */
    ExecutorFuture<void> _prepareStorageDirectoriesForMovingPhase();

    /**
     * Shuts down current storage engine and reinitializes it in 'dbpath/relativeToDbPath'.
     * Closes the catalog before shutting down the storage engine, performs startup recovery if the
     * value of 'startupRecoveryMode' exists, and then opens the catalog.
     *
     * The opCtx must be holding the global lock in exclusive mode.  The class mutex must NOT
     * be held, as this may result in a deadlock.  Thus _switchStorageTo must only access
     * members which do not require the mutex.
     *
     * The cancellation token is used only for failpoints.
     */
    void _switchStorageTo(
        OperationContext* opCtx,
        boost::optional<std::string> relativeToDbPath,
        boost::optional<startup_recovery::StartupRecoveryMode> startupRecoveryMode,
        const CancellationToken& token);

    /**
     * Gets the list of the old storage files that will be deleted in dbpath.
     */
    ExecutorFuture<void> _getListOfOldFilesToBeDeletedWithRetry();
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
     *
     * The WithRetry version retries for kFileCopyBasedInitialSyncMaxCursorFetchAttempts
     * as long as we get transient backup errors from the sync source.
     *
     */
    ExecutorFuture<void> _openBackupCursorWithRetry(
        std::shared_ptr<BackupFileMetadataCollection> returnedFiles);
    ExecutorFuture<void> _openBackupCursor(
        std::shared_ptr<BackupFileMetadataCollection> returnedFiles);

    /**
     * Extends the backup cursor on the sync source and gets the files to be cloned.
     * The WithRetry version retries until the _allowedOutageDuration is exceeded.
     */
    ExecutorFuture<void> _extendBackupCursorWithRetry(
        std::shared_ptr<BackupFileMetadataCollection> returnedFiles);
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
    ExecutorFuture<mongo::Timestamp> _getLastAppliedOpTimeFromSyncSource();

    /**
     * Modifies local documents copied from the sync source to reflect data from the syncing node.
     * This includes updating 'minValid', 'oplogTruncateAfterPoint', 'initialSyncId', the current
     * replica set configuration, and the election collection.
     */
    Status _cleanUpLocalCollectionsAfterSync(OperationContext* opCtx,
                                             StatusWith<BSONObj> swCurrConfig,
                                             StatusWith<LastVote> swCurrLastVote);

    // Keep issuing 'getMore' command to keep the backupCursor alive.
    void _keepBackupCursorAlive(WithLock);

    // Kills the backupCursor if it exists, and cancels backupCursorKeepAliveCancellation.
    // Safe to be called multiple times.
    void _killBackupCursor();

    /**
     * Computes a boost::filesystem::path generic-style relative path (always uses slashes)
     * from a base path and a relative path.
     */
    static std::string _getPathRelativeTo(StringData path, StringData basePath);

    /**
     * If the global lock is not held, create a client and opCtx which holds it, and return
     * a reference to the client.  Otherwise return a reference to the existing client holding
     * the global lock.
     */
    ServiceContext::UniqueClient& _getGlobalLockClient();

    /**
     * Release the global lock, associated opCtx, and associated client.
     *
     * Must not be called from the thread currently associated with the global lock client.
     */
    void _releaseGlobalLock();

    /**
     * Check if a status is one which means there's a retriable error and we should retry the
     * current operation, and records whether an operation is currently being retried.  Note this
     * can only handle one operation at a time.
     */
    bool _shouldRetryError(WithLock lk, Status status);

    /**
     * Indicates we are no longer handling a retriable error.
     */
    void _clearRetriableError(WithLock lk);

    /**
     * Hang in async way if the fail point is enabled.
     */
    ExecutorFuture<void> _hangAsyncIfFailPointEnabled(
        StringData failPoint,
        std::shared_ptr<executor::TaskExecutor> executor,
        const CancellationToken& token);

    /**
     * Recovering without preserving history before top of oplog.
     */
    void _replicationStartupRecovery();

    /**
     * Updates '_lastApplied' with the latest oplog timestamp.
     */
    void _updateLastAppliedOptime();

    /**
     * Gets the OpTime and wall time of the last entry in the oplog.
     */
    OpTimeAndWallTime _getTopOfOplogOpTimeAndWallTime(OperationContext* opCtx);

    /**
     * Updates storage timestamps after initialSync finished using '_lastApplied'.
     */
    void _updateStorageTimestampsAfterInitialSync(const StatusWith<OpTimeAndWallTime>& lastApplied);

    /**
     * Runs startup initialization for the storage engine which is normally run after replication
     * startup (in mongod_main).
     */
    void _runPostReplicationStartupStorageInitialization(OperationContext* opCtx);

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

    mutable stdx::mutex _mutex;                                                 // (S)
    const InitialSyncerInterface::Options _opts;                                // (R)
    std::unique_ptr<DataReplicatorExternalState> _dataReplicatorExternalState;  // (R)

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

    // CancellationSource used on stepdown/shutdown to cancel work in all running instances of an
    // initial sync.
    CancellationSource _initialSyncCancellationSource;  // (MX)

    // CancellationSource used to cancel work in one attempt of a initial sync.
    CancellationSource _attemptCancellationSource;  // (MX)

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
            const BackupFileMetadataCollection& backupCursorExtendFiles,
            Stats* statsPtr,
            WithLock lk);

        // The backupCursor used for initialSync, we should always have only one backupCursor opened
        // on the sync source.
        boost::optional<mongo::UUID> backupId;  // (X)
        CursorId backupCursorId;
        std::string backupCursorCollection;
        CancellationSource backupCursorKeepAliveCancellation;               // (X)
        boost::optional<ExecutorFuture<void>> backupCursorKeepAliveFuture;  // (M)

        // The last timestamp that the syncing node has caught up to which is known to be in the
        // stable checkpoint on the sync source.
        mongo::Timestamp lastSyncedStableTimestamp;  // (X)
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
        std::shared_ptr<executor::TaskExecutor> executor;  // (X)

        // The syncing file cancellation token.
        CancellationToken token = CancellationToken::uncancelable();  // (X)

        // The operation, if any, currently being retried because of a network error.
        InitialSyncSharedData::RetryableOperation retryingOperation;  // (M)

        // List of relative paths to the old storage files in dbpath that will be deleted.
        std::vector<std::string> oldStorageFilesToBeDeleted;
        std::unique_ptr<InitialSyncFileMover> currentFileMover;  // (X)
        ServiceContext::UniqueClient globalLockClient;           // (X)
        ServiceContext::UniqueOperationContext globalLockOpCtx;  // (X)
        std::unique_ptr<Lock::GlobalLock> globalLock;            // (X)
        std::string originalDbPath;                              // (X)

        WatchdogMonitorInterface* watchdogMonitor = nullptr;  // (X)
    } _syncingFilesState;


    BSONObj _getInitialSyncProgress(WithLock) const;
    Stats _stats;  // (M)
};

}  // namespace repl
}  // namespace mongo
