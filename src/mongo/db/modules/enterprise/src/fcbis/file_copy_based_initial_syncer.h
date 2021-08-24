/**
 *    Copyright (C) 2021 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include "mongo/client/dbclient_connection.h"
#include "mongo/db/repl/data_replicator_external_state.h"
#include "mongo/db/repl/initial_syncer_interface.h"
#include "mongo/db/repl/repl_server_parameters_gen.h"
#include "mongo/db/repl/replication_process.h"
#include "mongo/db/repl/storage_interface.h"
#include "mongo/executor/scoped_task_executor.h"
#include "mongo/executor/task_executor.h"
#include "mongo/util/concurrency/thread_pool.h"

namespace mongo {
namespace repl {

/**
 * The FileCopyBasedInitialSyncer performs initial sync through a file-copy based method rather than
 * a logical initial sync method.
 */
class FileCopyBasedInitialSyncer final : public InitialSyncerInterface {
    FileCopyBasedInitialSyncer(const FileCopyBasedInitialSyncer&) = delete;
    FileCopyBasedInitialSyncer& operator=(const FileCopyBasedInitialSyncer&) = delete;

public:
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
     * Return _syncSource.
     * For testing only.
     */
    HostAndPort getSyncSource_forTest();

private:
    /**
     * Open connection to the sync source for BackupFileCloners.
     */
    Status _connect();

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
    std::uint32_t _initialSyncAttempt;      // (MX)
    std::uint32_t _initialSyncMaxAttempts;  // (MX)

    // Used to keep track of which choose sync source attempt we are on.
    std::uint32_t _chooseSyncSourceAttempt;      // (MX)
    std::uint32_t _chooseSyncSourceMaxAttempts;  // (MX)


    // ExecutorFuture that is resolved when the initial sync has completed with success or failure.
    boost::optional<ExecutorFuture<void>> _startInitialSyncAttemptFuture;
};

}  // namespace repl
}  // namespace mongo
