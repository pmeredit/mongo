/**
 * Copyright (C) 2021 MongoDB, Inc.  All Rights Reserved.
 */
#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kReplicationInitialSync

#include "file_copy_based_initial_syncer.h"
#include "mongo/db/repl/initial_syncer_factory.h"
#include "mongo/db/repl/repl_server_parameters_gen.h"
#include "mongo/db/storage/encryption_hooks.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_global_options.h"
#include "mongo/executor/scoped_task_executor.h"
#include "mongo/logv2/log.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/destructor_guard.h"
#include "mongo/util/future_util.h"

namespace mongo {
namespace repl {

FileCopyBasedInitialSyncer::FileCopyBasedInitialSyncer(
    InitialSyncerInterface::Options opts,
    std::unique_ptr<DataReplicatorExternalState> dataReplicatorExternalState,
    ThreadPool* writerPool,
    StorageInterface* storage,
    ReplicationProcess* replicationProcess,
    const OnCompletionFn& onCompletion)
    : _opts(opts),
      _dataReplicatorExternalState(std::move(dataReplicatorExternalState)),
      _exec(_dataReplicatorExternalState->getSharedTaskExecutor()),
      _writerPool(writerPool),
      _storage(storage),
      _replicationProcess(replicationProcess),
      _onCompletion(onCompletion),
      _createClientFn(
          [] { return std::make_unique<DBClientConnection>(true /* autoReconnect */); }) {
    uassert(ErrorCodes::BadValue, "task executor cannot be null", _exec);
    uassert(ErrorCodes::BadValue, "invalid storage interface", _storage);
    uassert(ErrorCodes::BadValue, "invalid replication process", _replicationProcess);
    uassert(ErrorCodes::BadValue, "invalid getMyLastOptime function", _opts.getMyLastOptime);
    uassert(ErrorCodes::BadValue, "invalid setMyLastOptime function", _opts.setMyLastOptime);
    uassert(ErrorCodes::BadValue, "invalid resetOptimes function", _opts.resetOptimes);
    uassert(ErrorCodes::BadValue, "invalid sync source selector", _opts.syncSourceSelector);
    uassert(ErrorCodes::BadValue, "callback function cannot be null", _onCompletion);
}

FileCopyBasedInitialSyncer::~FileCopyBasedInitialSyncer() {
    DESTRUCTOR_GUARD({
        shutdown().transitional_ignore();
        join();
    });
}

Status FileCopyBasedInitialSyncer::startup(OperationContext* opCtx,
                                           std::uint32_t initialSyncMaxAttempts) noexcept {
    invariant(opCtx);
    invariant(initialSyncMaxAttempts >= 1U);

    stdx::lock_guard<Latch> lock(_mutex);
    switch (_state) {
        case State::kPreStart:
            _state = State::kRunning;
            break;
        case State::kRunning:
            return {ErrorCodes::IllegalOperation, "initial syncer already started"};
        case State::kShuttingDown:
            return {ErrorCodes::ShutdownInProgress, "initial syncer shutting down"};
        case State::kComplete:
            return {ErrorCodes::ShutdownInProgress, "initial syncer completed"};
    }

    _initialSyncAttempt = 0;
    _initialSyncMaxAttempts = initialSyncMaxAttempts;
    _source = CancellationSource();

    ExecutorFuture<void> startInitialSyncAttemptFuture =
        _startInitialSyncAttempt(lock, _exec, opCtx, _source.token())
            .onCompletion([this](StatusWith<OpTimeAndWallTime> lastApplied) {
                _finishCallback(lastApplied);
                return lastApplied.getStatus();
            });

    // If the future is ready immediately, this means that the executor must be shutdown and the
    // future failed to be scheduled on the executor. This is because the future can't have been
    // completed through .onCompletion, because _finishCallback also takes the lock we're holding
    // here in startup(). Otherwise, we assume we successfully started the initial sync attempt and
    // store the future.
    if (startInitialSyncAttemptFuture.isReady()) {
        auto status = startInitialSyncAttemptFuture.getNoThrow();
        // The status must be an error because the executor is shutdown.
        invariant(!status.isOK());
        _state = State::kComplete;
        return status;
    }

    _startInitialSyncAttemptFuture.emplace(std::move(startInitialSyncAttemptFuture));
    return Status::OK();
}

ExecutorFuture<OpTimeAndWallTime> FileCopyBasedInitialSyncer::_startInitialSyncAttempt(
    WithLock lock,
    std::shared_ptr<executor::TaskExecutor> executor,
    OperationContext* opCtx,
    const CancellationToken& token) {

    _lastApplied = {OpTime(), Date_t()};

    return AsyncTry([this, executor, opCtx, token] {
               stdx::lock_guard<Latch> lock(_mutex);
               return _selectAndValidateSyncSource(lock, executor, opCtx, token)
                   .then([this](HostAndPort syncSource) { return _lastApplied; });
           })
        .until([this](StatusWith<OpTimeAndWallTime> result) mutable {
            stdx::lock_guard<Latch> lock(_mutex);
            if (!result.isOK()) {
                LOGV2_ERROR(5781900,
                            "File Copy Based initial sync attempt failed -- attempts left: "
                            "{attemptsLeft} cause: "
                            "{error}",
                            "File Copy Based initial sync attempt failed",
                            "attemptsLeft"_attr =
                                (_initialSyncMaxAttempts - (_initialSyncAttempt + 1)),
                            "error"_attr = redact(result.getStatus()));
                if (++_initialSyncAttempt >= _initialSyncMaxAttempts) {
                    // Log with fatal severity because this will eventually cause an fassert in
                    // ReplicationCoordinatorImpl.
                    LOGV2_FATAL_CONTINUE(5781901,
                                         "The maximum number of retries have been exhausted for "
                                         "file copy based initial sync");
                    return true;
                }
                return false;
            }
            return true;
        })
        .withDelayBetweenIterations(_opts.initialSyncRetryWait)
        .on(executor, token);
}

StatusWith<HostAndPort> FileCopyBasedInitialSyncer::_chooseSyncSource(WithLock) {
    auto syncSource = _opts.syncSourceSelector->chooseNewSyncSource(_lastFetched);
    if (syncSource.empty()) {
        return {ErrorCodes::InvalidSyncSource,
                str::stream() << "No valid sync source available. Our last fetched optime: "
                              << _lastFetched.toString()};
    }
    return syncSource;
}


ExecutorFuture<HostAndPort> FileCopyBasedInitialSyncer::_selectAndValidateSyncSource(
    WithLock lock,
    std::shared_ptr<executor::TaskExecutor> executor,
    OperationContext* opCtx,
    const CancellationToken& token) {
    _chooseSyncSourceAttempt = 0;
    _chooseSyncSourceMaxAttempts = static_cast<std::uint32_t>(numInitialSyncConnectAttempts.load());

    return AsyncTry([this, executor, opCtx, token] {
               stdx::lock_guard<Latch> lock(_mutex);
               auto syncSource = _chooseSyncSource(lock);
               if (!syncSource.isOK()) {
                   uassertStatusOK({ErrorCodes::InitialSyncOplogSourceMissing,
                                    "No valid sync source found in current replica set to do an "
                                    "initial sync."});
               }

               // Validate that the sync source meets the requirements for file copy based initial
               // sync.
               // If the sync source does not meet the requirements, mark it as
               // unusable using the denylistSyncSource call and restart at sync source selection.
               constexpr Seconds kDenylistDuration(60);
               const executor::RemoteCommandRequest request(syncSource.getValue(),
                                                            "admin",
                                                            BSON("hello" << 1),
                                                            rpc::makeEmptyMetadata(),
                                                            nullptr);
               return executor->scheduleRemoteCommand(std::move(request), token)
                   .then([this, syncSource, kDenylistDuration](
                             const executor::TaskExecutor::ResponseStatus& response)
                             -> StatusWith<HostAndPort> {
                       uassertStatusOK(response.status);
                       auto commandStatus = getStatusFromCommandResult(response.data);
                       uassertStatusOK(commandStatus);

                       if (!response.data["isWritablePrimary"].booleanSafe() &&
                           !response.data["secondary"].booleanSafe()) {

                           _opts.syncSourceSelector->denylistSyncSource(
                               syncSource.getValue(), Date_t::now() + kDenylistDuration);
                           return Status({ErrorCodes::InvalidSyncSource,
                                          str::stream() << "Sync source is invalid because it is "
                                                           "not a primary or secondary."});
                       }

                       auto syncSourceMaxWireVersion = response.data["maxWireVersion"].numberInt();
                       // The sync source must have a wire version where file copy based initial
                       // sync exists, and the syncing node’s MAX_WIRE_VERSION must be  greater than
                       // or equal to the sync source node’s MAX_WIRE_VERSION.
                       if (syncSourceMaxWireVersion < WireVersion::WIRE_VERSION_51 ||
                           WireVersion::LATEST_WIRE_VERSION < syncSourceMaxWireVersion) {

                           _opts.syncSourceSelector->denylistSyncSource(
                               syncSource.getValue(), Date_t::now() + kDenylistDuration);
                           return Status(
                               {ErrorCodes::InvalidSyncSource,
                                str::stream()
                                    << "Sync source is invalid because it does not have a "
                                       "valid wire version."});
                       }

                       // TODO: Validate other sync source requirements for File Copy Based Initial
                       // Sync.
                       return syncSource;
                   });
           })
        .until([this](StatusWith<HostAndPort> status) mutable {
            stdx::lock_guard<Latch> lock(_mutex);
            if (!status.isOK()) {
                ++_chooseSyncSourceAttempt;
                return _chooseSyncSourceAttempt >= _chooseSyncSourceMaxAttempts;
            }

            _syncSource = status.getValue();
            return true;
        })
        .withDelayBetweenIterations(_opts.syncSourceRetryWait)
        .on(executor, token);
}

Status FileCopyBasedInitialSyncer::shutdown() {
    {
        stdx::lock_guard<Latch> lock(_mutex);
        switch (_state) {
            case State::kPreStart:
                // Transition directly from PreStart to Complete if not started yet.
                _state = State::kComplete;
                return Status::OK();
            case State::kRunning:
                _state = State::kShuttingDown;
                break;
            case State::kShuttingDown:
            case State::kComplete:
                // Nothing to do if we are already in ShuttingDown or Complete state.
                return Status::OK();
        }
        _cancelRemainingWork(lock);
    }

    // If the initial sync attempt has been started, wait for it to be canceled (through
    // _cancelRemainingWork()) and for the onCompletion lambda to run the cleanup work on
    // _exec before we can shut down _exec. For this reason shutdown() must be called before
    // shutting down _exec.
    if (_startInitialSyncAttemptFuture.is_initialized()) {
        _startInitialSyncAttemptFuture.get().wait();
    }

    _exec = nullptr;
    return Status::OK();
}

void FileCopyBasedInitialSyncer::_cancelRemainingWork(WithLock) {
    // Cancel the cancellation source to stop the work being run on the executor.
    _source.cancel();

    if (_client) {
        _client->shutdownAndDisallowReconnect();
    }
    _attemptCanceled = true;
}

void FileCopyBasedInitialSyncer::join() {
    stdx::unique_lock<Latch> lk(_mutex);
    _stateCondition.wait(lk, [this, &lk]() { return !_isActive(lk); });
}

bool FileCopyBasedInitialSyncer::isActive() const {
    stdx::lock_guard<Latch> lock(_mutex);
    return _isActive(lock);
}

bool FileCopyBasedInitialSyncer::_isActive(WithLock) const {
    return State::kRunning == _state || State::kShuttingDown == _state;
}

BSONObj FileCopyBasedInitialSyncer::getInitialSyncProgress() const {
    return BSONObj();
}

void FileCopyBasedInitialSyncer::cancelCurrentAttempt() {
    stdx::lock_guard lk(_mutex);
    if (_isActive(lk)) {
        // TODO: Log which # attempt we are on.
        LOGV2_DEBUG(5781902, 1, "Cancelling the current file copy based initial sync attempt.");
        _cancelRemainingWork(lk);
    } else {
        LOGV2_DEBUG(5781903,
                    1,
                    "There is no initial sync attempt to cancel because the file copy based "
                    "initial syncer is not "
                    "currently active.");
    }
}

Status FileCopyBasedInitialSyncer::_connect() {
    _client = _createClientFn();
    return _client->connect(_syncSource, "FileCopyBasedInitialSyncer", boost::none);
}

void FileCopyBasedInitialSyncer::_finishCallback(StatusWith<OpTimeAndWallTime> lastApplied) {
    // After running callback function, clear '_onCompletion' to release any resources that might be
    // held by this function object.
    // '_onCompletion' must be moved to a temporary copy and destroyed outside the lock in case
    // there is any logic that's invoked at the function object's destruction that might call into
    // this InitialSyncer. 'onCompletion' must be destroyed outside the lock and this should happen
    // before we transition the state to Complete.
    decltype(_onCompletion) onCompletion;
    {
        stdx::lock_guard<Latch> lock(_mutex);
        invariant(_onCompletion);
        std::swap(_onCompletion, onCompletion);
    }

    // Completion callback must be invoked outside mutex.
    try {
        onCompletion(lastApplied);
    } catch (...) {
        LOGV2_WARNING(5781904,
                      "File copy based initial syncer finish callback threw exception",
                      "error"_attr = redact(exceptionToStatus()));
    }

    // Destroy the remaining reference to the completion callback before we transition the state to
    // Complete so that callers can expect any resources bound to '_onCompletion' to be released
    // before InitialSyncer::join() returns.
    onCompletion = {};

    {
        stdx::lock_guard<Latch> lock(_mutex);
        invariant(_state != State::kComplete);
        _state = State::kComplete;
        _stateCondition.notify_all();
    }
}

Status FileCopyBasedInitialSyncer::getStartInitialSyncAttemptFutureStatus_forTest() {
    if (_startInitialSyncAttemptFuture.is_initialized()) {
        if (_startInitialSyncAttemptFuture.get().isReady()) {
            return _startInitialSyncAttemptFuture.get().getNoThrow();
        } else {
            return {ErrorCodes::IllegalOperation, "initial syncer in progress"};
        }
    } else {
        return {ErrorCodes::IllegalOperation, "Initial sync has not started"};
    }
}

HostAndPort FileCopyBasedInitialSyncer::getSyncSource_forTest() {
    return _syncSource;
}

void FileCopyBasedInitialSyncer::setCreateClientFn_forTest(const CreateClientFn& createClientFn) {
    stdx::lock_guard<Latch> lk(_mutex);
    _createClientFn = createClientFn;
}

ServiceContext::ConstructorActionRegisterer fileCopyBasedInitialSyncerRegisterer(
    "FileCopyBasedInitialSyncerRegisterer",
    {"InitialSyncerFactoryRegisterer"}, /* dependency list */
    [](ServiceContext* service) {
        InitialSyncerFactory::get(service)->registerInitialSyncer(
            "fileCopyBased",
            [](InitialSyncerInterface::Options opts,
               std::unique_ptr<DataReplicatorExternalState> dataReplicatorExternalState,
               ThreadPool* writerPool,
               StorageInterface* storage,
               ReplicationProcess* replicationProcess,
               const InitialSyncerInterface::OnCompletionFn& onCompletion) {
                return std::make_unique<FileCopyBasedInitialSyncer>(
                    opts,
                    std::move(dataReplicatorExternalState),
                    writerPool,
                    storage,
                    replicationProcess,
                    onCompletion);
            });
    });
}  // namespace repl
}  // namespace mongo
