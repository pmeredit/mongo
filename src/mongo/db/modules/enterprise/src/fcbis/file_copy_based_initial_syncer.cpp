/**
 * Copyright (C) 2021-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "file_copy_based_initial_syncer.h"
#include "initial_sync_file_mover.h"

#include "mongo/bson/bsonobj.h"
#include "mongo/db/catalog/catalog_control.h"
#include "mongo/db/catalog_raii.h"
#include "mongo/db/client.h"
#include "mongo/db/concurrency/exception_util.h"
#include "mongo/db/dbdirectclient.h"
#include "mongo/db/dbhelpers.h"
#include "mongo/db/index_builds/index_builds_coordinator.h"
#include "mongo/db/pipeline/aggregate_command_gen.h"
#include "mongo/db/query/client_cursor/cursor_server_params.h"
#include "mongo/db/repl/initial_syncer_common_stats.h"
#include "mongo/db/repl/initial_syncer_factory.h"
#include "mongo/db/repl/last_vote.h"
#include "mongo/db/repl/repl_server_parameters_gen.h"
#include "mongo/db/repl/replication_auth.h"
#include "mongo/db/repl/transaction_oplog_application.h"
#include "mongo/db/server_recovery.h"
#include "mongo/db/shard_role.h"
#include "mongo/db/storage/encryption_hooks.h"
#include "mongo/db/storage/storage_options.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_global_options.h"
#include "mongo/db/transaction/transaction_participant.h"
#include "mongo/db/transaction_resources.h"
#include "mongo/db/wire_version.h"
#include "mongo/executor/scoped_task_executor.h"
#include "mongo/logv2/log.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/future_util.h"
#include "mongo/watchdog/watchdog.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kReplicationInitialSync

namespace mongo {
namespace repl {
using startup_recovery::StartupRecoveryMode;

// Failpoint which causes the file copy based initial sync to hang after opening the backupCursor.
MONGO_FAIL_POINT_DEFINE(fCBISHangAfterOpeningBackupCursor);

// Failpoint which causes the file copy based initial sync to hang after extending the backupCursor.
MONGO_FAIL_POINT_DEFINE(fCBISHangAfterExtendingBackupCursor);

// Failpoint which forces the file copy based initial sync to extend the backupCursor.
MONGO_FAIL_POINT_DEFINE(fCBISForceExtendBackupCursor);

// Failpoint which causes the file copy based initial sync to skip the syncing files phase.
MONGO_FAIL_POINT_DEFINE(fCBISSkipSyncingFilesPhase);

// Failpoint which causes the initial sync function to hang after cloning all backup files.
MONGO_FAIL_POINT_DEFINE(fCBISHangAfterFileCloningAsync);

// Failpoint which causes the initial sync function to hang after cloning all backup files.
MONGO_FAIL_POINT_DEFINE(fCBISHangAfterFileCloning);

// Failpoint which causes the initial sync function to hang before extending the backup cursor.
MONGO_FAIL_POINT_DEFINE(fCBISHangBeforeExtendBackupCursor);

// Failpoint which causes the initial sync function to hang after a backup cursor extend attempt.
MONGO_FAIL_POINT_DEFINE(fCBISHangAfterAttemptingExtendBackupCursor);

// Failpoint which causes the file copy based initial sync to skip switching storage engine.
MONGO_FAIL_POINT_DEFINE(fCBISSkipSwitchingStorage);

// Failpoint which causes the file copy based initial sync to hang before switching storage engine.
MONGO_FAIL_POINT_DEFINE(fCBISHangBeforeSwitchingStorage);

// Failpoint which causes the file copy based initial sync to hang before deleting the old storage
// files.
MONGO_FAIL_POINT_DEFINE(fCBISHangBeforeDeletingOldStorageFiles);

// Failpoint which causes the file copy based initial sync to skip the moving new storage files
// phase.
MONGO_FAIL_POINT_DEFINE(fCBISSkipMovingFilesPhase);

// Failpoint which causes the file copy based initial sync to hang before deleting the delete
// marker.
MONGO_FAIL_POINT_DEFINE(fCBISHangBeforeDeletingTheDeleteMarker);

// Failpoint which causes the file copy based initial sync to hang before moving the new storage
// files.
MONGO_FAIL_POINT_DEFINE(fCBISHangBeforeMovingTheNewFiles);

// Failpoint which causes the file copy based initial sync to hang after moving the new storage
// files.
MONGO_FAIL_POINT_DEFINE(fCBISHangAfterMovingTheNewFiles);

// Failpoint which causes the file copy based initial sync to skip updating '_lastApplied' from
// latest oplog entry.
MONGO_FAIL_POINT_DEFINE(fCBISSkipUpdatingLastApplied);

// Failpoint which causes the file copy based initial sync to hang after attempting to get the last
// applied optime from the sync source.
MONGO_FAIL_POINT_DEFINE(fCBISHangAfterAttemptingGetLastApplied);

// Failpoint which causes the file copy based initial sync function to hang before finishing.
MONGO_FAIL_POINT_DEFINE(fCBISHangBeforeFinish);

MONGO_FAIL_POINT_DEFINE(fCBISHangAfterStartingFileClone);

// Failpoint which allows getting the progress even after initial sync completes.
MONGO_FAIL_POINT_DEFINE(fCBISAllowGettingProgressAfterInitialSyncCompletes);
// Failpoint which causes the file copy based initial sync to hang after deleting old
// storage files and writing the list of files to the delete marker.
MONGO_FAIL_POINT_DEFINE(fCBISHangAfterDeletingOldStorageFiles);

MONGO_FAIL_POINT_DEFINE(fCBISHangAfterShutdownCancellation);

// Failpoint which hangs after FCBIS pauses watchdog checks.
MONGO_FAIL_POINT_DEFINE(fCBISHangAfterPausingWatchdogChecks);

static constexpr Seconds kDenylistDuration(60);

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
    try {
        shutdown().transitional_ignore();
        join();
    } catch (...) {
        reportFailedDestructor(MONGO_SOURCE_LOCATION());
    }
}

std::string FileCopyBasedInitialSyncer::getInitialSyncMethod() const {
    return "fileCopyBased";
}

void FileCopyBasedInitialSyncer::_createOplogIfNeeded(OperationContext* opCtx) {
    // The oplog is not really used, but it must exist to run the local backup cursor to
    // enumerate files to delete.
    Lock::GlobalWrite glk(opCtx);
    AutoGetOplogFastPath oplogRead(opCtx, OplogAccessMode::kRead);
    const auto& oplog = oplogRead.getCollection();
    if (!oplog) {
        uassertStatusOK(_storage->createOplog(opCtx, NamespaceString::kRsOplogNamespace));
    }
}

Status FileCopyBasedInitialSyncer::startup(OperationContext* opCtx,
                                           std::uint32_t initialSyncMaxAttempts) noexcept {
    invariant(opCtx);
    invariant(initialSyncMaxAttempts >= 1U);

    stdx::lock_guard<stdx::mutex> lock(_mutex);
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
    _initialSyncCancellationSource = CancellationSource();
    _stats.initialSyncStart = _exec->now();

    _createOplogIfNeeded(opCtx);

    ExecutorFuture<void> startInitialSyncAttemptFuture =
        _startInitialSyncAttempt(lock, _exec, _initialSyncCancellationSource.token())
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

void FileCopyBasedInitialSyncer::_updateLastAppliedOptime() {
    if (MONGO_unlikely(fCBISSkipUpdatingLastApplied.shouldFail())) {
        // Noop.
        return;
    }

    stdx::lock_guard<stdx::mutex> lock(_mutex);
    auto opCtx = cc().makeOperationContext();
    _lastApplied = _getTopOfOplogOpTimeAndWallTime(opCtx.get());
    invariant(_lastApplied.opTime.getTimestamp() >= _syncingFilesState.lastSyncedStableTimestamp);
}

OpTimeAndWallTime FileCopyBasedInitialSyncer::_getTopOfOplogOpTimeAndWallTime(
    OperationContext* opCtx) {
    BSONObj oplogEntryBSON;
    invariant(Helpers::getLast(opCtx, NamespaceString::kRsOplogNamespace, oplogEntryBSON));

    auto optimeAndWallTime =
        OpTimeAndWallTime::parseOpTimeAndWallTimeFromOplogEntry(oplogEntryBSON);
    invariant(optimeAndWallTime.getStatus(),
              str::stream() << "Found an invalid oplog entry: " << oplogEntryBSON
                            << ", error: " << optimeAndWallTime.getStatus());

    return optimeAndWallTime.getValue();
}

ExecutorFuture<OpTimeAndWallTime> FileCopyBasedInitialSyncer::_startInitialSyncAttempt(
    WithLock lock,
    std::shared_ptr<executor::TaskExecutor> executor,
    const CancellationToken& token) {
    if (storageGlobalParams.engine != "wiredTiger") {
        LOGV2_ERROR(5952600,
                    "File copy based initial sync requires using the WiredTiger storage engine.");
        return future_util_details::makeExecutorFutureWith(
            executor, []() -> StatusWith<OpTimeAndWallTime> {
                return Status({ErrorCodes::IncompatibleServerVersion,
                               str::stream() << "File copy based initial sync requires using the "
                                                "WiredTiger storage engine."});
            });
    }

    _lastApplied = {OpTime(), Date_t()};
    LOGV2(9650300, "Starting FCBIS");
    return AsyncTry([this, self = shared_from_this(), executor, token] {
               stdx::lock_guard<stdx::mutex> lock(_mutex);
               _attemptCancellationSource = CancellationSource(token);
               LOGV2(9650301, "Selecting FCBIS source");
               return _selectAndValidateSyncSource(
                          lock, executor, _attemptCancellationSource.token())
                   .then([this, self = shared_from_this(), executor](HostAndPort) {
                       LOGV2(9650302, "Start syncing files");
                       return _startSyncingFiles(executor, _attemptCancellationSource.token());
                   })
                   .then([this, self = shared_from_this()] {
                       LOGV2(9650303, "Prepare storage directories");
                       return _prepareStorageDirectoriesForMovingPhase();
                   })
                   .then([this, self = shared_from_this()] {
                       LOGV2(9650304, "Start moving storage files");
                       return _startMovingNewStorageFilesPhase();
                   })
                   .then([this, self = shared_from_this()] {
                       LOGV2(9650305, "Update last applied value");
                       _updateLastAppliedOptime();
                       return _lastApplied;
                   });
           })
        .until([this, self = shared_from_this()](StatusWith<OpTimeAndWallTime> result) mutable {
            // Always release the global lock.
            _releaseGlobalLock();

            stdx::lock_guard<stdx::mutex> lock(_mutex);
            auto runTime = _stats.attemptTimer.millis();
            int operationsRetried = 0;
            int totalTimeUnreachableMillis = 0;
            if (_sharedData) {
                stdx::lock_guard<InitialSyncSharedData> sdLock(*_sharedData);
                operationsRetried = _sharedData->getTotalRetries(sdLock);
                totalTimeUnreachableMillis =
                    durationCount<Milliseconds>(_sharedData->getTotalTimeUnreachable(sdLock));
            }
            _stats.initialSyncAttemptInfos.emplace_back(
                InitialSyncer::InitialSyncAttemptInfo{runTime,
                                                      result.getStatus(),
                                                      _syncSource,
                                                      operationsRetried,
                                                      totalTimeUnreachableMillis});

            initial_sync_common_stats::LogInitialSyncAttemptStats(result,
                                                                  _initialSyncAttempt + 1 <
                                                                      _initialSyncMaxAttempts,
                                                                  _getInitialSyncProgress(lock));

            if (!result.isOK()) {
                LOGV2_ERROR(5781900,
                            "File Copy Based initial sync attempt failed",
                            "attemptsLeft"_attr =
                                (_initialSyncMaxAttempts - (_initialSyncAttempt + 1)),
                            "error"_attr = redact(result.getStatus()));
                initial_sync_common_stats::initialSyncFailedAttempts.increment();
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
    const CancellationToken& token) {
    _chooseSyncSourceAttempt = 0;
    _chooseSyncSourceMaxAttempts = static_cast<std::uint32_t>(numInitialSyncConnectAttempts.load());

    return AsyncTry([this, self = shared_from_this(), executor, token] {
               stdx::lock_guard<stdx::mutex> lock(_mutex);
               auto syncSource = _chooseSyncSource(lock);
               if (!syncSource.isOK()) {
                   uassertStatusOK({ErrorCodes::InvalidSyncSource,
                                    "No valid sync source found in current replica set to do an "
                                    "initial sync."});
               }

               // Validate that the sync source meets the requirements for file copy based initial
               // sync.
               // If the sync source does not meet the requirements, mark it as
               // unusable using the denylistSyncSource call and restart at sync source selection.
               const executor::RemoteCommandRequest request(syncSource.getValue(),
                                                            DatabaseName::kAdmin,
                                                            BSON("hello" << 1),
                                                            rpc::makeEmptyMetadata(),
                                                            nullptr);
               return executor->scheduleRemoteCommand(std::move(request), token)
                   .then([this, self = shared_from_this(), syncSource](
                             const executor::TaskExecutor::ResponseStatus& response) {
                       stdx::lock_guard<stdx::mutex> lock(_mutex);
                       uassertStatusOK(response.status);
                       auto commandStatus = getStatusFromCommandResult(response.data);
                       uassertStatusOK(commandStatus);

                       if (!_getBSONField(
                                response.data, "isWritablePrimary", "sync source's hello response")
                                .booleanSafe() &&
                           !_getBSONField(
                                response.data, "secondary", "sync source's hello response")
                                .booleanSafe()) {
                           _opts.syncSourceSelector->denylistSyncSource(
                               syncSource.getValue(), Date_t::now() + kDenylistDuration);
                           return Status({ErrorCodes::InvalidSyncSource,
                                          str::stream() << "Sync source is invalid because it is "
                                                           "not a primary or secondary."});
                       }

                       auto syncSourceMaxWireVersion = _getBSONField(response.data,
                                                                     "maxWireVersion",
                                                                     "sync source's hello response")
                                                           .numberInt();
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
                       return Status::OK();
                   })
                   .then([this, self = shared_from_this(), syncSource, executor, token]() {
                       stdx::lock_guard<stdx::mutex> lock(_mutex);

                       const executor::RemoteCommandRequest request(
                           syncSource.getValue(),
                           DatabaseName::kAdmin,
                           BSON("getParameter" << 1 << "storageGlobalParams.directoryperdb" << 1
                                               << "wiredTigerDirectoryForIndexes" << 1),
                           rpc::makeEmptyMetadata(),
                           nullptr);

                       return executor->scheduleRemoteCommand(std::move(request), token)
                           .then([this, self = shared_from_this(), syncSource](
                                     const executor::TaskExecutor::ResponseStatus& response) {
                               stdx::lock_guard<stdx::mutex> lock(_mutex);
                               uassertStatusOK(response.status);
                               auto commandStatus = getStatusFromCommandResult(response.data);
                               uassertStatusOK(commandStatus);

                               if (storageGlobalParams.directoryperdb !=
                                   _getBSONField(response.data,
                                                 "storageGlobalParams.directoryperdb",
                                                 "sync source's getParameter response")
                                       .booleanSafe()) {
                                   _opts.syncSourceSelector->denylistSyncSource(
                                       syncSource.getValue(), Date_t::now() + kDenylistDuration);
                                   return Status(
                                       {ErrorCodes::InvalidSyncSource,
                                        str::stream()
                                            << "Sync source is invalid because its directoryPerDB "
                                               "parameter does not match the local value."});
                               }

                               if (wiredTigerGlobalOptions.directoryForIndexes !=
                                   _getBSONField(response.data,
                                                 "wiredTigerDirectoryForIndexes",
                                                 "sync source's getParameter response")
                                       .booleanSafe()) {
                                   _opts.syncSourceSelector->denylistSyncSource(
                                       syncSource.getValue(), Date_t::now() + kDenylistDuration);
                                   return Status({ErrorCodes::InvalidSyncSource,
                                                  str::stream()
                                                      << "Sync source is invalid because its "
                                                         "directoryForIndexes parameter does "
                                                         "not match the local value."});
                               }
                               return Status::OK();
                           });
                   })
                   .then([this, self = shared_from_this(), syncSource, executor, token]() {
                       stdx::lock_guard<stdx::mutex> lock(_mutex);
                       const executor::RemoteCommandRequest request(syncSource.getValue(),
                                                                    DatabaseName::kAdmin,
                                                                    BSON("serverStatus" << 1),
                                                                    rpc::makeEmptyMetadata(),
                                                                    nullptr);

                       return executor->scheduleRemoteCommand(std::move(request), token)
                           .then([this, self = shared_from_this(), syncSource](
                                     const executor::TaskExecutor::ResponseStatus& response)
                                     -> StatusWith<HostAndPort> {
                               stdx::lock_guard<stdx::mutex> lock(_mutex);
                               uassertStatusOK(response.status);
                               auto commandStatus = getStatusFromCommandResult(response.data);
                               uassertStatusOK(commandStatus);

                               auto syncSourceStorageEngine =
                                   _getBSONField(
                                       _getBSONField(response.data,
                                                     "storageEngine",
                                                     "sync source's serverStatus response")
                                           .Obj(),
                                       "name",
                                       "sync source's storageEngine")
                                       .valueStringDataSafe();
                               if (syncSourceStorageEngine != "wiredTiger") {
                                   _opts.syncSourceSelector->denylistSyncSource(
                                       syncSource.getValue(), Date_t::now() + kDenylistDuration);
                                   return Status({ErrorCodes::InvalidSyncSource,
                                                  str::stream() << "Both the sync source and the "
                                                                   "local node must be using the "
                                                                   "WiredTiger storage engine."});
                               }
                               auto opCtxHolder = cc().makeOperationContext();
                               auto opCtx = opCtxHolder.get();
                               auto encHooks = EncryptionHooks::get(opCtx->getServiceContext());
                               // If the sync source doesn't have the encryptionAtRest field, that
                               // means it is not using the encrypted storage engine, so the local
                               // node should not be either. Otherwise, if the sync source has the
                               // encryptionAtRest.encryptionEnabled field, check if it matches the
                               // local node's value.
                               if ((!response.data.hasField("encryptionAtRest") &&
                                    encHooks->enabled()) ||
                                   (response.data.hasField("encryptionAtRest") &&
                                    encHooks->enabled() !=
                                        _getBSONField(
                                            _getBSONField(response.data,
                                                          "encryptionAtRest",
                                                          "sync source's serverStatus response")
                                                .Obj(),
                                            "encryptionEnabled",
                                            "sync source's serverStatus response")
                                            .booleanSafe())) {
                                   _opts.syncSourceSelector->denylistSyncSource(
                                       syncSource.getValue(), Date_t::now() + kDenylistDuration);
                                   return Status({ErrorCodes::InvalidSyncSource,
                                                  str::stream() << "Both the sync source and the "
                                                                   "local node must be "
                                                                   "using the encrypted storage "
                                                                   "engine, or neither."});
                               }
                               return syncSource;
                           });
                   });
           })
        .until([this, self = shared_from_this()](StatusWith<HostAndPort> status) mutable {
            stdx::lock_guard<stdx::mutex> lock(_mutex);
            if (!status.isOK()) {
                LOGV2(5780601,
                      "File copy based initial sync failed to choose sync source",
                      "error"_attr = status.getStatus());
                ++_chooseSyncSourceAttempt;
                return _chooseSyncSourceAttempt >= _chooseSyncSourceMaxAttempts;
            }

            _syncSource = status.getValue();
            LOGV2(9650306, "Found sync source", "host"_attr = _syncSource.toString());
            return true;
        })
        .withDelayBetweenIterations(_opts.syncSourceRetryWait)
        .on(executor, token);
}

void FileCopyBasedInitialSyncer::_keepBackupCursorAlive(WithLock) {
    if (_initialSyncCancellationSource.token().isCanceled()) {
        return;
    }

    LOGV2_DEBUG(
        5782303,
        2,
        "Starting to periodically send 'getMore' requests to keep the backup cursor alive.");
    invariant(_syncingFilesState.backupId);
    _syncingFilesState.backupCursorKeepAliveCancellation =
        CancellationSource(_syncingFilesState.token);
    executor::RemoteCommandRequest request(
        _syncSource,
        DatabaseName::kLocal,
        std::move(BSON("getMore" << _syncingFilesState.backupCursorId << "collection"
                                 << _syncingFilesState.backupCursorCollection)),
        rpc::makeEmptyMetadata(),
        nullptr);
    // We're not expecting a response, set to fire and forget
    request.fireAndForget = true;

    _syncingFilesState.backupCursorKeepAliveFuture =
        AsyncTry([this, self = shared_from_this(), request] {
            return _syncingFilesState.executor->scheduleRemoteCommand(
                std::move(request),
                _syncingFilesState.backupCursorKeepAliveCancellation
                    .token());  // Ignore the result Future;
        })
            .until([this, self = shared_from_this()](auto&&) { return false; })
            .withDelayBetweenIterations(
                Minutes(kFileCopyBasedInitialSyncKeepBackupCursorAliveIntervalInMin))
            .on(_syncingFilesState.executor,
                _syncingFilesState.backupCursorKeepAliveCancellation.token())
            .onCompletion(
                [this, self = shared_from_this()](auto&&) {});  // Ignore the result Future;
}

void FileCopyBasedInitialSyncer::_killBackupCursor() {
    if (!_syncingFilesState.backupId) {
        return;
    }

    LOGV2(5782305, "Cancelling the 'getMore' requests that keeps the backCursor alive.");
    _syncingFilesState.backupCursorKeepAliveCancellation.cancel();

    LOGV2(5782306, "Trying to kill the backup cursor");
    const auto cmdObj = BSON("killCursors" << _syncingFilesState.backupCursorCollection << "cursors"
                                           << BSON_ARRAY(_syncingFilesState.backupCursorId));
    executor::RemoteCommandRequest request(
        _syncSource, DatabaseName::kLocal, std::move(cmdObj), rpc::makeEmptyMetadata(), nullptr);

    // We're not expecting a response, set to fire and forget
    request.fireAndForget = true;
    return _syncingFilesState.executor
        ->scheduleRemoteCommand(std::move(request), CancellationToken::uncancelable())
        .getAsync([this, self = shared_from_this()](auto&&) {});  // Ignore the result Future;
}

FileCopyBasedInitialSyncer::BackupFileMetadataCollection
FileCopyBasedInitialSyncer::SyncingFilesState::getNewFilesToClone(
    const BackupFileMetadataCollection& backupCursorExtendFiles, Stats* statsPtr, WithLock lk) {
    BackupFileMetadataCollection newFilesToClone;
    std::copy_if(backupCursorExtendFiles.begin(),
                 backupCursorExtendFiles.end(),
                 std::inserter(newFilesToClone, newFilesToClone.begin()),
                 [this](const BSONObj& p) {
                     return extendedCursorFiles.find(p["filename"].str()) ==
                         extendedCursorFiles.end();
                 });
    for (auto it = newFilesToClone.begin(); it != newFilesToClone.end(); it++) {
        extendedCursorFiles.insert((*it)["filename"].str());
        statsPtr->totalFileSize += std::max(0ll, (*it)["fileSize"].safeNumberLong());
    }
    return newFilesToClone;
}

Status FileCopyBasedInitialSyncer::_cleanUpLocalCollectionsAfterSync(
    OperationContext* opCtx,
    StatusWith<BSONObj> swCurrConfig,
    StatusWith<LastVote> swCurrLastVote) {
    // We need to first clear the 'initialSyncId' that was copied from the sync source, and then
    // generate a new one for the syncing node.
    _replicationProcess->getConsistencyMarkers()->clearInitialSyncId(opCtx);
    _replicationProcess->getConsistencyMarkers()->setInitialSyncIdIfNotSet(opCtx);

    LastVote lastVote{OpTime::kInitialTerm, -1};
    if (swCurrLastVote.isOK()) {
        lastVote = swCurrLastVote.getValue();
    }
    // We clear the last vote if this node has not participated in any elections yet, as is the
    // usual case.  If a resyncing node participated in an election, we carry the last vote over
    // from that election.
    writeConflictRetry(opCtx,
                       "clear or carry over lastVote after file copy based initial sync",
                       NamespaceString::kLastVoteNamespace,
                       [opCtx, &lastVote] {
                           WriteUnitOfWork wuow(opCtx);
                           auto coll = acquireCollection(
                               opCtx,
                               CollectionAcquisitionRequest(
                                   NamespaceString(NamespaceString::kLastVoteNamespace),
                                   PlacementConcern{boost::none, ShardVersion::UNSHARDED()},
                                   repl::ReadConcernArgs::get(opCtx),
                                   AcquisitionPrerequisites::kWrite),
                               MODE_X);

                           Helpers::putSingleton(opCtx, coll, lastVote.toBSON());
                           wuow.commit();
                       });

    if (!swCurrConfig.isOK()) {
        return swCurrConfig.getStatus();
    }

    Status status =
        _dataReplicatorExternalState->storeLocalConfigDocument(opCtx, swCurrConfig.getValue());
    if (!status.isOK()) {
        return status;
    }

    return Status::OK();
}

void FileCopyBasedInitialSyncer::SyncingFilesState::reset() {
    if (backupCursorKeepAliveFuture) {
        backupCursorKeepAliveCancellation.cancel();
        backupCursorKeepAliveFuture.value().wait();
        backupCursorKeepAliveFuture = boost::none;
    }
    backupCursorKeepAliveCancellation = {};
    backupId = boost::none;
    backupCursorId = 0;
    backupCursorCollection = {};
    lastSyncedStableTimestamp = {};
    extendBackupAttemptTimedOut = false;
    extendedCursorFiles.clear();
    fileBasedInitialSyncCycle = 1;
    currentBackupFileCloner.reset();
    backupFileClonerStats.clear();
    executor.reset();
    oldStorageFilesToBeDeleted.clear();
    currentFileMover.reset();
    filesRelativePathsToBeMoved.clear();
    globalLock.reset();
    globalLockOpCtx.reset();
    globalLockClient.reset();
    originalDbPath = "";

    token = CancellationToken::uncancelable();
    retryingOperation = boost::none;
    if (watchdogMonitor != nullptr) {
        watchdogMonitor->unpauseChecks();
    }
    watchdogMonitor = nullptr;
}

BSONElement FileCopyBasedInitialSyncer::_getBSONField(const BSONObj& obj,
                                                      const std::string& fieldName,
                                                      const std::string& objName) {
    auto result = obj.getField(fieldName);
    uassert(ErrorCodes::NoSuchKey,
            str::stream() << "Missing '" << fieldName << "' field for " << objName << ".",
            !result.eoo());
    return result;
}

ExecutorFuture<mongo::Timestamp> FileCopyBasedInitialSyncer::_getLastAppliedOpTimeFromSyncSource() {
    return AsyncTry([this, self = shared_from_this()] {
               stdx::lock_guard<stdx::mutex> lock(_mutex);
               executor::RemoteCommandRequest request(_syncSource,
                                                      DatabaseName::kAdmin,
                                                      std::move(BSON("replSetGetStatus" << 1)),
                                                      rpc::makeEmptyMetadata(),
                                                      nullptr);
               return _syncingFilesState.executor
                   ->scheduleRemoteCommand(std::move(request), _syncingFilesState.token)
                   .then([this, self = shared_from_this()](const auto& response) {
                       uassertStatusOK(response.status);
                       auto& reply = response.data;
                       uassertStatusOK(getStatusFromCommandResult(reply));
                       // Parsing replSetGetStatus's reply to get lastAppliedOpTime.
                       // ReplSetGetStatus's reply example:
                       // {
                       //     ...
                       //     "optimes" : {
                       //         ...
                       //         "appliedOpTime" : {
                       //             "ts" : Timestamp(1583385878, 1),
                       //             "t" : NumberLong(3)
                       //         },
                       //         ...
                       //     }
                       //     ...
                       // }
                       auto lastAppliedOpTime = _getBSONField(
                           _getBSONField(
                               _getBSONField(reply, "optimes", "replSetGetStatus's reply").Obj(),
                               "appliedOpTime",
                               "replSetGetStatus's reply.optimes")
                               .Obj(),
                           "ts",
                           "replSetGetStatus's reply.optimes.appliedOpTime");
                       return lastAppliedOpTime.timestamp();
                   });
           })
        .until([this, self = shared_from_this()](StatusWith<Timestamp> result) {
            fCBISHangAfterAttemptingGetLastApplied.pauseWhileSetAndNotCanceled(
                Interruptible::notInterruptible(), _syncingFilesState.token);
            stdx::lock_guard<stdx::mutex> lock(_mutex);
            return !_shouldRetryError(lock, result.getStatus());
        })
        .on(_syncingFilesState.executor, _syncingFilesState.token);
}

bool FileCopyBasedInitialSyncer::_shouldRetryError(WithLock lk, Status status) {
    if (ErrorCodes::isRetriableError(status)) {
        stdx::lock_guard<InitialSyncSharedData> sharedDataLock(*_sharedData);
        return _sharedData->shouldRetryOperation(sharedDataLock,
                                                 &_syncingFilesState.retryingOperation);
    }
    // The status was OK or some error other than a retriable error, so clear the retriable error
    // state and indicate that we should not retry.
    _clearRetriableError(lk);
    return false;
}

void FileCopyBasedInitialSyncer::_clearRetriableError(WithLock lk) {
    _syncingFilesState.retryingOperation = boost::none;
}

ExecutorFuture<void> FileCopyBasedInitialSyncer::_openBackupCursorWithRetry(
    std::shared_ptr<BackupFileMetadataCollection> returnedFiles) {
    return AsyncTry([this, self = shared_from_this(), returnedFiles] {
               return _openBackupCursor(returnedFiles);
           })
        .until([this, self = shared_from_this(), attempt = 0](Status error) mutable {
            attempt++;
            // This error code, despite being category Retriable, is not handled by the internal
            // Fetcher retry logic because we get it in the first getMore, not the aggregate
            // command.
            if (error.code() == ErrorCodes::BackupCursorOpenConflictWithCheckpoint) {
                LOGV2(6196400,
                      "Transient error while opening backup cursor",
                      "error"_attr = error,
                      "attempt"_attr = attempt);
            }
            return attempt >= kFileCopyBasedInitialSyncMaxCursorFetchAttempts ||
                error.code() != ErrorCodes::BackupCursorOpenConflictWithCheckpoint;
        })
        .on(_syncingFilesState.executor, _syncingFilesState.token);
}

ExecutorFuture<void> FileCopyBasedInitialSyncer::_openBackupCursor(
    std::shared_ptr<BackupFileMetadataCollection> returnedFiles) {
    LOGV2_DEBUG(
        5782307, 2, "Trying to open backup cursor on sync source.", "SyncSrc"_attr = _syncSource);
    const auto cmdObj = [this, self = shared_from_this()] {
        AggregateCommandRequest aggRequest(
            NamespaceString::makeCollectionlessAggregateNSS(DatabaseName::kLocal),
            {BSON("$backupCursor" << BSONObj())});
        // We must set a writeConcern on internal commands.
        aggRequest.setWriteConcern(WriteConcernOptions());
        return aggRequest.toBSON();
    }();

    auto fetchStatus = std::make_shared<boost::optional<Status>>();
    auto fetcherCallback = [this, self = shared_from_this(), fetchStatus, returnedFiles](
                               const Fetcher::QueryResponseStatus& dataStatus,
                               Fetcher::NextAction* nextAction,
                               BSONObjBuilder* getMoreBob) {
        // Throw out any accumulated results on error
        if (!dataStatus.isOK()) {
            *fetchStatus = dataStatus.getStatus();
            returnedFiles->clear();
            return;
        }

        if (_syncingFilesState.token.isCanceled()) {
            *fetchStatus = Status(ErrorCodes::CallbackCanceled,
                                  "Syncing files during file copy based initial sync interrupted");
            returnedFiles->clear();
            return;
        }

        const auto& data = dataStatus.getValue();
        for (const BSONObj& doc : data.documents) {
            stdx::lock_guard<stdx::mutex> lock(_mutex);
            if (!_syncingFilesState.backupId) {
                // First batch must contain the metadata.
                // Parsing the metadata to get backupId and checkpointTimestamp for the
                // the backupCursor.
                const auto& metaData =
                    _getBSONField(doc, "metadata", "backupCursor's first batch").Obj();
                _syncingFilesState.backupId = UUID(uassertStatusOK(UUID::parse(
                    _getBSONField(metaData, "backupId", "backupCursor's first batch.metadata"))));
                _syncingFilesState.lastSyncedStableTimestamp =
                    _getBSONField(
                        metaData, "checkpointTimestamp", "backupCursor's first batch.metadata")
                        .timestamp();
                _syncingFilesState.remoteDbpath =
                    _getBSONField(metaData, "dbpath", "remote dbpath").str();
                _syncingFilesState.backupCursorId = data.cursorId;
                _syncingFilesState.backupCursorCollection = data.nss.coll().toString();
                LOGV2(5782308,

                      "Opened backup cursor on sync source.",
                      "SyncSrc"_attr = _syncSource,
                      "backupId"_attr = _syncingFilesState.backupId.value(),
                      "lastSyncedStableTimestamp"_attr =
                          _syncingFilesState.lastSyncedStableTimestamp,
                      "backupCursorId"_attr = _syncingFilesState.backupCursorId,
                      "backupCursorCollection"_attr = _syncingFilesState.backupCursorCollection);
            } else {
                // Ensure filename field exists.
                _getBSONField(doc, "filename", "backupCursor's batches");
                _stats.totalFileSize += std::max(0ll, doc["fileSize"].safeNumberLong());
                returnedFiles->emplace_back(doc.getOwned());
            }
        }

        *fetchStatus = Status::OK();
        if (!getMoreBob || data.documents.empty()) {
            // Exist fetcher but keep the backupCursor alive.
            *nextAction = Fetcher::NextAction::kExitAndKeepCursorAlive;
            return;
        }

        getMoreBob->append("getMore", data.cursorId);
        getMoreBob->append("collection", data.nss.coll());
    };

    auto fetcher = std::make_shared<Fetcher>(
        _syncingFilesState.executor.get(),
        _syncSource,
        DatabaseName::kLocal,
        cmdObj,
        fetcherCallback,
        ReadPreferenceSetting(ReadPreference::PrimaryPreferred).toContainingBSON(),
        executor::RemoteCommandRequest::kNoTimeout, /* aggregateNetworkTimeout */
        executor::RemoteCommandRequest::kNoTimeout, /* getMoreNetworkTimeout */
        RemoteCommandRetryScheduler::makeRetryPolicy<ErrorCategory::RetriableError>(
            kFileCopyBasedInitialSyncMaxCursorFetchAttempts,
            executor::RemoteCommandRequest::kNoTimeout),
        transport::kGlobalSSLMode);

    uassertStatusOK(fetcher->schedule());

    return fetcher->onCompletion()
        .thenRunOn(_syncingFilesState.executor)
        .then([fetcher, fetchStatus] {
            // Scoping fetcher to make sure it won't get destructed until fetching is completed.
            if (!*fetchStatus) {
                // The callback never got invoked.
                uasserted(5782302, "Internal error running cursor callback in command");
            }
            uassertStatusOK(fetchStatus->get());
        });
}

ExecutorFuture<void> FileCopyBasedInitialSyncer::_extendBackupCursorWithRetry(
    std::shared_ptr<BackupFileMetadataCollection> returnedFiles) {
    fCBISHangBeforeExtendBackupCursor.pauseWhileSetAndNotCanceled(Interruptible::notInterruptible(),
                                                                  _syncingFilesState.token);
    return AsyncTry([this, self = shared_from_this(), returnedFiles] {
               return _extendBackupCursor(returnedFiles);
           })
        .until([this, self = shared_from_this()](Status error) {
            fCBISHangAfterAttemptingExtendBackupCursor.pauseWhileSetAndNotCanceled(
                Interruptible::notInterruptible(), _syncingFilesState.token);
            stdx::lock_guard<stdx::mutex> lock(_mutex);
            return !_shouldRetryError(lock, error);
        })
        .on(_syncingFilesState.executor, _syncingFilesState.token);
}

ExecutorFuture<void> FileCopyBasedInitialSyncer::_extendBackupCursor(
    std::shared_ptr<BackupFileMetadataCollection> returnedFiles) {
    LOGV2_DEBUG(5782309,
                2,
                "Trying to extend the backup cursor on sync source.",
                "SyncSrc"_attr = _syncSource,
                "backupId"_attr = _syncingFilesState.backupId.value(),
                "extendTo"_attr = _syncingFilesState.lastAppliedOpTimeOnSyncSrc);
    const auto cmdObj = [this, self = shared_from_this()] {
        AggregateCommandRequest aggRequest(
            NamespaceString::makeCollectionlessAggregateNSS(DatabaseName::kLocal),
            {BSON("$backupCursorExtend"
                  << BSON("backupId" << _syncingFilesState.backupId.value() << "timestamp"
                                     << _syncingFilesState.lastAppliedOpTimeOnSyncSrc))});
        // We must set a writeConcern on internal commands.
        aggRequest.setWriteConcern(WriteConcernOptions());
        // The command may not return immediately because it may wait for the node to have the full
        // oplog history up to the backup point in time.
        aggRequest.setMaxTimeMS(fileBasedInitialSyncExtendCursorTimeoutMS);
        return aggRequest.toBSON();
    }();

    auto fetchStatus = std::make_shared<boost::optional<Status>>();
    auto extendedFiles = std::make_shared<BackupFileMetadataCollection>();
    auto fetcherCallback = [this, self = shared_from_this(), fetchStatus, extendedFiles](
                               const Fetcher::QueryResponseStatus& dataStatus,
                               Fetcher::NextAction* nextAction,
                               BSONObjBuilder* getMoreBob) {
        stdx::lock_guard<stdx::mutex> lock(_mutex);
        // Throw out any accumulated results on error
        if (!dataStatus.isOK()) {
            *fetchStatus = dataStatus.getStatus();
            extendedFiles->clear();
            return;
        }

        if (_syncingFilesState.token.isCanceled()) {
            *fetchStatus = Status(ErrorCodes::CallbackCanceled,
                                  "Syncing files during file copy based initial sync interrupted");
            extendedFiles->clear();
            return;
        }

        const auto& data = dataStatus.getValue();
        for (const BSONObj& doc : data.documents) {
            // Ensure filename field exists.
            _getBSONField(doc, "filename", "extended backupCursor's batches");
            _stats.totalExtendedFileSize += std::max(0ll, doc["fileSize"].safeNumberLong());
            extendedFiles->emplace_back(doc.getOwned());
        }

        *fetchStatus = Status::OK();
        if (!getMoreBob || !data.documents.size()) {
            // Stop the fetcher.
            return;
        }

        getMoreBob->append("getMore", data.cursorId);
        getMoreBob->append("collection", data.nss.coll());
    };

    auto fetcher = std::make_shared<Fetcher>(
        _syncingFilesState.executor.get(),
        _syncSource,
        DatabaseName::kLocal,
        cmdObj,
        fetcherCallback,
        ReadPreferenceSetting(ReadPreference::PrimaryPreferred).toContainingBSON(),
        executor::RemoteCommandRequest::kNoTimeout, /* aggregateNetworkTimeout */
        executor::RemoteCommandRequest::kNoTimeout, /* getMoreNetworkTimeout */
        RemoteCommandRetryScheduler::makeRetryPolicy<ErrorCategory::RetriableError>(
            kFileCopyBasedInitialSyncMaxCursorFetchAttempts,
            executor::RemoteCommandRequest::kNoTimeout),
        transport::kGlobalSSLMode);
    uassertStatusOK(fetcher->schedule());

    return fetcher->onCompletion()
        .thenRunOn(_syncingFilesState.executor)
        .then([this,
               self = shared_from_this(),
               fetcher,
               fetchStatus,
               returnedFiles,
               extendedFiles,
               statsPtr = &_stats] {
            stdx::lock_guard<stdx::mutex> lock(_mutex);
            if (!*fetchStatus) {
                // The callback never got invoked.
                uasserted(5782301, "Internal error running cursor callback in command");
            }
            // If there is a MaxTimeMSExpired error, set this field to true so when the error is
            // returned to the caller, it will end the extension round but still continue with
            // initial sync.
            if (fetchStatus->get().code() == ErrorCodes::MaxTimeMSExpired) {
                _syncingFilesState.extendBackupAttemptTimedOut = true;
            }
            uassertStatusOK(fetchStatus->get());
            *returnedFiles = _syncingFilesState.getNewFilesToClone(*extendedFiles, statsPtr, lock);
        });
}

ExecutorFuture<void> FileCopyBasedInitialSyncer::_cloneFromSyncSourceCursor() {
    if (_syncingFilesState.fileBasedInitialSyncCycle == 1) {
        // Open backupCursor during the first cyle.
        invariant(!_syncingFilesState.backupId);
        auto returnedFiles = std::make_shared<BackupFileMetadataCollection>();
        return _openBackupCursorWithRetry(returnedFiles)
            .onError([this, self = shared_from_this()](const Status& status) {
                // If backup cursor cannot be opened because the sync source node is fsynclocked,
                // or it already has a backup cursor open, or if the $backupCursor command is not
                // supported.
                if (status.code() == 50887 || status.code() == 50886 || status.code() == 40324) {
                    _opts.syncSourceSelector->denylistSyncSource(_syncSource,
                                                                 Date_t::now() + kDenylistDuration);
                    LOGV2_ERROR(5973000,
                                "Could not open backup cursor on the sync source",
                                "error"_attr = redact(status));
                    return Status{ErrorCodes::Error(5973001),
                                  "Could not open backup cursor on the sync source: " +
                                      status.reason()};
                }
                return status;
            })
            .then([this, self = shared_from_this(), returnedFiles] {
                stdx::lock_guard<stdx::mutex> lock(_mutex);
                _keepBackupCursorAlive(lock);
                _syncingFilesState.backupCursorFiles = returnedFiles;
                return _hangAsyncIfFailPointEnabled("fCBISHangAfterOpeningBackupCursor",
                                                    _syncingFilesState.executor,
                                                    _syncingFilesState.token);
            })
            .then([this, self = shared_from_this(), returnedFiles] {
                stdx::lock_guard lock(_mutex);
                uassertStatusOK(_connect(lock));
                _sharedData = std::make_unique<InitialSyncSharedData>(
                    -1 /* Rollback ID; not used for file copy based initial sync */,
                    _allowedOutageDuration,
                    getGlobalServiceContext()->getFastClockSource());
                return _cloneFiles(returnedFiles);
            });
    }

    invariant(_syncingFilesState.backupId);
    // Extend the backupCursor opened in the first cycle.
    auto returnedFiles = std::make_shared<BackupFileMetadataCollection>();
    return _extendBackupCursorWithRetry(returnedFiles)
        .then([this, self = shared_from_this(), returnedFiles] {
            stdx::lock_guard<stdx::mutex> lock(_mutex);
            _syncingFilesState.lastSyncedStableTimestamp =
                _syncingFilesState.lastAppliedOpTimeOnSyncSrc;
            return _hangAsyncIfFailPointEnabled("fCBISHangAfterExtendingBackupCursor",
                                                _syncingFilesState.executor,
                                                _syncingFilesState.token);
        })
        .then([this, self = shared_from_this(), returnedFiles] {
            stdx::lock_guard<stdx::mutex> lock(_mutex);
            return _cloneFiles(returnedFiles);
        });
}

ExecutorFuture<void> FileCopyBasedInitialSyncer::_startSyncingFiles(
    std::shared_ptr<executor::TaskExecutor> executor, const CancellationToken& token) {
    stdx::lock_guard<stdx::mutex> lock(_mutex);
    auto retryPeriod = initialSyncTransientErrorRetryPeriodSeconds.load();
    auto maxIdleTime = getCursorTimeoutMillis();
    // There's no point in waiting out an outage longer than our idle time, as the source will have
    // closed the cursor and we'll fail.
    if (retryPeriod > maxIdleTime) {
        LOGV2(5877602,
              "Parameter 'initialSyncTransientErrorRetryPeriodSeconds' is set to a value larger "
              "than the maximum idle time of a file copy based initial sync; actual retry period "
              "will be reduced to the idle time",
              "initialSyncTransientErrorRetryPeriodSeconds"_attr = retryPeriod,
              "maxIdleTime"_attr = maxIdleTime);
    }
    _allowedOutageDuration = Seconds(retryPeriod);

    // Initialize the state.
    _killBackupCursor();
    _syncingFilesState.reset();
    _syncingFilesState.executor = executor;
    _syncingFilesState.token = token;
    _syncingFilesState.originalDbPath = storageGlobalParams.dbpath;
    _stats.reset();

    if (MONGO_unlikely(fCBISSkipSyncingFilesPhase.shouldFail())) {
        // Noop.
        return ExecutorFuture<void>(executor);
    }

    // Some of our unit tests utilize the 'fCBISSkipSyncingFilesPhase' failpoint to skip this stage
    // and therefore need to create the mock storage files in the '.initialsync' directory before
    // the initial sync component actually starts up. To prevent the unit test from deleting these
    // storage files before we've actually executed the test logic, we need to delete the
    // '.initialsync' directory after the failpoint.
    InitialSyncFileMover::deleteInitialSyncDir(storageGlobalParams.dbpath);
    return AsyncTry([this, self = shared_from_this()]() mutable {
               return _cloneFromSyncSourceCursor()
                   .then([this, self = shared_from_this()]() {
                       fCBISHangAfterFileCloning.pauseWhileSetAndNotCanceled(
                           Interruptible::notInterruptible(), _syncingFilesState.token);
                       return _hangAsyncIfFailPointEnabled("fCBISHangAfterFileCloningAsync",
                                                           _syncingFilesState.executor,
                                                           _syncingFilesState.token);
                   })
                   .then([this, self = shared_from_this()]() {
                       return _getLastAppliedOpTimeFromSyncSource();
                   })
                   .then([this,
                          self = shared_from_this()](mongo::Timestamp lastAppliedOpTimeOnSyncSrc) {
                       stdx::lock_guard<stdx::mutex> lock(_mutex);
                       _syncingFilesState.lastAppliedOpTimeOnSyncSrc = lastAppliedOpTimeOnSyncSrc;
                   });
           })
        .until([this, self = shared_from_this()](Status cloningStatus) mutable {
            stdx::lock_guard<stdx::mutex> lock(_mutex);
            if (!cloningStatus.isOK()) {
                // Returns the error to the caller.
                return true;
            }
            auto lagInSecs =
                static_cast<int>(_syncingFilesState.lastAppliedOpTimeOnSyncSrc.getSecs() -
                                 _syncingFilesState.lastSyncedStableTimestamp.getSecs());
            if (lagInSecs > fileBasedInitialSyncMaxLagSec ||
                MONGO_unlikely(fCBISForceExtendBackupCursor.shouldFail())) {
                if (++_syncingFilesState.fileBasedInitialSyncCycle <=
                    fileBasedInitialSyncMaxCyclesWithoutProgress) {
                    // We need to extend the backupCursor to get the updates till
                    // lastAppliedOpTimeOnSyncSrc.
                    return false;
                } else {
                    LOGV2_WARNING(5782300,
                                  "Finishing File Copy Based initial sync with undesired lag",
                                  "currentLagInSec"_attr = lagInSecs,
                                  "fileBasedInitialSyncMaxLagSec"_attr =
                                      fileBasedInitialSyncMaxLagSec,
                                  "fileBasedInitialSyncMaxCyclesWithoutProgress"_attr =
                                      fileBasedInitialSyncMaxCyclesWithoutProgress);
                }
            }
            return true;
        })
        .on(_syncingFilesState.executor, _syncingFilesState.token)
        .onCompletion([this, self = shared_from_this()](Status cloningStatus) {
            // Make sure to kill the backupCursor whether cloning succeeds or fails.
            _killBackupCursor();
            auto lagInSecs =
                static_cast<int>(_syncingFilesState.lastAppliedOpTimeOnSyncSrc.getSecs() -
                                 _syncingFilesState.lastSyncedStableTimestamp.getSecs());
            if (_syncingFilesState.extendBackupAttemptTimedOut) {
                // If this field was set during the extension round, then there was a MaxTimeMS
                // Error raised. Since it occurred during the extension round, we can still continue
                // with initial sync, so we log a warning that there was an undesired lag, and
                // return Status::OK,  then continue rather than failing.
                LOGV2_WARNING(7929800,
                              "Finishing File Copy Based initial sync with undesired lag",
                              "currentLagInSec"_attr = lagInSecs,
                              "fileBasedInitialSyncMaxLagSec"_attr = fileBasedInitialSyncMaxLagSec,
                              "fileBasedInitialSyncMaxCyclesWithoutProgress"_attr =
                                  fileBasedInitialSyncMaxCyclesWithoutProgress);
                return Status::OK();
            }
            return cloningStatus;
        });
}

/* static */
std::string FileCopyBasedInitialSyncer::_getPathRelativeTo(StringData path, StringData basePath) {
    uassert(5877601,
            str::stream() << "The file " << path << " is not a subdirectory of " << basePath,
            path.startsWith(basePath));
    size_t startAt = basePath.size();
    // skip separators at the beginning of the relative part.
    while (startAt < path.size() && (path[startAt] == '/' || path[startAt] == '\\'))
        startAt++;
    std::string result(path.size() - startAt, 0);
    const auto* srcp = path.rawData() + startAt;
    auto* destp = result.data();
    for (; startAt < path.size(); ++startAt, ++srcp, ++destp) {
        if (*srcp == '\\')
            *destp = '/';
        else
            *destp = *srcp;
    }
    return result;
}

ExecutorFuture<void> FileCopyBasedInitialSyncer::_cloneFiles(
    std::shared_ptr<BackupFileMetadataCollection> filesToClone) {
    return AsyncTry([this, self = shared_from_this(), filesToClone, fileIndex = size_t(0)]() mutable
                    -> ExecutorFuture<bool> {
               stdx::lock_guard lock(_mutex);
               // Returns "true" only when all files are finished cloning.
               if (fileIndex == filesToClone->size())
                   return ExecutorFuture<bool>(_syncingFilesState.executor, true);
               auto metadata = (*filesToClone)[fileIndex];
               auto fileName = metadata["filename"].str();
               LOGV2_DEBUG(5877600,
                           1,
                           "Attempting to clone file in FileCopyBasedInitialSyncer",
                           "fileName"_attr = fileName,
                           "metadata"_attr = metadata,
                           "fileIndex"_attr = fileIndex);
               ++fileIndex;
               auto relativePath = _getPathRelativeTo(fileName, _syncingFilesState.remoteDbpath);
               // Note that fileSize is not present in backup extensions.  It's only used for the
               // progress indicator so that's OK.  It will never be negative, but limiting it to
               // zero avoids static-analysis errors.
               size_t fileSize = std::max(0ll, metadata["fileSize"].safeNumberLong());
               // Extension numbers are specified to start at 1 for the first extension.  The
               // cycle starts at 1 for the initial non-extended backup.  So subtract 1 for the
               // correct extension number.
               int extensionNumber = _syncingFilesState.fileBasedInitialSyncCycle - 1;
               if (extensionNumber > 0) {
                   _stats.extensionDataSize = fileSize;
               }
               _syncingFilesState.currentBackupFileCloner =
                   std::make_unique<BackupFileCloner>(*_syncingFilesState.backupId,
                                                      fileName,
                                                      fileSize,
                                                      relativePath,
                                                      extensionNumber,
                                                      _sharedData.get(),
                                                      _syncSource,
                                                      _client.get(),
                                                      _storage,
                                                      _writerPool);
               auto [startClonerFuture, startCloner] =
                   _syncingFilesState.currentBackupFileCloner->runOnExecutorEvent(
                       _syncingFilesState.executor.get());
               // runOnExecutorEvent ensures the future is not ready unless an error has occurred.
               if (startClonerFuture.isReady()) {
                   auto status = startClonerFuture.getNoThrow();
                   invariant(!status.isOK());
                   return ExecutorFuture<bool>(_syncingFilesState.executor, status);
               }
               _syncingFilesState.executor->signalEvent(startCloner);
               return std::move(startClonerFuture)
                   .thenRunOn(_syncingFilesState.executor)
                   .then([this, self = shared_from_this()] {
                       {
                           stdx::lock_guard lock(_mutex);
                           _syncingFilesState.backupFileClonerStats.emplace_back(
                               _syncingFilesState.currentBackupFileCloner->getStats());
                           _stats.copiedFileSize +=
                               _syncingFilesState.currentBackupFileCloner->getStats().bytesCopied;
                           _syncingFilesState.currentBackupFileCloner = nullptr;
                       }
                       fCBISHangAfterStartingFileClone.pauseWhileSetAndNotCanceled(
                           Interruptible::notInterruptible(), _syncingFilesState.token);
                       return false;  // Continue the loop.
                   });
           })
        .until([](StatusWith<bool> result) {
            // Stop and return on error or if result is "true".
            return !result.isOK() || result.getValue();
        })
        .on(_syncingFilesState.executor, _syncingFilesState.token)
        .then([](bool) { return; });
}

ExecutorFuture<void> FileCopyBasedInitialSyncer::_hangAsyncIfFailPointEnabled(
    StringData failPoint,
    std::shared_ptr<executor::TaskExecutor> executor,
    const CancellationToken& token) {
    if (MONGO_unlikely(globalFailPointRegistry().find(failPoint)->shouldFail())) {
        // Hang in an async way to let other threads run in unit tests.
        return AsyncTry([this, self = shared_from_this()] {})
            .until([logged = false, failPoint](Status) mutable {
                if (MONGO_unlikely(globalFailPointRegistry().find(failPoint)->shouldFail())) {
                    if (!logged) {
                        logged = true;
                        LOGV2(5972801,
                              "file copy based initial sync: Fail point is enabled. "
                              "Blocking until fail point is disabled.",
                              "failPoint"_attr = failPoint);
                    }
                    return false;
                }

                return true;
            })
            .withDelayBetweenIterations(Milliseconds(100))
            .on(executor, token);
    } else {
        // Noop.
        return ExecutorFuture<void>(executor);
    }
}

ExecutorFuture<void> FileCopyBasedInitialSyncer::_startMovingNewStorageFilesPhase() {
    if (MONGO_unlikely(fCBISSkipMovingFilesPhase.shouldFail())) {
        // Noop.
        return ExecutorFuture<void>(_syncingFilesState.executor);
    }

    return ExecutorFuture<void>(_syncingFilesState.executor)
        .then([this, self = shared_from_this()] {
            _syncingFilesState.filesRelativePathsToBeMoved =
                _syncingFilesState.currentFileMover->createListOfFilesToMove();

            // Writing the files to the move marker.
            _syncingFilesState.currentFileMover->writeMarker(
                _syncingFilesState.filesRelativePathsToBeMoved,
                InitialSyncFileMover::kMovingFilesMarker,
                InitialSyncFileMover::kMovingFilesTmpMarker);
        })
        .then([this, self = shared_from_this()] {
            return _hangAsyncIfFailPointEnabled("fCBISHangBeforeDeletingTheDeleteMarker",
                                                _syncingFilesState.executor,
                                                _syncingFilesState.token)
                .then([this, self = shared_from_this()] {
                    // Deleting the delete marker.
                    _syncingFilesState.currentFileMover->deleteFiles(
                        {InitialSyncFileMover::kFilesToDeleteMarker.toString()});
                });
        })
        .then([this, self = shared_from_this()] {
            return _hangAsyncIfFailPointEnabled("fCBISHangBeforeMovingTheNewFiles",
                                                _syncingFilesState.executor,
                                                _syncingFilesState.token)
                .then([this, self = shared_from_this()] {
                    // Move the new files to dbpath.
                    _syncingFilesState.currentFileMover->moveFilesAndHandleFailure(
                        _syncingFilesState.filesRelativePathsToBeMoved);

                    return _hangAsyncIfFailPointEnabled("fCBISHangAfterMovingTheNewFiles",
                                                        _syncingFilesState.executor,
                                                        _syncingFilesState.token);
                });
        })
        .then([this, self = shared_from_this()] {
            // Open storage in dbPath with the new synced files.
            {
                AlternativeClientRegion globalLockRegion(_getGlobalLockClient());
                auto opCtx = _syncingFilesState.globalLockOpCtx.get();
                _switchStorageTo(opCtx,
                                 boost::none /* relativeToDbPath */,
                                 StartupRecoveryMode::kReplicaSetMember, /* startupRecoveryMode */
                                 _syncingFilesState.token);

                // Make sure the replication coordinator gets durability updates.
                opCtx->getServiceContext()->getStorageEngine()->setJournalListener(
                    _dataReplicatorExternalState->getReplicationJournalListener());

                // Remove the move marker and '.initialsync' directory.
                _syncingFilesState.currentFileMover->completeMovingInitialSyncFiles();

                if (_syncingFilesState.watchdogMonitor) {
                    _syncingFilesState.watchdogMonitor->unpauseChecks();
                }
            }
            LOGV2_DEBUG(5994406,
                        2,
                        "Releasing the global lock for file copy based initial sync after "
                        "switching storage engines.");
            _releaseGlobalLock();
        });
}

ServiceContext::UniqueClient& FileCopyBasedInitialSyncer::_getGlobalLockClient() {
    if (!_syncingFilesState.globalLockClient) {
        invariant(!_syncingFilesState.globalLockOpCtx);
        _syncingFilesState.globalLockClient = cc().getService()->makeClient("Global Lock FCBIS");

        AlternativeClientRegion acr(_syncingFilesState.globalLockClient);
        _syncingFilesState.globalLockOpCtx = cc().makeOperationContext();
        _syncingFilesState.globalLock =
            std::make_unique<Lock::GlobalLock>(_syncingFilesState.globalLockOpCtx.get(), MODE_X);
    }
    return _syncingFilesState.globalLockClient;
}

void FileCopyBasedInitialSyncer::_releaseGlobalLock() {
    _syncingFilesState.globalLock.reset();
    _syncingFilesState.globalLockOpCtx.reset();
    _syncingFilesState.globalLockClient.reset();
}

void FileCopyBasedInitialSyncer::_replicationStartupRecovery() {
    auto opCtx = cc().makeOperationContext();
    // Replay the oplog.  Setting inReplicationRecovery tells storage not to update the
    // size storer (for fastcount), which should be correct already in the files we received.
    inReplicationRecovery(opCtx->getServiceContext()).store(true);
    ON_BLOCK_EXIT([serviceContext = opCtx->getServiceContext()] {
        inReplicationRecovery(serviceContext).store(false);
    });

    // The oplogTruncateAfterPoint is a logged table, so it will be correct whether we have done
    // only an initial backup, or extensions.  We will apply all oplog entries up to the
    // oplogTruncateAfterPoint, and put them in a stable checkpoint.  This may result in our stable
    // optime being ahead of the commit point of the system, but that is OK because we will
    // eventually set our initialDataTimestamp to the end of the oplog, which will prevent us from
    // attempting to move the stable optime backwards.
    _replicationProcess->getReplicationRecovery()->recoverFromOplogAsStandalone(
        opCtx.get(), true /* duringInitialSync */);

    // Index builds may have been started and not completed during oplog application.  Stop
    // those index builds now; they will be resumed during final recovery.
    IndexBuildsCoordinator::get(opCtx.get())->stopIndexBuildsForRollback(opCtx.get());

    // We may not have an oplog if we're not using real storage.
    if (MONGO_unlikely(fCBISSkipSwitchingStorage.shouldFail()))
        return;

    // To make wiredTiger take a stable checkpoint at shutdown.
    auto lastApplied = _getTopOfOplogOpTimeAndWallTime(opCtx.get()).opTime;
    _storage->setStableTimestamp(opCtx.get()->getServiceContext(), lastApplied.getTimestamp());
}

ExecutorFuture<void> FileCopyBasedInitialSyncer::_prepareStorageDirectoriesForMovingPhase() {
    return _getListOfOldFilesToBeDeletedWithRetry()
        .then([this, self = shared_from_this()] {
            {
                LOGV2_DEBUG(8350804,
                            2,
                            "Obtaining the global lock for file copy based initial sync before "
                            "pausing watchdog checks");
                AlternativeClientRegion globalLockRegion(_getGlobalLockClient());
                auto opCtx = _syncingFilesState.globalLockOpCtx.get();

                _syncingFilesState.watchdogMonitor = WatchdogMonitorInterface::get(opCtx);
                if (_syncingFilesState.watchdogMonitor) {
                    _syncingFilesState.watchdogMonitor->pauseChecks();
                }
            }
            return _hangAsyncIfFailPointEnabled("fCBISHangAfterPausingWatchdogChecks",
                                                _syncingFilesState.executor,
                                                _syncingFilesState.token);
        })
        .then([this, self = shared_from_this()] {
            LOGV2_DEBUG(5994400,
                        2,
                        "Obtaining the global lock for file copy based initial sync before "
                        "switching storage engines.");
            {
                AlternativeClientRegion globalLockRegion(_getGlobalLockClient());
                auto opCtx = _syncingFilesState.globalLockOpCtx.get();
                // Get a copy of the local config as it is on disk.  Since we may be in the middle
                // of a reconfig, the on-disk state may be more up-to-date than the in-memory state.
                StatusWith<BSONObj> config =
                    _dataReplicatorExternalState->loadLocalConfigDocument(opCtx);
                // Get a copy of the current last vote document, to keep the RAFT state correct
                // across the storage change.
                StatusWith<LastVote> lastVote =
                    _dataReplicatorExternalState->loadLocalLastVoteDocument(opCtx);

                // Switch storage to '.initialsync' directory.
                _switchStorageTo(
                    opCtx,
                    InitialSyncFileMover::kInitialSyncDir.toString() /* relativeToDbPath */,
                    StartupRecoveryMode::kReplicaSetMember, /* startupRecoveryMode */
                    _syncingFilesState.token);

                // Fix the local collections in '.initialsync' directory before moving it.
                uassertStatusOK(_cleanUpLocalCollectionsAfterSync(opCtx, config, lastVote));
            }

            _releaseGlobalLock();
            _replicationStartupRecovery();

            {
                AlternativeClientRegion globalLockRegion(_getGlobalLockClient());
                auto opCtx = _syncingFilesState.globalLockOpCtx.get();
                // Switch storage to '.initialsync/.dummy' directory to start moving the synced
                // files.
                boost::filesystem::path fileRelativePath(
                    InitialSyncFileMover::kInitialSyncDir.toString());
                fileRelativePath.append(InitialSyncFileMover::kInitialSyncDummyDir.toString());
                _switchStorageTo(opCtx,
                                 fileRelativePath.string() /* relativeToDbPath */,
                                 boost::none /* startupRecoveryMode */,
                                 _syncingFilesState.token);
                // We need to create some local collections while mounted on the
                // '.initialsync/.dummy' directory, in case we attempt to shut down.
                uassertStatusOK(
                    _replicationProcess->getConsistencyMarkers()->createInternalCollections(opCtx));
                _replicationProcess->getConsistencyMarkers()->setInitialSyncFlag(opCtx);
            }

            return _hangAsyncIfFailPointEnabled("fCBISHangBeforeDeletingOldStorageFiles",
                                                _syncingFilesState.executor,
                                                _syncingFilesState.token);
        })
        .then([this, self = shared_from_this()] {
            _syncingFilesState.currentFileMover =
                std::make_unique<InitialSyncFileMover>(_syncingFilesState.originalDbPath);
            // Before we delete the list of old storage files from disk, we write them to the delete
            // marker for recovery purposes.
            _syncingFilesState.currentFileMover->writeMarker(
                _syncingFilesState.oldStorageFilesToBeDeleted,
                InitialSyncFileMover::kFilesToDeleteMarker,
                InitialSyncFileMover::kFilesToDeleteTmpMarker);
            _syncingFilesState.currentFileMover->deleteFiles(
                _syncingFilesState.oldStorageFilesToBeDeleted);

            return _hangAsyncIfFailPointEnabled("fCBISHangAfterDeletingOldStorageFiles",
                                                _syncingFilesState.executor,
                                                _syncingFilesState.token);
        });
}

ExecutorFuture<void> FileCopyBasedInitialSyncer::_getListOfOldFilesToBeDeletedWithRetry() {
    return AsyncTry([this, self = shared_from_this()] { return _getListOfOldFilesToBeDeleted(); })
        .until([this, self = shared_from_this(), attempt = 0](Status error) mutable {
            attempt++;
            // BackupCursorOpenConflictWithCheckpoint is a transient error while establishing backup
            // cursor and we should retry on it.
            if (error.code() == ErrorCodes::BackupCursorOpenConflictWithCheckpoint) {
                LOGV2(8191601,
                      "Transient error while opening backup cursor",
                      "error"_attr = error,
                      "attempt"_attr = attempt);
            }
            return attempt >= kFileCopyBasedInitialSyncMaxCursorFetchAttempts ||
                error.code() != ErrorCodes::BackupCursorOpenConflictWithCheckpoint;
        })
        .on(_syncingFilesState.executor, _syncingFilesState.token);
}

ExecutorFuture<void> FileCopyBasedInitialSyncer::_getListOfOldFilesToBeDeleted() {
    // Files should be already cloned in '.initialSync' directory.
    auto opCtx = cc().makeOperationContext();
    DBDirectClient client(opCtx.get());
    NamespaceString nss = NamespaceString::makeCollectionlessAggregateNSS(DatabaseName::kLocal);

    AggregateCommandRequest aggRequest(nss, {BSON("$backupCursor" << BSONObj())});
    aggRequest.setWriteConcern(WriteConcernOptions());
    auto cursor = uassertStatusOKWithContext(
        DBClientCursor::fromAggregationRequest(
            &client, aggRequest, false /* secondaryOk */, false /* useExhaust */),
        "Failed to establish an aggregation cursor for enumerating old files");

    // The DBClientCursor will not actually close the cursor if we happen to be in global shutdown
    // when the cursor->close() call or cursor destructor is executed.  Leaving a local backup
    // cursor open when we try to execute storage change (because the executor has not been shut
    // down yet) will result in a crash.  (This will kill the cursor twice in the normal case, which
    // shoud be of no consequence).
    ON_BLOCK_EXIT([&client, cursorId = cursor->getCursorId(), nss = cursor->getNamespaceString()] {
        if (cursorId) {
            try {
                client.killCursor(nss, cursorId);
            } catch (...) {
                reportFailedDestructor(MONGO_SOURCE_LOCATION());
            }
        }
    });

    // Traverse local backup cursor and write filenames to oldStorageFilesToBeDeleted.
    while (cursor->more()) {
        BSONObj doc = cursor->nextSafe();
        // The first 'doc' read from the cursor contains metadata and no 'filename' field.
        if (doc.hasField("filename")) {
            auto absoluteFilename = doc.getField("filename").valueStringDataSafe();
            if (!absoluteFilename.startsWith(storageGlobalParams.dbpath)) {
                continue;
            }
            std::string filename =
                _getPathRelativeTo(absoluteFilename.toString(), storageGlobalParams.dbpath);
            _syncingFilesState.oldStorageFilesToBeDeleted.push_back(filename);
        }
    }

    cursor->kill();

    // These fixed-name files exist in the dbpath and are not returned by the backup cursor.
    // Instead, we must explicitly add them to the 'oldStorageFilesToBeDeleted' vector to ensure
    // their deletion.
    _syncingFilesState.oldStorageFilesToBeDeleted.insert(
        _syncingFilesState.oldStorageFilesToBeDeleted.end(),
        {"WiredTiger.wt", "WiredTiger.turtle", "WiredTiger.lock", "storage.bson"});


    return ExecutorFuture<void>(_syncingFilesState.executor);
}

Status FileCopyBasedInitialSyncer::shutdown() {
    // We check the optional inside the lock but don't want to wait on the future inside the lock.
    boost::optional<ExecutorFuture<void>> backupCursorKeepAliveFuture;
    {
        stdx::lock_guard<stdx::mutex> lock(_mutex);
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
        _initialSyncCancellationSource.cancel();

        if (_syncingFilesState.backupCursorKeepAliveFuture) {
            backupCursorKeepAliveFuture = std::move(_syncingFilesState.backupCursorKeepAliveFuture);
        }
    }

    fCBISHangAfterShutdownCancellation.pauseWhileSet();

    if (backupCursorKeepAliveFuture) {
        // Wait for the thread that keeps the backupCursor alive.
        backupCursorKeepAliveFuture.value().wait();
    }

    // If the initial sync attempt has been started, wait for it to be canceled (through
    // _cancelRemainingWork()) and for the onCompletion lambda to run the cleanup work on
    // _exec.
    if (_startInitialSyncAttemptFuture.has_value()) {
        auto status = _startInitialSyncAttemptFuture->getNoThrow();

        // As we don't own the _exec, it may get shutdown before the future is finished, so we need
        // to make sure to call _finishCallback.
        if (_state != State::kComplete) {
            _finishCallback(status);
        }
    }

    _exec = nullptr;
    return Status::OK();
}

void FileCopyBasedInitialSyncer::_switchStorageTo(
    OperationContext* opCtx,
    boost::optional<std::string> relativeToDbPath,
    boost::optional<StartupRecoveryMode> startupRecoveryMode,
    const CancellationToken& token) {
    fCBISHangBeforeSwitchingStorage.pauseWhileSetAndNotCanceled(Interruptible::notInterruptible(),
                                                                token);
    if (MONGO_unlikely(fCBISSkipSwitchingStorage.shouldFail())) {
        // Noop.
        return;
    }

    // Must have the global lock.
    invariant(shard_role_details::getLocker(opCtx)->isW());

    boost::filesystem::path dirPath(_syncingFilesState.originalDbPath);
    if (relativeToDbPath) {
        dirPath.append(relativeToDbPath.value());
    }
    // Creates the directory if it doesn't exist.
    boost::filesystem::create_directories(dirPath);

    LOGV2_DEBUG(5994401,
                2,
                "Starting to switch storage engine. Closing the catalog.",
                "dbPath"_attr = dirPath.string());
    catalog::closeCatalog(opCtx);
    // Reinitializes storage engine and waits for it to complete startup.
    LOGV2_DEBUG(5994403, 2, "Reinitializing storage engine.", "dbPath"_attr = dirPath.string());
    auto lastShutdownState =
        reinitializeStorageEngine(opCtx, StorageEngineInitFlags{}, [&dirPath, opCtx] {
            // Update the global dbpath between shutdown of the old engine and startup of the new.
            storageGlobalParams.dbpath = dirPath.string();
            // Clear the cached oplog pointer in the service context.
            repl::clearLocalOplogPtr(opCtx->getServiceContext());
        });
    opCtx->getServiceContext()->getStorageEngine()->notifyStorageStartupRecoveryComplete();
    invariant(StorageEngine::LastShutdownState::kClean == lastShutdownState);

    if (startupRecoveryMode) {
        LOGV2_DEBUG(5994404,
                    2,
                    "Performing startup recovery after switching storage engine.",
                    "dbPath"_attr = dirPath.string());
        startup_recovery::runStartupRecoveryInMode(
            opCtx, lastShutdownState, startupRecoveryMode.value());
    }
    LOGV2_DEBUG(5994405,
                2,
                "Reopening the catalog after switching storage engine.",
                "dbPath"_attr = dirPath.string());
    catalog::openCatalogAfterStorageChange(opCtx);
}

void FileCopyBasedInitialSyncer::_cancelRemainingWork(WithLock) {
    // Cancel the cancellation source to stop the work being run on the executor.
    _attemptCancellationSource.cancel();

    if (_sharedData) {
        stdx::lock_guard<InitialSyncSharedData> lock(*_sharedData);
        _sharedData->setStatusIfOK(
            lock, Status{ErrorCodes::CallbackCanceled, "Initial sync attempt canceled"});
    }
    if (_client) {
        _client->shutdownAndDisallowReconnect();
    }
    _attemptCanceled = true;
}

void FileCopyBasedInitialSyncer::join() {
    {
        stdx::unique_lock<stdx::mutex> lk(_mutex);
        _stateCondition.wait(lk, [this, &lk]() { return !_isActive(lk); });
    }
    if (_startInitialSyncAttemptFuture) {
        _startInitialSyncAttemptFuture->wait();
    }
}

bool FileCopyBasedInitialSyncer::isActive() const {
    stdx::lock_guard<stdx::mutex> lock(_mutex);
    return _isActive(lock);
}

bool FileCopyBasedInitialSyncer::_isActive(WithLock) const {
    return State::kRunning == _state || State::kShuttingDown == _state;
}

void FileCopyBasedInitialSyncer::cancelCurrentAttempt() {
    stdx::lock_guard lk(_mutex);
    if (_isActive(lk)) {
        // TODO: Log which # attempt we are on.
        LOGV2(5781902, "Cancelling the current file copy based initial sync attempt.");
        _cancelRemainingWork(lk);
    } else {
        LOGV2(5781903,
              "There is no initial sync attempt to cancel because the file copy based "
              "initial syncer is not "
              "currently active.");
    }
}

std::string FileCopyBasedInitialSyncer::Stats::toString() const {
    return toBSON().toString();
}

BSONObj FileCopyBasedInitialSyncer::Stats::toBSON() const {
    BSONObjBuilder bob;
    append(&bob);
    return bob.obj();
}

void FileCopyBasedInitialSyncer::Stats::append(BSONObjBuilder* builder) const {
    if (initialSyncStart != Date_t()) {
        builder->appendDate("initialSyncStart", initialSyncStart);
        auto elapsedDurationEnd = Date_t::now();
        if (initialSyncEnd != Date_t()) {
            builder->appendDate("initialSyncEnd", initialSyncEnd);
            elapsedDurationEnd = initialSyncEnd;
        }
        long long elapsedMillis =
            duration_cast<Milliseconds>(elapsedDurationEnd - initialSyncStart).count();
        builder->appendNumber("totalInitialSyncElapsedMillis",
                              static_cast<long long>(elapsedMillis));
    }

    BSONArrayBuilder arrBuilder(builder->subarrayStart("initialSyncAttempts"));
    for (unsigned int i = 0; i < initialSyncAttemptInfos.size(); ++i) {
        BSONObj obj = initialSyncAttemptInfos[i].toBSON();
        if (builder->len() + obj.objsize() > BSONObjMaxUserSize) {
            arrBuilder.append(BSON("Warning"
                                   << "output truncated due to BSON object limit"));
            break;
        }
        arrBuilder.append(obj);
    }
    arrBuilder.doneFast();
}


BSONObj FileCopyBasedInitialSyncer::_getInitialSyncProgress(WithLock) const {
    BSONObjBuilder bob;
    bob.append("method", "fileCopyBased");

    bob.appendNumber("failedInitialSyncAttempts", static_cast<long long>(_initialSyncAttempt));
    bob.appendNumber("maxFailedInitialSyncAttempts",
                     static_cast<long long>(_initialSyncMaxAttempts));

    _stats.append(&bob);

    if (_state != State::kRunning)
        return bob.obj();


    // The following fields are present if there is a sync in progress

    long long approxTotalDataSize = _stats.totalFileSize + _stats.totalExtendedFileSize;
    bob.appendNumber("approxTotalDataSize", static_cast<long long>(approxTotalDataSize));

    long long approxTotalBytesCopied = _stats.copiedFileSize;
    if (_syncingFilesState.currentBackupFileCloner) {
        approxTotalBytesCopied +=
            _syncingFilesState.currentBackupFileCloner->getStats().bytesCopied;
    }
    bob.appendNumber("approxTotalBytesCopied", static_cast<long long>(approxTotalBytesCopied));
    if (approxTotalBytesCopied > 0) {
        const auto statsObj = bob.asTempObj();
        auto totalInitialSyncElapsedMillis =
            statsObj.getField("totalInitialSyncElapsedMillis").safeNumberLong();
        const auto downloadRate =
            (double)totalInitialSyncElapsedMillis / (double)approxTotalBytesCopied;
        const auto remainingInitialSyncEstimatedMillis =
            downloadRate * (double)(approxTotalDataSize - approxTotalBytesCopied);
        bob.appendNumber("remainingInitialSyncEstimatedMillis",
                         (long long)remainingInitialSyncEstimatedMillis);
    }

    // Total bytes in the initial set of files, before any $backupCursorExtend calls.
    long int initialBackupDataSize = _stats.totalFileSize;
    bob.appendNumber("initialBackupDataSize", static_cast<long long>(initialBackupDataSize));

    // Last OpTime phase to be available in the previous backup cursor phase
    // (not present if in the initial backup phase)
    auto& previousOplogEnd = _syncingFilesState.lastSyncedStableTimestamp;
    if (!previousOplogEnd.isNull()) {
        bob.append("previousOplogEnd", previousOplogEnd);
    }

    auto& currentOplogEnd =
        _syncingFilesState.lastAppliedOpTimeOnSyncSrc;  // Last OpTime guaranteed to be available in
                                                        // the current backup cursor phase
    bob.append("currentOplogEnd", currentOplogEnd);

    auto& syncSourceLastApplied =
        _syncingFilesState
            .lastAppliedOpTimeOnSyncSrc;  // The last applied optime at the sync source, as of the
                                          // start of this backup phase (not present if in the
                                          // initial backup phase)
    if (!syncSourceLastApplied.isNull()) {
        bob.append("syncSourceLastApplied", syncSourceLastApplied);
    }

    int numExtensions = 0;
    if (_syncingFilesState.currentBackupFileCloner) {
        numExtensions = _syncingFilesState.currentBackupFileCloner->getStats().extensionNumber;
    }
    // Number of times we have started a $backupCursorExtend (absent if never)
    if (numExtensions > 0) {
        bob.appendNumber("numExtensions", static_cast<long long>(numExtensions));
    }

    // Total bytes in the current extension (absent if no extension in progress)
    int extensionDataSize = _stats.extensionDataSize;
    if (extensionDataSize > 0) {
        bob.appendNumber("extensionDataSize", static_cast<long long>(extensionDataSize));
    }

    if (_sharedData) {
        stdx::lock_guard<InitialSyncSharedData> sdLock(*_sharedData);
        auto unreachableSince = _sharedData->getSyncSourceUnreachableSince(sdLock);
        if (unreachableSince != Date_t()) {
            bob.append("syncSourceUnreachableSince", unreachableSince);
            bob.append("currentOutageDurationMillis",
                       durationCount<Milliseconds>(_sharedData->getCurrentOutageDuration(sdLock)));
        }
        bob.append("totalTimeUnreachableMillis",
                   durationCount<Milliseconds>(_sharedData->getTotalTimeUnreachable(sdLock)));
    }

    // append each BackupFileCloners stats making sure not to exceed BSONObjMaxUserSize
    BSONArrayBuilder fls(bob.subarrayStart("files"));
    for (auto el = _syncingFilesState.backupFileClonerStats.begin();
         el != _syncingFilesState.backupFileClonerStats.end();
         el++) {
        BSONObjBuilder flStat;
        el->append(&flStat);
        if (bob.len() + flStat.len() > BSONObjMaxUserSize) {
            fls.append(BSON("Warning"
                            << "output truncated due to BSON object limit"));
            break;
        }
        fls.append(flStat.obj());
    }
    fls.doneFast();

    return bob.obj();
}

BSONObj FileCopyBasedInitialSyncer::getInitialSyncProgress() const {
    stdx::lock_guard<stdx::mutex> lock(_mutex);
    // We return an empty BSON object after an initial sync attempt has been successfully
    // completed.
    if (initial_sync_common_stats::initialSyncCompletes.get() > 0 &&
        !MONGO_unlikely(fCBISAllowGettingProgressAfterInitialSyncCompletes.shouldFail())) {
        return BSONObj();
    }

    try {
        return _getInitialSyncProgress(lock);
    } catch (const DBException& e) {
        LOGV2(8423328, "Error creating initial sync progress object", "error"_attr = e.toString());
    }

    return BSONObj();
}

Status FileCopyBasedInitialSyncer::_connect(WithLock) try {
    _client = _createClientFn();
    _client->connect(_syncSource, "FileCopyBasedInitialSyncer", boost::none);
    return replAuthenticate(_client.get())
        .withContext(str::stream() << "Failed to authenticate to " << _syncSource);
} catch (const DBException& e) {
    return e.toStatus();
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
        stdx::lock_guard<stdx::mutex> lock(_mutex);
        _updateStorageTimestampsAfterInitialSync(lastApplied);
        invariant(_onCompletion);
        std::swap(_onCompletion, onCompletion);
    }

    if (MONGO_unlikely(fCBISHangBeforeFinish.shouldFail())) {
        LOGV2(5973002,
              "File copy based initial sync - fCBISHangBeforeFinish fail point "
              "enabled. Blocking until fail point is disabled.",
              "error"_attr = lastApplied.getStatus());
        try {
            fCBISHangBeforeFinish.pauseWhileSetAndNotCanceled(Interruptible::notInterruptible(),
                                                              _syncingFilesState.token);
        } catch (const ExceptionFor<ErrorCodes::Interrupted>& ex) {
            LOGV2(7437500, "fCBISHangBeforeFinish fail point interrupted", "error"_attr = ex);
            // We just continue in this case, because when we exit _finishCallback, either the
            // completion routine must be called or _onCompletion must still be set.  In the latter
            // case we'd call _finishCallback again anyway, so we might as well continue.
        }
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
        stdx::lock_guard<stdx::mutex> lock(_mutex);
        invariant(_state != State::kComplete);
        _state = State::kComplete;
        _markInitialSyncCompleted(lock, lastApplied);
        _stateCondition.notify_all();
    }
    LOGV2(9650307, "FCBIS complete");
}

void FileCopyBasedInitialSyncer::_markInitialSyncCompleted(
    WithLock lock, const StatusWith<OpTimeAndWallTime>& lastApplied) {
    _stats.initialSyncEnd = _exec->now();
    if (!lastApplied.isOK()) {
        initial_sync_common_stats::initialSyncFailures.increment();
        return;
    }

    LOGV2(6119800,
          "File copy based initial sync done",
          "duration"_attr =
              duration_cast<Seconds>(_stats.initialSyncEnd - _stats.initialSyncStart));
    initial_sync_common_stats::initialSyncCompletes.increment();
}

void FileCopyBasedInitialSyncer::_updateStorageTimestampsAfterInitialSync(
    const StatusWith<OpTimeAndWallTime>& lastApplied) {

    if (!lastApplied.isOK()) {
        return;
    }

    auto opCtxHolder = cc().makeOperationContext();
    auto opCtx = opCtxHolder.get();
    const auto lastAppliedOpTime = lastApplied.getValue().opTime;
    auto initialDataTimestamp = lastAppliedOpTime.getTimestamp();

    // A node coming out of initial sync must guarantee at least one oplog document is visible
    // such that others can sync from this node. Oplog visibility is only advanced when applying
    // oplog entries during initial sync. Correct the visibility to match the initial sync time
    // before transitioning to steady state replication.
    const bool orderedCommit = true;
    _storage->oplogDiskLocRegister(opCtx, initialDataTimestamp, orderedCommit);

    // Setting inReplicationRecovery prevents double-counting of reconstructed prepared
    // transactions.
    inReplicationRecovery(opCtx->getServiceContext()).store(true);
    ON_BLOCK_EXIT([serviceContext = opCtx->getServiceContext()] {
        inReplicationRecovery(serviceContext).store(false);
    });
    reconstructPreparedTransactions(opCtx, repl::OplogApplication::Mode::kInitialSync);

    _runPostReplicationStartupStorageInitialization(opCtx);

    // All updates that represent initial sync must be completed before setting the initial data
    // timestamp.
    _storage->setInitialDataTimestamp(opCtx->getServiceContext(), initialDataTimestamp);
}

void FileCopyBasedInitialSyncer::_runPostReplicationStartupStorageInitialization(
    OperationContext* opCtx) {
    // If we didn't actually switch storage, re-running storage initialization can crash.
    if (MONGO_unlikely(fCBISSkipSwitchingStorage.shouldFail()))
        return;
    // This must be run after prepared transactions are reconstructed, or an invariant may occur.
    auto* storageEngine = opCtx->getServiceContext()->getStorageEngine();
    storageEngine->setOldestActiveTransactionTimestampCallback(
        TransactionParticipant::getOldestActiveTimestamp);

    // This handles dropping of drop-pending collections.
    storageEngine->startTimestampMonitor();
}

bool FileCopyBasedInitialSyncer::_isShuttingDown() const {
    stdx::lock_guard<stdx::mutex> lock(_mutex);
    return _isShuttingDown_inlock();
}

bool FileCopyBasedInitialSyncer::_isShuttingDown_inlock() const {
    return State::kShuttingDown == _state;
}

Status FileCopyBasedInitialSyncer::getStartInitialSyncAttemptFutureStatus_forTest() {
    if (_startInitialSyncAttemptFuture.has_value()) {
        if (_startInitialSyncAttemptFuture.value().isReady()) {
            return _startInitialSyncAttemptFuture.value().getNoThrow();
        } else {
            return {ErrorCodes::IllegalOperation, "initial syncer in progress"};
        }
    } else {
        return {ErrorCodes::IllegalOperation, "Initial sync has not started"};
    }
}

Status FileCopyBasedInitialSyncer::waitForStartInitialSyncAttemptFutureStatus_forTest() {
    if (_startInitialSyncAttemptFuture.has_value()) {
        return _startInitialSyncAttemptFuture.value().getNoThrow();
    } else {
        return {ErrorCodes::IllegalOperation, "Initial sync has not started"};
    }
}

HostAndPort FileCopyBasedInitialSyncer::getSyncSource_forTest() {
    return _syncSource;
}

void FileCopyBasedInitialSyncer::setCreateClientFn_forTest(const CreateClientFn& createClientFn) {
    stdx::lock_guard<stdx::mutex> lk(_mutex);
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
            },
            []() {
                InitialSyncFileMover mover(storageGlobalParams.dbpath);
                mover.recoverFileCopyBasedInitialSyncAtStartup();
            });
    });
}  // namespace repl
}  // namespace mongo
