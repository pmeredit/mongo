/**
 * Copyright (C) 2021 MongoDB, Inc.  All Rights Reserved.
 */
#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kReplicationInitialSync

#include "file_copy_based_initial_syncer.h"
#include "initial_sync_file_mover.h"

#include "mongo/bson/bsonobj.h"
#include "mongo/db/cursor_server_params.h"
#include "mongo/db/pipeline/aggregate_command_gen.h"
#include "mongo/db/repl/initial_syncer_factory.h"
#include "mongo/db/repl/repl_server_parameters_gen.h"
#include "mongo/db/repl/replication_auth.h"
#include "mongo/db/storage/encryption_hooks.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_global_options.h"
#include "mongo/executor/scoped_task_executor.h"
#include "mongo/logv2/log.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/destructor_guard.h"
#include "mongo/util/future_util.h"

// Failpoint which causes the initial sync function to hang after cloning all backup files
MONGO_FAIL_POINT_DEFINE(fileCopyBasedInitialSyncHangAfterFileCloning);

namespace mongo {
namespace repl {

// Failpoint which causes the file copy based initial sync to hang after opening the backupCursor.
MONGO_FAIL_POINT_DEFINE(fCBISHangAfterOpeningBackupCursor);

// Failpoint which causes the file copy based initial sync to skip the syncing files phase.
MONGO_FAIL_POINT_DEFINE(fCBISSkipSyncingFilesPhase);

// Failpoint which causes the file copy based initial sync to hang before deleting the old storage
// files.
MONGO_FAIL_POINT_DEFINE(fCBISHangBeforeDeletingOldStorageFiles);

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

    return AsyncTry([this, self = shared_from_this(), executor, opCtx, token] {
               stdx::lock_guard<Latch> lock(_mutex);
               return _selectAndValidateSyncSource(lock, executor, opCtx, token)
                   .then([this, self = shared_from_this(), executor, token](HostAndPort) {
                       return _startSyncingFiles(executor, token);
                   })
                   .then([this, self = shared_from_this()] {
                       return _startDeletingOldStorageFilesPhase();
                   })
                   .then([this, self = shared_from_this()] { return _lastApplied; });
           })
        .until([this, self = shared_from_this()](StatusWith<OpTimeAndWallTime> result) mutable {
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

    return AsyncTry([this, self = shared_from_this(), executor, opCtx, token] {
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
                   .then([this, self = shared_from_this(), syncSource, kDenylistDuration](
                             const executor::TaskExecutor::ResponseStatus& response) {
                       stdx::lock_guard<Latch> lock(_mutex);
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
                       return Status::OK();
                   })
                   .then([this,
                          self = shared_from_this(),
                          opCtx,
                          syncSource,
                          kDenylistDuration,
                          executor,
                          token]() {
                       stdx::lock_guard<Latch> lock(_mutex);

                       const executor::RemoteCommandRequest request(
                           syncSource.getValue(),
                           "admin",
                           BSON("getParameter" << 1 << "'storageGlobalParams.directoryperdb'" << 1
                                               << "'wiredTigerDirectoryForIndexes'" << 1),
                           rpc::makeEmptyMetadata(),
                           nullptr);

                       return executor->scheduleRemoteCommand(std::move(request), token)
                           .then([this,
                                  self = shared_from_this(),
                                  opCtx,
                                  syncSource,
                                  kDenylistDuration](
                                     const executor::TaskExecutor::ResponseStatus& response) {
                               stdx::lock_guard<Latch> lock(_mutex);
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
                   .then([this,
                          self = shared_from_this(),
                          opCtx,
                          syncSource,
                          kDenylistDuration,
                          executor,
                          token]() {
                       stdx::lock_guard<Latch> lock(_mutex);
                       const executor::RemoteCommandRequest request(syncSource.getValue(),
                                                                    "admin",
                                                                    BSON("serverStatus" << 1),
                                                                    rpc::makeEmptyMetadata(),
                                                                    nullptr);

                       return executor->scheduleRemoteCommand(std::move(request), token)
                           .then([this,
                                  self = shared_from_this(),
                                  opCtx,
                                  syncSource,
                                  kDenylistDuration](
                                     const executor::TaskExecutor::ResponseStatus& response)
                                     -> StatusWith<HostAndPort> {
                               stdx::lock_guard<Latch> lock(_mutex);
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

void FileCopyBasedInitialSyncer::_keepBackupCursorAlive() {
    LOGV2_DEBUG(
        5782303,
        2,
        "Starting to periodically send 'getMore' requests to keep the backup cursor alive.");
    invariant(_syncingFilesState.backupId);
    _syncingFilesState.backupCursorKeepAliveCancellation =
        CancellationSource(_syncingFilesState.token);
    executor::RemoteCommandRequest request(
        _syncSource,
        NamespaceString::kAdminDb.toString(),
        std::move(BSON("getMore" << _syncingFilesState.backupCursorId << "collection"
                                 << _syncingFilesState.backupCursorCollection)),
        rpc::makeEmptyMetadata(),
        nullptr);
    // We're not expecting a response, set to fire and forget
    request.fireAndForgetMode = executor::RemoteCommandRequest::FireAndForgetMode::kOn;

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

    LOGV2_DEBUG(5782305, 2, "Cancelling the 'getMore' requests that keeps the backCursor alive.");
    _syncingFilesState.backupCursorKeepAliveCancellation.cancel();

    LOGV2_DEBUG(5782306, 2, "Trying to kill the backup cursor");
    const auto cmdObj = BSON("killCursors" << _syncingFilesState.backupCursorCollection << "cursors"
                                           << BSON_ARRAY(_syncingFilesState.backupCursorId));
    executor::RemoteCommandRequest request(_syncSource,
                                           NamespaceString::kAdminDb.toString(),
                                           std::move(cmdObj),
                                           rpc::makeEmptyMetadata(),
                                           nullptr);

    // We're not expecting a response, set to fire and forget
    request.fireAndForgetMode = executor::RemoteCommandRequest::FireAndForgetMode::kOn;
    return _syncingFilesState.executor
        ->scheduleRemoteCommand(std::move(request), CancellationToken::uncancelable())
        .getAsync([this, self = shared_from_this()](auto&&) {});  // Ignore the result Future;
}

FileCopyBasedInitialSyncer::BackupFileMetadataCollection
FileCopyBasedInitialSyncer::SyncingFilesState::getNewFilesToClone(
    const BackupFileMetadataCollection& backupCursorExtendFiles, WithLock lk) {
    BackupFileMetadataCollection newFilesToClone;
    std::copy_if(backupCursorExtendFiles.begin(),
                 backupCursorExtendFiles.end(),
                 std::inserter(newFilesToClone, newFilesToClone.begin()),
                 [this](const BSONObj& p) {
                     return extendedCursorFiles.find(p["filename"].str()) ==
                         extendedCursorFiles.end();
                 });
    std::for_each(newFilesToClone.begin(), newFilesToClone.end(), [this](const BSONObj& p) {
        extendedCursorFiles.insert(p["filename"].str());
    });
    return newFilesToClone;
}

void FileCopyBasedInitialSyncer::SyncingFilesState::reset() {
    if (backupCursorKeepAliveFuture) {
        backupCursorKeepAliveCancellation.cancel();
        backupCursorKeepAliveFuture.get().wait();
        backupCursorKeepAliveFuture = boost::none;
    }
    backupCursorKeepAliveCancellation = {};
    backupId = boost::none;
    backupCursorId = 0;
    backupCursorCollection = {};
    lastSyncedOpTime = {};
    extendedCursorFiles.clear();
    fileBasedInitialSyncCycle = 1;
    currentBackupFileCloner.reset();
    backupFileClonerStats.clear();
    executor.reset();
    oldStorageFilesToBeDeleted.clear();
    currentFileMover.reset();

    token = CancellationToken::uncancelable();
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

ExecutorFuture<mongo::Timestamp> FileCopyBasedInitialSyncer::_getLastAppliedOpTimeFromSyncSource(
    std::shared_ptr<executor::TaskExecutor> executor, const CancellationToken& token) {
    stdx::lock_guard<Latch> lock(_mutex);
    executor::RemoteCommandRequest request(_syncSource,
                                           NamespaceString::kAdminDb.toString(),
                                           std::move(BSON("replSetGetStatus" << 1)),
                                           rpc::makeEmptyMetadata(),
                                           nullptr);
    return executor->scheduleRemoteCommand(std::move(request), token)
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
                _getBSONField(_getBSONField(reply, "optimes", "replSetGetStatus's reply").Obj(),
                              "appliedOpTime",
                              "replSetGetStatus's reply.optimes")
                    .Obj(),
                "ts",
                "replSetGetStatus's reply.optimes.appliedOpTime");
            return lastAppliedOpTime.timestamp();
        });
}

ExecutorFuture<void> FileCopyBasedInitialSyncer::_openBackupCursor(
    std::shared_ptr<BackupFileMetadataCollection> returnedFiles) {
    LOGV2_DEBUG(
        5782307, 2, "Trying to open backup cursor on sync source.", "SyncSrc"_attr = _syncSource);
    const auto cmdObj = [this, self = shared_from_this()] {
        AggregateCommandRequest aggRequest(
            NamespaceString::makeCollectionlessAggregateNSS(NamespaceString::kAdminDb),
            {BSON("$backupCursor" << BSONObj())});
        // We must set a writeConcern on internal commands.
        aggRequest.setWriteConcern(WriteConcernOptions());
        return aggRequest.toBSON(BSONObj());
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
            if (!_syncingFilesState.backupId) {
                stdx::lock_guard<Latch> lock(_mutex);
                // First batch must contain the metadata.
                // Parsing the metadata to get backupId and checkpointTimestamp for the
                // the backupCursor.
                const auto& metaData =
                    _getBSONField(doc, "metadata", "backupCursor's first batch").Obj();
                _syncingFilesState.backupId = UUID(uassertStatusOK(UUID::parse(
                    _getBSONField(metaData, "backupId", "backupCursor's first batch.metadata"))));
                _syncingFilesState.lastSyncedOpTime =
                    _getBSONField(
                        metaData, "checkpointTimestamp", "backupCursor's first batch.metadata")
                        .timestamp();
                _syncingFilesState.remoteDbpath =
                    _getBSONField(metaData, "dbpath", "remote dbpath").str();
                _syncingFilesState.backupCursorId = data.cursorId;
                _syncingFilesState.backupCursorCollection = data.nss.coll().toString();
                LOGV2_DEBUG(5782308,
                            2,
                            "Opened backup cursor on sync source.",
                            "SyncSrc"_attr = _syncSource,
                            "backupId"_attr = _syncingFilesState.backupId.get(),
                            "lastSyncedOpTime"_attr = _syncingFilesState.lastSyncedOpTime,
                            "backupCursorId"_attr = _syncingFilesState.backupCursorId,
                            "backupCursorCollection"_attr =
                                _syncingFilesState.backupCursorCollection);
            } else {
                // Ensure filename field exists.
                _getBSONField(doc, "filename", "backupCursor's batches");
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
        NamespaceString::kAdminDb.toString(),
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

ExecutorFuture<void> FileCopyBasedInitialSyncer::_extendBackupCursor(
    std::shared_ptr<BackupFileMetadataCollection> returnedFiles) {
    LOGV2_DEBUG(5782309,
                2,
                "Trying to extend the backup cursor on sync source.",
                "SyncSrc"_attr = _syncSource,
                "backupId"_attr = _syncingFilesState.backupId.get(),
                "extendTo"_attr = _syncingFilesState.lastAppliedOpTimeOnSyncSrc);
    const auto cmdObj = [this, self = shared_from_this()] {
        AggregateCommandRequest aggRequest(
            NamespaceString::makeCollectionlessAggregateNSS(NamespaceString::kAdminDb),
            {BSON("$backupCursorExtend"
                  << BSON("backupId" << _syncingFilesState.backupId.get() << "timestamp"
                                     << _syncingFilesState.lastAppliedOpTimeOnSyncSrc))});
        // We must set a writeConcern on internal commands.
        aggRequest.setWriteConcern(WriteConcernOptions());
        // The command may not return immediately because it may wait for the node to have the full
        // oplog history up to the backup point in time.
        aggRequest.setMaxTimeMS(fileBasedInitialSyncExtendCursorTimeoutMS);
        return aggRequest.toBSON(BSONObj());
    }();

    auto fetchStatus = std::make_shared<boost::optional<Status>>();
    auto extendedFiles = std::make_shared<BackupFileMetadataCollection>();
    auto fetcherCallback = [this, self = shared_from_this(), fetchStatus, extendedFiles](
                               const Fetcher::QueryResponseStatus& dataStatus,
                               Fetcher::NextAction* nextAction,
                               BSONObjBuilder* getMoreBob) {
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
        NamespaceString::kAdminDb.toString(),
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
        .then(
            [this, self = shared_from_this(), fetcher, fetchStatus, returnedFiles, extendedFiles] {
                stdx::lock_guard<Latch> lock(_mutex);
                if (!*fetchStatus) {
                    // The callback never got invoked.
                    uasserted(5782301, "Internal error running cursor callback in command");
                }
                uassertStatusOK(fetchStatus->get());
                *returnedFiles = _syncingFilesState.getNewFilesToClone(*extendedFiles, lock);
            });
}

ExecutorFuture<void> FileCopyBasedInitialSyncer::_cloneFromSyncSourceCursor() {
    if (_syncingFilesState.fileBasedInitialSyncCycle == 1) {
        // Open backupCursor during the first cyle.
        invariant(!_syncingFilesState.backupId);
        auto returnedFiles = std::make_shared<BackupFileMetadataCollection>();
        return _openBackupCursor(returnedFiles)
            .then([this, self = shared_from_this(), returnedFiles] {
                stdx::lock_guard<Latch> lock(_mutex);
                _keepBackupCursorAlive();
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
    return _extendBackupCursor(returnedFiles)
        .then([this, self = shared_from_this(), returnedFiles] {
            stdx::lock_guard<Latch> lock(_mutex);
            _syncingFilesState.lastSyncedOpTime = _syncingFilesState.lastAppliedOpTimeOnSyncSrc;
            return _cloneFiles(returnedFiles);
        });
}

ExecutorFuture<void> FileCopyBasedInitialSyncer::_startSyncingFiles(
    std::shared_ptr<executor::TaskExecutor> executor, const CancellationToken& token) {
    stdx::lock_guard<Latch> lock(_mutex);
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

    if (MONGO_unlikely(fCBISSkipSyncingFilesPhase.shouldFail())) {
        // Noop.
        return ExecutorFuture<void>(executor);
    }

    return AsyncTry([this, self = shared_from_this()]() mutable {
               return _cloneFromSyncSourceCursor()
                   .then([this, self = shared_from_this()]() {
                       return _getLastAppliedOpTimeFromSyncSource(_syncingFilesState.executor,
                                                                  _syncingFilesState.token);
                   })
                   .then([this,
                          self = shared_from_this()](mongo::Timestamp lastAppliedOpTimeOnSyncSrc) {
                       stdx::lock_guard<Latch> lock(_mutex);
                       _syncingFilesState.lastAppliedOpTimeOnSyncSrc = lastAppliedOpTimeOnSyncSrc;
                       fileCopyBasedInitialSyncHangAfterFileCloning.pauseWhileSet();
                   });
           })
        .until([this, self = shared_from_this()](Status cloningStatus) mutable {
            stdx::lock_guard<Latch> lock(_mutex);
            if (!cloningStatus.isOK()) {
                // Make sure to kill the backupCursor on failure.
                _killBackupCursor();

                // Returns the error to the caller.
                return true;
            }

            auto lagInSecs =
                static_cast<int>(_syncingFilesState.lastAppliedOpTimeOnSyncSrc.getSecs() -
                                 _syncingFilesState.lastSyncedOpTime.getSecs());
            if (lagInSecs > fileBasedInitialSyncMaxLagSec) {
                if (++_syncingFilesState.fileBasedInitialSyncCycle <=
                    fileBasedInitialSyncMaxCyclesWithoutProgress) {
                    // We need to extend the backupCursor to get the updates till
                    // lastAppliedOpTimeOnSyncSrc.
                    return false;
                } else {
                    LOGV2_WARNING(
                        5782300,
                        "Finishing File Copy Based initial sync with undesired lag "
                        "({currentLagInSec} secs): "
                        "Failed to reduce the lag between the syncing node and the syncSrc to be "
                        "less than '(fileBasedInitialSyncMaxLagSec: "
                        "{fileBasedInitialSyncMaxLagSec} secs)', while running for "
                        "('fileBasedInitialSyncMaxCyclesWithoutProgress:' "
                        "{fileBasedInitialSyncMaxCyclesWithoutProgress} cylces)'.",
                        "Finishing File Copy Based initial sync with undesired lag",
                        "currentLagInSec"_attr = lagInSecs,
                        "fileBasedInitialSyncMaxLagSec"_attr = fileBasedInitialSyncMaxLagSec,
                        "fileBasedInitialSyncMaxCyclesWithoutProgress"_attr =
                            fileBasedInitialSyncMaxCyclesWithoutProgress);
                }
            }

            // Make sure to kill the backupCursor on success.
            _killBackupCursor();
            return true;
        })
        .on(_syncingFilesState.executor, _syncingFilesState.token);
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
                    -> Future<bool> {
               stdx::lock_guard lock(_mutex);
               // Returns "true" only when all files are finished cloning.
               if (fileIndex == filesToClone->size())
                   return coerceToFuture(true);
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
               // progress indicator so that's OK.
               size_t fileSize = metadata["fileSize"].safeNumberLong();
               _syncingFilesState.currentBackupFileCloner =
                   std::make_unique<BackupFileCloner>(*_syncingFilesState.backupId,
                                                      fileName,
                                                      fileSize,
                                                      relativePath,
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
                   return status;
               }
               _syncingFilesState.executor->signalEvent(startCloner);
               return std::move(startClonerFuture).then([this, self = shared_from_this()] {
                   stdx::lock_guard lock(_mutex);
                   _syncingFilesState.backupFileClonerStats.emplace_back(
                       _syncingFilesState.currentBackupFileCloner->getStats());
                   _syncingFilesState.currentBackupFileCloner = nullptr;
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

ExecutorFuture<void> FileCopyBasedInitialSyncer::_startDeletingOldStorageFilesPhase() {
    return _getListOfOldFilesToBeDeleted()
        .then([this, self = shared_from_this()] {
            return _hangAsyncIfFailPointEnabled("fCBISHangBeforeDeletingOldStorageFiles",
                                                _syncingFilesState.executor,
                                                _syncingFilesState.token);
        })
        .then([this, self = shared_from_this()] {
            _syncingFilesState.currentFileMover =
                std::make_unique<InitialSyncFileMover>(storageGlobalParams.dbpath);
            _syncingFilesState.currentFileMover->deleteFiles(
                _syncingFilesState.oldStorageFilesToBeDeleted);
        });
}

ExecutorFuture<void> FileCopyBasedInitialSyncer::_getListOfOldFilesToBeDeleted() {
    // Files should be already clonned in '.initialSync' direcotry.
    // TODO (SERVER-57826): Get list of deleted files and create
    // INITIAL_SYNC_FILES_TO_DELETE.
    return ExecutorFuture<void>(_syncingFilesState.executor);
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

    if (_syncingFilesState.backupCursorKeepAliveFuture) {
        // Wait for the thread that keeps the backupCursor alive.
        _syncingFilesState.backupCursorKeepAliveFuture.get().wait();
    }

    // If the initial sync attempt has been started, wait for it to be canceled (through
    // _cancelRemainingWork()) and for the onCompletion lambda to run the cleanup work on
    // _exec.
    if (_startInitialSyncAttemptFuture.is_initialized()) {
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

void FileCopyBasedInitialSyncer::_cancelRemainingWork(WithLock) {
    // Cancel the cancellation source to stop the work being run on the executor.
    _source.cancel();

    _killBackupCursor();

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
        stdx::unique_lock<Latch> lk(_mutex);
        _stateCondition.wait(lk, [this, &lk]() { return !_isActive(lk); });
    }
    if (_startInitialSyncAttemptFuture) {
        _startInitialSyncAttemptFuture->wait();
    }
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

Status FileCopyBasedInitialSyncer::_connect(WithLock) {
    _client = _createClientFn();
    Status status = _client->connect(_syncSource, "FileCopyBasedInitialSyncer", boost::none);
    if (!status.isOK())
        return status;
    return replAuthenticate(_client.get())
        .withContext(str::stream() << "Failed to authenticate to " << _syncSource);
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

Status FileCopyBasedInitialSyncer::waitForStartInitialSyncAttemptFutureStatus_forTest() {
    if (_startInitialSyncAttemptFuture.is_initialized()) {
        return _startInitialSyncAttemptFuture.get().getNoThrow();
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
            },
            []() {
                InitialSyncFileMover mover(storageGlobalParams.dbpath);
                mover.recoverFileCopyBasedInitialSyncAtStartup();
            });
    });
}  // namespace repl
}  // namespace mongo
