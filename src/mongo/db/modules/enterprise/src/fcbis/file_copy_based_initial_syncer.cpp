/**
 * Copyright (C) 2021 MongoDB, Inc.  All Rights Reserved.
 */
#include "file_copy_based_initial_syncer.h"
#include "mongo/db/repl/initial_syncer_factory.h"

namespace mongo {
namespace repl {

FileCopyBasedInitialSyncer::FileCopyBasedInitialSyncer(
    InitialSyncerInterface::Options opts,
    std::unique_ptr<DataReplicatorExternalState> dataReplicatorExternalState,
    ThreadPool* writerPool,
    StorageInterface* storage,
    ReplicationProcess* replicationProcess,
    const OnCompletionFn& onCompletion) {}

FileCopyBasedInitialSyncer::~FileCopyBasedInitialSyncer() {}

Status FileCopyBasedInitialSyncer::startup(OperationContext* opCtx,
                                           std::uint32_t initialSyncMaxAttempts) noexcept {
    return Status::OK();
}

Status FileCopyBasedInitialSyncer::shutdown() {
    return Status::OK();
}

void FileCopyBasedInitialSyncer::join() {}

BSONObj FileCopyBasedInitialSyncer::getInitialSyncProgress() const {
    return BSONObj();
}

void FileCopyBasedInitialSyncer::cancelCurrentAttempt() {}

Status FileCopyBasedInitialSyncer::_connect() {
    _client = std::make_unique<DBClientConnection>(true /* autoReconnect */);
    return _client->connect(_syncSource, "FileCopyBasedInitialSyncer", boost::none);
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
