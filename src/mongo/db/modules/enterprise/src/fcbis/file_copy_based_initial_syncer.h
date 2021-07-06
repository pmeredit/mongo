/**
 *    Copyright (C) 2021 MongoDB, Inc.  All Rights Reserved.
 */

#pragma once

#include "mongo/db/repl/data_replicator_external_state.h"
#include "mongo/db/repl/initial_syncer_interface.h"
#include "mongo/db/repl/replication_process.h"
#include "mongo/db/repl/storage_interface.h"
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
};

}  // namespace repl
}  // namespace mongo
