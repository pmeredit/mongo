/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/exec/checkpoint_coordinator.h"

#include "mongo/db/service_context.h"
#include "mongo/logv2/log.h"
#include "streams/exec/checkpoint_storage.h"
#include "streams/exec/executor.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

namespace streams {

CheckpointCoordinator::CheckpointCoordinator(Options options) : _options(std::move(options)) {
    invariant(_options.svcCtx->getPeriodicRunner());
}

CheckpointCoordinator::~CheckpointCoordinator() {
    invariant(!_backgroundjob);
}

void CheckpointCoordinator::start() {
    invariant(!_backgroundjob);
    _backgroundjob = _options.svcCtx->getPeriodicRunner()->makeJob(
        PeriodicRunner::PeriodicJob{fmt::format("CheckpointCoordinator-{}", _options.processorId),
                                    [this](Client* client) { startCheckpoint(); },
                                    _options.interval,
                                    true /*isKillableByStepdown*/});

    _backgroundjob.start();
}

void CheckpointCoordinator::stop() {
    if (_backgroundjob) {
        LOGV2_INFO(75805, "Shutting down coordinator background job");
        _backgroundjob.stop();
        _backgroundjob.detach();
    }
}

void CheckpointCoordinator::startCheckpoint() {
    stdx::lock_guard<Latch> lock(_mutex);
    CheckpointId id = _options.storage->createCheckpointId();
    _options.executor->insertControlMsg(
        {.checkpointMsg = CheckpointControlMsg{.id = std::move(id)}});
}

}  // namespace streams
