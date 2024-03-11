/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/exec/checkpoint_coordinator.h"

#include <chrono>

#include "mongo/db/service_context.h"
#include "mongo/logv2/log.h"
#include "streams/exec/checkpoint_storage.h"
#include "streams/exec/old_checkpoint_storage.h"
#include "streams/exec/stats_utils.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

using namespace mongo;
using mongo::stdx::chrono::steady_clock;

namespace streams {

CheckpointCoordinator::CheckpointCoordinator(Options options) : _options(std::move(options)) {}

boost::optional<CheckpointControlMsg> CheckpointCoordinator::getCheckpointControlMsgIfReady(
    bool force) {
    if (force) {
        return createCheckpointControlMsg();
    }

    if (_options.writeFirstCheckpoint &&
        _lastCheckpointTimestamp.time_since_epoch() == steady_clock::duration::zero()) {
        return createCheckpointControlMsg();
    }

    auto now = steady_clock::now();
    dassert(_lastCheckpointTimestamp <= now);
    if (now - _lastCheckpointTimestamp <= _options.checkpointIntervalMs) {
        return boost::none;
    }
    return createCheckpointControlMsg();
}

CheckpointControlMsg CheckpointCoordinator::createCheckpointControlMsg() {
    _lastCheckpointTimestamp = steady_clock::now();
    CheckpointId id;
    if (_options.oldStorage) {
        id = _options.oldStorage->createCheckpointId();
    } else {
        invariant(_options.storage);
        id = _options.storage->startCheckpoint();
    }
    if (_options.restoreCheckpointOperatorInfo) {
        for (auto& opInfo : *_options.restoreCheckpointOperatorInfo) {
            auto checkpointStats = toOperatorStats(opInfo.getStats()).getAdditiveStats();
            if (_options.oldStorage) {
                _options.oldStorage->addStats(
                    id, opInfo.getOperatorId(), std::move(checkpointStats));
            } else {
                _options.storage->addStats(id, opInfo.getOperatorId(), std::move(checkpointStats));
            }
        }
    }
    return CheckpointControlMsg{.id = std::move(id)};
}

}  // namespace streams
