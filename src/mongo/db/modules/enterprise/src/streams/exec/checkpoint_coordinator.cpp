/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */

#include "streams/exec/checkpoint_coordinator.h"

#include <chrono>

#include "mongo/db/service_context.h"
#include "mongo/logv2/log.h"
#include "streams/exec/change_stream_source_operator.h"
#include "streams/exec/checkpoint_storage.h"
#include "streams/exec/old_checkpoint_storage.h"
#include "streams/exec/stats_utils.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

using namespace mongo;
using mongo::stdx::chrono::steady_clock;

namespace streams {

CheckpointCoordinator::CheckpointCoordinator(Options options)
    : _options(std::move(options)), _lastCheckpointTimestamp{steady_clock::now()} {}

boost::optional<CheckpointControlMsg> CheckpointCoordinator::getCheckpointControlMsgIfReady(
    const CheckpointRequest& req) {
    // The current logic is:
    // 1) When SP is started for the first time ever, we take a checkpoint.
    // 2) Each time a SP is stopped, we take a checkpoint.
    // 3) When the SP is running, we take a checkpoint based on the pipeline's inter-checkpoint
    //    interval. This defaults to 1 hour for pipelines with a window and to 5 mins for other
    //    pipelines. (tests can set this interval to other values)
    // 4) A checkpoint request can be made via an "internal" RPC call (not exposed via Agent). Such
    //    a request can be normal or have a "force" priority. A normal request follows the same
    //    logic as above but additionally bypasses the time based wait. i.e. if nothing has changed
    //    then it will still skip taking a checkpoint. A "force" request will cause a checkpoint to
    //    be taken even if nothing has changed.

    if (!_options.enableDataFlow) {
        return boost::none;
    }

    if (_options.writeFirstCheckpoint && !writtenFirstCheckpoint()) {
        return createCheckpointControlMsg();
    }

    // A high priority request bypasses all checks and forces a checkpoint.
    if (req.writeCheckpointCommand == WriteCheckpointCommand::kForce) {
        return createCheckpointControlMsg();
    }

    // Currently we always take a checkpoint at shutdown.
    if (req.shutdown) {
        return createCheckpointControlMsg();
    }

    // If nothing has changed, then skip taking a checkpoint.
    if (!(req.uncheckpointedState || req.changeStreamAdvanced)) {
        return boost::none;
    }

    // Some state has changed.
    // If we have an externally requested checkpoint, then bypass the time based wait.
    if (req.writeCheckpointCommand == WriteCheckpointCommand::kNormal) {
        return createCheckpointControlMsg();
    }

    // Else, if sufficient time has elapsed, then take a checkpoint.
    auto now = steady_clock::now();
    dassert(_lastCheckpointTimestamp <= now);
    if (now - _lastCheckpointTimestamp <= _options.checkpointIntervalMs) {
        return boost::none;
    }
    return createCheckpointControlMsg();
}

CheckpointControlMsg CheckpointCoordinator::createCheckpointControlMsg() {
    _writtenFirstCheckpoint = true;
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
