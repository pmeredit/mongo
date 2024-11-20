/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */


#include <boost/none.hpp>
#include <chrono>

#include "mongo/logv2/log.h"
#include "streams/exec/checkpoint_coordinator.h"
#include "streams/exec/checkpoint_storage.h"
#include "streams/exec/stats_utils.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

using namespace mongo;
using mongo::stdx::chrono::steady_clock;
using namespace std::chrono_literals;

namespace streams {

CheckpointCoordinator::CheckpointCoordinator(Options options)
    : _options(std::move(options)),
      _lastCheckpointTimestamp{steady_clock::now()},
      _interval{_options.minInterval} {
    if (_options.fixedInterval) {
        setCheckpointInterval(*_options.fixedInterval);
    }
}

boost::optional<CheckpointControlMsg> CheckpointCoordinator::getCheckpointControlMsgIfReady(
    const CheckpointRequest& req) {
    if (!_options.fixedInterval) {
        _interval = getDynamicInterval(req.lastCheckpointSizeBytes);
    }

    auto createCheckpoint = evaluateIfCheckpointShouldBeWritten(req);

    if (createCheckpoint == CreateCheckpoint::kNotNeeded) {
        return boost::none;
    }

    invariant(_options.checkpointController);

    bool hasRoom = _options.checkpointController->startNewCheckpointIfRoom(
        createCheckpoint == CreateCheckpoint::kForce);
    if (!hasRoom && createCheckpoint == CreateCheckpoint::kIfRoom) {
        auto minutesSinceLastCheckpoint = std::chrono::duration_cast<std::chrono::minutes>(
            steady_clock::now() - _lastCheckpointTimestamp);
        if (minutesSinceLastCheckpoint >= 120min) {
            LOGV2_WARNING(8368300,
                          "unable to take checkpoint due to max concurrent checkpoints reached",
                          "spid"_attr = _options.processorId,
                          "minutesSinceLastCheckpoint"_attr = minutesSinceLastCheckpoint.count());
        }
        return boost::none;
    }

    return createCheckpointControlMsg();
}

mongo::Milliseconds CheckpointCoordinator::getDynamicInterval(int64_t stateSize) {
    const int64_t range = _options.maxInterval.count() - _options.minInterval.count();
    const double multiplier =
        std::min(1.0, static_cast<double>(stateSize) / _options.stateSizeToUseMaxInterval);
    const double interval = _options.minInterval.count() + (multiplier * range);
    return Milliseconds(static_cast<int64_t>(interval));
}

CheckpointCoordinator::CreateCheckpoint CheckpointCoordinator::evaluateIfCheckpointShouldBeWritten(
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
        return CreateCheckpoint::kNotNeeded;
    }

    if (_options.writeFirstCheckpoint && !writtenFirstCheckpoint()) {
        return CreateCheckpoint::kForce;
    }

    // A high priority request bypasses all checks and forces a checkpoint.
    if (req.writeCheckpointCommand == WriteCheckpointCommand::kForce) {
        return CreateCheckpoint::kForce;
    }

    // Currently we always take a checkpoint at shutdown.
    if (req.shutdown) {
        return CreateCheckpoint::kForce;
    }

    // If nothing has changed, then skip taking a checkpoint.
    if (!(req.uncheckpointedState || req.changeStreamAdvanced)) {
        return CreateCheckpoint::kNotNeeded;
    }

    // Some state has changed.
    // If we have an externally requested checkpoint, then bypass the time based wait.
    if (req.writeCheckpointCommand == WriteCheckpointCommand::kNormal) {
        return CreateCheckpoint::kForce;
    }

    // Else, if sufficient time has elapsed, then take a checkpoint.
    auto now = steady_clock::now();
    dassert(_lastCheckpointTimestamp <= now);
    if (now - _lastCheckpointTimestamp <= _interval.toSystemDuration()) {
        return CreateCheckpoint::kNotNeeded;
    }
    return CreateCheckpoint::kIfRoom;
}


CheckpointControlMsg CheckpointCoordinator::createCheckpointControlMsg() {
    _writtenFirstCheckpoint = true;
    _lastCheckpointTimestamp = steady_clock::now();
    invariant(_options.storage);
    CheckpointId id = _options.storage->startCheckpoint();
    return CheckpointControlMsg{.id = std::move(id)};
}

}  // namespace streams
