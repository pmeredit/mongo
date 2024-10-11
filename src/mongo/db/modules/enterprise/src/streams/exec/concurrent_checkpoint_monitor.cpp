/**
 *    Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#include "streams/exec/concurrent_checkpoint_monitor.h"
#include "mongo/util/assert_util_core.h"

using namespace mongo;

namespace streams {

ConcurrentCheckpointController::ConcurrentCheckpointController(int32_t maxConcurrentCheckpoints)
    : _maxConcurrentCheckpoints(maxConcurrentCheckpoints) {
    invariant(_maxConcurrentCheckpoints > 0);
}

bool ConcurrentCheckpointController::startNewCheckpointIfRoom(bool force) {
    stdx::lock_guard<stdx::mutex> lk(_mutex);
    if (!force && _concurrentCheckpoints + 1 > _maxConcurrentCheckpoints) {
        return false;
    }
    _concurrentCheckpoints++;
    return true;
}

void ConcurrentCheckpointController::onCheckpointComplete() {
    stdx::lock_guard<stdx::mutex> lk(_mutex);
    _concurrentCheckpoints--;
    invariant(_concurrentCheckpoints >= 0);
}

void ConcurrentCheckpointController::setMaxInprogressCheckpoints(int32_t maxConcurrentCheckpoints) {
    stdx::lock_guard<stdx::mutex> lk(_mutex);
    invariant(_maxConcurrentCheckpoints > 0);
    _maxConcurrentCheckpoints = maxConcurrentCheckpoints;
}

};  // namespace streams
