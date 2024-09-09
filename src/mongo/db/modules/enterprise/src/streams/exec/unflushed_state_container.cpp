/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */
#include "streams/exec/unflushed_state_container.h"
#include "mongo/base/error_codes.h"

namespace streams {

void UnflushedStateContainer::add(CheckpointId id, mongo::BSONObj state) {
    _unflushedCheckpoints.push_back(std::make_pair(id, std::move(state)));
}

bool UnflushedStateContainer::contains(CheckpointId id) {
    auto it = std::find_if(_unflushedCheckpoints.begin(),
                           _unflushedCheckpoints.end(),
                           [id](auto kv) { return kv.first == id; });
    return it != _unflushedCheckpoints.end();
}

mongo::BSONObj UnflushedStateContainer::pop(CheckpointId checkpointId) {
    uassert(mongo::ErrorCodes::InternalError,
            fmt::format("Target checkpointID {} is not in the UnflushedStateContainer, it's empty.",
                        checkpointId),
            !_unflushedCheckpoints.empty());
    uassert(mongo::ErrorCodes::InternalError,
            fmt::format("Target checkpointID {} is not the oldest in the UnflushedStateContainer, "
                        "the oldest is {}",
                        checkpointId,
                        _unflushedCheckpoints.front().first),
            _unflushedCheckpoints.front().first == checkpointId);
    auto unflushedCheckpoint = std::move(_unflushedCheckpoints.front());
    _unflushedCheckpoints.pop_front();
    return std::move(unflushedCheckpoint.second);
}

}  // namespace streams
