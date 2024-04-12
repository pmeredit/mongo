/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */
#include "streams/exec/unflushed_state_container.h"
#include "mongo/base/error_codes.h"

namespace streams {

void UnflushedStateContainer::add(CheckpointId id, mongo::BSONObj state) {
    _unflushedCheckpoints.push_back(std::make_pair(id, std::move(state)));
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
