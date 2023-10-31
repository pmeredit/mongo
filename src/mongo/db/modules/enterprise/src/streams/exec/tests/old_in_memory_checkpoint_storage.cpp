/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */
#include "streams/exec/tests/old_in_memory_checkpoint_storage.h"

#include "mongo/bson/bsonelement.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/logv2/log.h"
#include "streams/exec/checkpoint_data_gen.h"

namespace streams {

using namespace mongo;

CheckpointId OldInMemoryCheckpointStorage::doCreateCheckpointId() {
    CheckpointId id{_nextCheckpointId++};
    _checkpoints.emplace(id, Checkpoint{});
    return id;
}

void OldInMemoryCheckpointStorage::doCommit(CheckpointId id, CheckpointInfo checkpointInfo) {
    invariant(id > _mostRecentCommitted);
    _checkpoints[id].committed = true;
    checkpointInfo.set_id(fmt::format("checkpoint/{}", id));
    _checkpoints[id].checkpointInfo = std::move(checkpointInfo);
    _mostRecentCommitted = id;
}

void OldInMemoryCheckpointStorage::doAddState(CheckpointId checkpointId,
                                              OperatorId operatorId,
                                              BSONObj operatorState,
                                              int32_t chunkNumber) {
    invariant(size_t(chunkNumber) == _checkpoints[checkpointId].operatorState[operatorId].size());
    _checkpoints[checkpointId].operatorState[operatorId].push_back(std::move(operatorState));
}

boost::optional<BSONObj> OldInMemoryCheckpointStorage::doReadState(CheckpointId checkpointId,
                                                                   OperatorId operatorId,
                                                                   int32_t chunkNumber) {
    auto checkpointIt = _checkpoints.find(checkpointId);
    if (checkpointIt == _checkpoints.end()) {
        return {};
    }

    auto& checkpoint = checkpointIt->second;
    if (!checkpoint.operatorState.contains(operatorId)) {
        return {};
    }

    auto& operatorState = checkpoint.operatorState[operatorId];
    if (size_t(chunkNumber) < operatorState.size()) {
        return checkpoint.operatorState[operatorId][chunkNumber];
    } else {
        return boost::none;
    }
}

boost::optional<CheckpointId> OldInMemoryCheckpointStorage::doReadLatestCheckpointId() {
    return _mostRecentCommitted;
}

boost::optional<mongo::CheckpointInfo> OldInMemoryCheckpointStorage::doReadCheckpointInfo(
    CheckpointId checkpointId) {
    if (!_checkpoints.contains(checkpointId) || !_checkpoints[checkpointId].committed) {
        return boost::none;
    }
    return _checkpoints[checkpointId].checkpointInfo;
}

}  // namespace streams
