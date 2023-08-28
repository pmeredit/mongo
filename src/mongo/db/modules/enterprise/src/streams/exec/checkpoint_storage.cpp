/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */
#include "streams/exec/checkpoint_storage.h"
#include "streams/exec/checkpoint_data_gen.h"
#include "streams/exec/context.h"
#include "streams/exec/stats_utils.h"
#include "streams/exec/stream_stats.h"

using namespace mongo;

namespace streams {

CheckpointId CheckpointStorage::createCheckpointId() {
    auto checkpointId = doCreateCheckpointId();
    _stats.emplace(checkpointId, std::map<OperatorId, OperatorStats>{});
    return checkpointId;
}

void CheckpointStorage::addState(CheckpointId checkpointId,
                                 OperatorId operatorId,
                                 mongo::BSONObj operatorState,
                                 int32_t chunkNumber) {
    invariant(chunkNumber >= 0);
    invariant(operatorId >= 0);
    invariant(checkpointId >= 0);
    doAddState(checkpointId, operatorId, std::move(operatorState), chunkNumber);
}

void CheckpointStorage::commit(CheckpointId checkpointId) {
    invariant(checkpointId >= 0);
    auto it = _stats.find(checkpointId);
    invariant(it != _stats.end());
    std::map<OperatorId, OperatorStats> stats = std::move(it->second);
    _stats.erase(it);
    CheckpointInfo checkpointInfo;
    std::vector<CheckpointOperatorInfo> operatorInfo;
    operatorInfo.reserve(stats.size());
    for (auto& [operatorId, s] : stats) {
        operatorInfo.push_back(CheckpointOperatorInfo{operatorId, toOperatorStatsDoc(s)});
    }
    checkpointInfo.setOperatorInfo(std::move(operatorInfo));
    doCommit(checkpointId, std::move(checkpointInfo));
}

boost::optional<CheckpointId> CheckpointStorage::readLatestCheckpointId() {
    return doReadLatestCheckpointId();
}

boost::optional<mongo::BSONObj> CheckpointStorage::readState(CheckpointId checkpointId,
                                                             OperatorId operatorId,
                                                             int32_t chunkNumber) {
    invariant(chunkNumber >= 0);
    invariant(operatorId >= 0);
    invariant(checkpointId >= 0);
    return doReadState(checkpointId, operatorId, chunkNumber);
}

boost::optional<mongo::CheckpointInfo> CheckpointStorage::readCheckpointInfo(
    CheckpointId checkpointId) {
    return doReadCheckpointInfo(checkpointId);
}

void CheckpointStorage::addStats(CheckpointId checkpointId,
                                 OperatorId operatorId,
                                 const OperatorStats& stats) {
    invariant(_stats.contains(checkpointId));
    if (!_stats[checkpointId].contains(operatorId)) {
        _stats[checkpointId].insert(
            std::make_pair(operatorId, OperatorStats{.operatorName = stats.operatorName}));
    }
    _stats[checkpointId][operatorId] += stats;
}

}  // namespace streams
