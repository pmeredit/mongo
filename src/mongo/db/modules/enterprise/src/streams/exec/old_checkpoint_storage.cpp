/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */
#include "streams/exec/old_checkpoint_storage.h"
#include "mongo/logv2/log.h"
#include "streams/exec/checkpoint_data_gen.h"
#include "streams/exec/constants.h"
#include "streams/exec/context.h"
#include "streams/exec/executor.h"
#include "streams/exec/log_util.h"
#include "streams/exec/stats_utils.h"
#include "streams/exec/stream_stats.h"
#include "streams/util/metric_manager.h"

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kStreams

using namespace mongo;

namespace streams {

OldCheckpointStorage::OldCheckpointStorage(Context* context) : _context(context) {}

CheckpointId OldCheckpointStorage::createCheckpointId() {
    auto checkpointId = doCreateCheckpointId();
    while (_lastCreatedCheckpointId && *_lastCreatedCheckpointId == checkpointId) {
        // With MongoDBCheckpointStorage, checkpointId is chosen based on the current wallclock
        // milliseconds. Checkpoints are usually spaced out by a few minutes, so we should never
        // end up with the same wallclock millis on the same node. Some unit tests can cause this
        // to occur though, so we handle the situation and print a warning.
        LOGV2_WARNING(74810,
                      "Next checkpoint ID is the same as the last checkpoint ID, retrying",
                      "context"_attr = _context,
                      "checkpointId"_attr = checkpointId);
        checkpointId = doCreateCheckpointId();
    }
    _lastCreatedCheckpointId = checkpointId;
    _stats.emplace(checkpointId, std::map<OperatorId, OperatorStats>{});
    _numOngoingCheckpointsGauge->set(_numOngoingCheckpointsGauge->value() + 1);
    return checkpointId;
}

void OldCheckpointStorage::addState(CheckpointId checkpointId,
                                    OperatorId operatorId,
                                    mongo::BSONObj operatorState,
                                    int32_t chunkNumber) {
    invariant(chunkNumber >= 0);
    invariant(operatorId >= 0);
    invariant(checkpointId >= 0);
    doAddState(checkpointId, operatorId, std::move(operatorState), chunkNumber);
}

void OldCheckpointStorage::registerMetrics(MetricManager* metricManager) {
    invariant(metricManager);
    _numOngoingCheckpointsGauge = metricManager->registerGauge(
        "checkpoint_num_ongoing",
        "Number of ongoing checkpoints that have started but not yet committed.",
        getDefaultMetricLabels(_context),
        /* initialValue */ 0);

    _durationSinceLastCommitMsGauge = metricManager->registerCallbackGauge(
        "checkpoint_duration_since_last_committed_ms",
        "Duration since the timestamp of the last committed checkpoint.",
        getDefaultMetricLabels(_context),
        [this]() -> double {
            int64_t timestamp = _lastCommittedCheckpointId.load();
            if (timestamp == 0) {
                return 0;
            }
            return Date_t::now().toMillisSinceEpoch() - timestamp;
        });
}

void OldCheckpointStorage::commit(CheckpointId checkpointId) {
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
    _lastCommittedCheckpointId.store(checkpointId);
    _numOngoingCheckpointsGauge->set(_numOngoingCheckpointsGauge->value() - 1);
    if (_postCommitCallback) {
        _postCommitCallback.get()(checkpointId);
    }
}

boost::optional<CheckpointId> OldCheckpointStorage::readLatestCheckpointId() {
    auto checkpointId = doReadLatestCheckpointId();
    if (checkpointId) {
        invariant(*checkpointId >= _lastCommittedCheckpointId.load());
        _lastCommittedCheckpointId.store(*checkpointId);
    }
    return checkpointId;
}

boost::optional<mongo::BSONObj> OldCheckpointStorage::readState(CheckpointId checkpointId,
                                                                OperatorId operatorId,
                                                                int32_t chunkNumber) {
    invariant(chunkNumber >= 0);
    invariant(operatorId >= 0);
    invariant(checkpointId >= 0);
    return doReadState(checkpointId, operatorId, chunkNumber);
}

boost::optional<mongo::CheckpointInfo> OldCheckpointStorage::readCheckpointInfo(
    CheckpointId checkpointId) {
    return doReadCheckpointInfo(checkpointId);
}

void OldCheckpointStorage::addStats(CheckpointId checkpointId,
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
