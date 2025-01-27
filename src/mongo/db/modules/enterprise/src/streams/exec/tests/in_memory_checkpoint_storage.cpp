/**
 *    Copyright (C) 2023-present MongoDB, Inc. and subject to applicable commercial license.
 */
#include "streams/exec/tests/in_memory_checkpoint_storage.h"
#include "mongo/bson/simple_bsonobj_comparator.h"
#include "mongo/util/assert_util.h"
#include "streams/exec/checkpoint_data_gen.h"
#include "streams/exec/message.h"
#include "streams/exec/stats_utils.h"

namespace streams {

using namespace mongo;

CheckpointId InMemoryCheckpointStorage::doStartCheckpoint() {
    CheckpointId id{_nextCheckpointId++};
    invariant(!_checkpoints.contains(id));
    _checkpoints.emplace(id, Checkpoint{});
    _lastCreatedCheckpointId = id;
    return id;
}

void InMemoryCheckpointStorage::doCommitCheckpoint(CheckpointId id) {
    invariant(id > _mostRecentCommitted);
    invariant(!_writer);
    _mostRecentCommitted = id;
    _lastCheckpointSizeBytes = _currentMemoryBytes;
    _currentMemoryBytes = 0;

    addUnflushedCheckpoint(id,
                           {*_mostRecentCommitted,
                            "inmemory",
                            _lastCheckpointSizeBytes,
                            mongo::Date_t::now(),
                            Milliseconds{1} /* writeDurationMs */});
    _checkpoints[id].committed = true;

    // Compute the summary stats for this checkpoint.
    // This is the current $source and sink operator stats, plus the summary stats
    // in the restore checkpoint.
    auto& metadata = _checkpoints[id].checkpointInfo;
    std::vector<OperatorStats> operatorStats;
    for (const auto& [opId, stats] : _checkpoints[id].operatorStats) {
        operatorStats.push_back(stats);
    }
    auto summaryStats = computeStreamSummaryStats(operatorStats);
    if (_context->restoredCheckpointInfo) {
        summaryStats += toSummaryStats(*_context->restoredCheckpointInfo->summaryStats);
    }
    metadata.setSummaryStats(toSummaryStatsDoc(std::move(summaryStats)));

    if (_context->restoredCheckpointInfo && _context->restoredCheckpointInfo->operatorInfo) {
        // If there is a restore checkpoint, add its stats to the current operator stats.
        operatorStats = combineAdditiveStats(
            operatorStats, toOperatorStats(*_context->restoredCheckpointInfo->operatorInfo));
    }
    // Save the operator level stats in the checkpoint.
    metadata.setOperatorStats(toCheckpointOpInfo(operatorStats));

    metadata.setExecutionPlan(_context->executionPlan);
    metadata.setUserPipeline(std::vector<BSONObj>{});
    _checkpoints[id].checkpointInfo.setPipelineVersion(_context->pipelineVersion);
}

std::unique_ptr<CheckpointStorage::WriterHandle> InMemoryCheckpointStorage::doCreateStateWriter(
    CheckpointId id, OperatorId opId) {
    if (_writer) {
        invariant(false, "Only one writer at a time supported.");
    }
    WriterHandle::Options opts{this, id, opId};
    auto writer = std::unique_ptr<WriterHandle>(new WriterHandle(opts));
    _writer = WriterInfo{id, opId};
    return writer;
}

RestoredCheckpointInfo InMemoryCheckpointStorage::doStartCheckpointRestore(CheckpointId id) {
    _restoreCheckpoint = id;
    auto& manifest = _checkpoints[id].checkpointInfo;
    return RestoredCheckpointInfo{
        .description =
            mongo::CheckpointDescription{
                *_mostRecentCommitted,
                "inmemory",
                _lastCheckpointSizeBytes,
                mongo::Date_t::now(), /* we do not track the actual commit
                                         ts, so just return now() instead */
                mongo::Milliseconds{1} /* writeDurationMs */},
        .userPipeline = *manifest.getUserPipeline(),
        .pipelineVersion = *manifest.getPipelineVersion(),
        .summaryStats = manifest.getSummaryStats(),
        .operatorInfo = manifest.getOperatorStats(),
        .executionPlan = *manifest.getExecutionPlan()};
}

std::unique_ptr<CheckpointStorage::ReaderHandle> InMemoryCheckpointStorage::doCreateStateReader(
    CheckpointId id, OperatorId opId) {
    auto reader =
        std::unique_ptr<ReaderHandle>(new ReaderHandle(ReaderHandle::Options{this, id, opId}));
    if (_reader) {
        invariant(false, "Only one reader at a time supported.");
    }
    _reader = ReaderInfo{id, opId};
    return reader;
}

void InMemoryCheckpointStorage::doCloseStateReader(ReaderHandle* reader) {
    invariant(_reader && reader->getCheckpointId() == _reader->checkpointId &&
              reader->getOperatorId() == _reader->operatorId);
    _reader = boost::none;
}

void InMemoryCheckpointStorage::doCloseStateWriter(WriterHandle* writer) {
    invariant(_writer && writer->getCheckpointId() == _writer->checkpointId &&
              writer->getOperatorId() == _writer->operatorId);
    _writer = boost::none;
}

void InMemoryCheckpointStorage::doAppendRecord(WriterHandle* writer, mongo::Document record) {
    invariant(_writer && writer->getCheckpointId() == _writer->checkpointId &&
              writer->getOperatorId() == _writer->operatorId);

    if (writer->getOperatorId() == 0 /* $source */) {
        mongo::ReplaySourceState sourceState;
        sourceState.setCheckpointId(writer->getCheckpointId());
        sourceState.setSourceState(record.toBson());
        setLastCreatedCheckpointSourceState(sourceState);
    }

    _checkpoints[writer->getCheckpointId()].operatorState[writer->getOperatorId()].push_back(
        record.getOwned());
    _currentMemoryBytes += record.getCurrentApproximateSize();
    _maxMemoryUsageBytes->set(std::max(_maxMemoryUsageBytes->value(), (double)_currentMemoryBytes));
}

void InMemoryCheckpointStorage::setLastCreatedCheckpointSourceState(
    mongo::ReplaySourceState sourceState) {
    invariant(_lastCreatedCheckpointId);
    invariant(sourceState.getCheckpointId() == *_lastCreatedCheckpointId);
    _lastCheckpointSourceState = sourceState;
}

boost::optional<CheckpointId> InMemoryCheckpointStorage::doOnWindowOpen() {
    if (!_lastCreatedCheckpointId) {
        // Handle the case of in-memory source.
        invariant(_checkpoints.empty());
        return boost::none;
    }
    invariant(_lastCreatedCheckpointId);
    invariant(_lastCheckpointSourceState);

    auto [itr, res] = _replayCheckpointSourceStates.emplace(
        std::make_pair(*_lastCreatedCheckpointId, std::make_pair(*_lastCheckpointSourceState, 1)));

    if (!res) {
        // Source state entry for the checkpoint Id already exist, increment the counter.
        invariant(SimpleBSONObjComparator::kInstance.evaluate(
            itr->second.first.toBSON() == (*_lastCheckpointSourceState).toBSON()));
        itr->second.second++;
    }
    return _lastCreatedCheckpointId;
}

void InMemoryCheckpointStorage::doOnWindowRestore(CheckpointId checkpointId) {
    auto itr = _replayCheckpointSourceStates.find(checkpointId);
    invariant(itr != _replayCheckpointSourceStates.end());
    itr->second.second++;
}

void InMemoryCheckpointStorage::doOnWindowClose(CheckpointId checkpointId) {
    auto itr = _replayCheckpointSourceStates.find(checkpointId);
    invariant(itr != _replayCheckpointSourceStates.end() && itr->second.second > 0);
    if (--itr->second.second == 0) {
        _replayCheckpointSourceStates.erase(itr);
    }
}

void InMemoryCheckpointStorage::doAddMinWindowStartTime(int64_t minWindowStartTime) {
    _minWindowStartTime = minWindowStartTime;
}

boost::optional<mongo::Document> InMemoryCheckpointStorage::doGetNextRecord(ReaderHandle* reader) {
    invariant(_reader && reader->getCheckpointId() == _reader->checkpointId &&
              reader->getOperatorId() == _reader->operatorId);
    auto& operatorState =
        _checkpoints[reader->getCheckpointId()].operatorState[reader->getOperatorId()];
    if (size_t(_reader->position) < operatorState.size()) {
        return operatorState[_reader->position++];
    }
    return boost::none;
}

void InMemoryCheckpointStorage::doAddStats(CheckpointId checkpointId,
                                           OperatorId operatorId,
                                           const OperatorStats& stats) {
    auto& statsMap = _checkpoints[checkpointId].operatorStats;
    if (!statsMap.contains(operatorId)) {
        statsMap.insert(
            std::make_pair(operatorId, OperatorStats{.operatorName = stats.operatorName}));
    }
    statsMap[operatorId] += stats;
}

boost::optional<CheckpointId> InMemoryCheckpointStorage::doGetRestoreCheckpointId() {
    return _mostRecentCommitted;
}
}  // namespace streams
