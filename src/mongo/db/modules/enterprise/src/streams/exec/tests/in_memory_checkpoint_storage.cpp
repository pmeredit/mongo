/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */
#include "streams/exec/tests/in_memory_checkpoint_storage.h"
#include "streams/exec/checkpoint_data_gen.h"
#include "streams/exec/stats_utils.h"

namespace streams {

using namespace mongo;

CheckpointId InMemoryCheckpointStorage::doStartCheckpoint() {
    CheckpointId id{_nextCheckpointId++};
    invariant(!_checkpoints.contains(id));
    _checkpoints.emplace(id, Checkpoint{});
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
    _checkpoints[writer->getCheckpointId()].operatorState[writer->getOperatorId()].push_back(
        record.getOwned());
    _currentMemoryBytes += record.getCurrentApproximateSize();
    _maxMemoryUsageBytes->set(std::max(_maxMemoryUsageBytes->value(), (double)_currentMemoryBytes));
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

std::vector<mongo::CheckpointOperatorInfo>
InMemoryCheckpointStorage::doGetRestoreCheckpointOperatorInfo() {
    std::vector<mongo::CheckpointOperatorInfo> results;
    for (auto& [operatorId, stats] : _checkpoints[*_restoreCheckpoint].operatorStats) {
        results.push_back(CheckpointOperatorInfo{operatorId, toOperatorStatsDoc(stats)});
    }
    return results;
}

boost::optional<CheckpointId> InMemoryCheckpointStorage::doGetRestoreCheckpointId() {
    return _mostRecentCommitted;
}

}  // namespace streams
