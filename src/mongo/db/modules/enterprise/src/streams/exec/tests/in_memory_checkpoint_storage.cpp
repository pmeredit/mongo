/**
 *    Copyright (C) 2023-present MongoDB, Inc.
 */
#include "streams/exec/tests/in_memory_checkpoint_storage.h"

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
    _checkpoints[id].committed = true;
    _mostRecentCommitted = id;
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

void InMemoryCheckpointStorage::doAppendRecord(WriterHandle* writer, mongo::BSONObj record) {
    invariant(_writer && writer->getCheckpointId() == _writer->checkpointId &&
              writer->getOperatorId() == _writer->operatorId);
    _checkpoints[writer->getCheckpointId()].operatorState[writer->getOperatorId()].push_back(
        std::move(record));
}

boost::optional<mongo::BSONObj> InMemoryCheckpointStorage::doGetNextRecord(ReaderHandle* reader) {
    invariant(_reader && reader->getCheckpointId() == _reader->checkpointId &&
              reader->getOperatorId() == _reader->operatorId);
    auto& operatorState =
        _checkpoints[reader->getCheckpointId()].operatorState[reader->getOperatorId()];
    if (size_t(_reader->position) < operatorState.size()) {
        return operatorState[_reader->position++];
    }
    return boost::none;
}

}  // namespace streams
