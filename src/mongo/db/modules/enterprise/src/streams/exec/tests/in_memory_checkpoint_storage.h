/**
 *     Copyright (C) 2024-present MongoDB, Inc. and subject to applicable commercial license.
 */

#pragma once

#include <boost/optional/optional.hpp>

#include "mongo/bson/bsonobj.h"
#include "mongo/stdx/unordered_map.h"
#include "mongo/stdx/unordered_set.h"
#include "streams/exec/checkpoint_data_gen.h"
#include "streams/exec/checkpoint_storage.h"
#include "streams/exec/context.h"
#include "streams/exec/exec_internal_gen.h"
#include "streams/exec/message.h"

namespace streams {

// An in-memory implementation of CheckpointStorage, used in unit tests.
class InMemoryCheckpointStorage : public CheckpointStorage {
public:
    boost::optional<CheckpointId> getLatestCommittedCheckpointId() {
        return _mostRecentCommitted;
    }
    boost::optional<CheckpointId> getLastCreatedCheckpointId() {
        return _lastCreatedCheckpointId;
    }

    explicit InMemoryCheckpointStorage(Context* ctxt) : CheckpointStorage{ctxt} {}

private:
    struct Checkpoint {
        mongo::CheckpointMetadata checkpointInfo;
        bool committed{false};
        mongo::stdx::unordered_map<OperatorId, std::vector<mongo::Document>> operatorState;
        std::map<OperatorId, OperatorStats> operatorStats;
    };

    struct WriterInfo {
        CheckpointId checkpointId;
        OperatorId operatorId;
    };

    struct ReaderInfo {
        CheckpointId checkpointId;
        OperatorId operatorId;
        // Used to track the next index to read in the OperatorState vector.
        int32_t position{0};
    };


    friend class CheckpointTestWorkload;

    boost::optional<CheckpointId> doGetRestoreCheckpointId() override;

    CheckpointId doStartCheckpoint() override;

    void doCommitCheckpoint(CheckpointId id) override;

    RestoredCheckpointInfo doStartCheckpointRestore(CheckpointId id) override;
    void doCreateCheckpointRestorer(CheckpointId chkId, bool replayRestorer) override {}
    void doMarkCheckpointRestored(CheckpointId id) override {
        _restoreCheckpoint = boost::none;
    }

    std::unique_ptr<WriterHandle> doCreateStateWriter(CheckpointId id, OperatorId opId) override;

    std::unique_ptr<ReaderHandle> doCreateStateReader(CheckpointId id, OperatorId opId) override;

    void doCloseStateReader(ReaderHandle* reader) override;

    void doCloseStateWriter(WriterHandle* writer) override;

    void doAppendRecord(WriterHandle* writer, mongo::Document record) override;
    boost::optional<CheckpointId> doOnWindowOpen() override;
    void doOnWindowRestore(CheckpointId checkpointId) override;
    void doOnWindowClose(CheckpointId checkpointId) override;
    void doAddMinWindowStartTime(int64_t minWindowStartTime) override;

    boost::optional<mongo::Document> doGetNextRecord(ReaderHandle* reader) override;

    void doAddStats(CheckpointId checkpointId,
                    OperatorId operatorId,
                    const OperatorStats& stats) override;

    void setLastCreatedCheckpointSourceState(mongo::ReplaySourceState sourceState);

    mongo::stdx::unordered_map<CheckpointId, Checkpoint> _checkpoints;

    boost::optional<WriterInfo> _writer;
    boost::optional<ReaderInfo> _reader;
    boost::optional<CheckpointId> _mostRecentCommitted;
    boost::optional<CheckpointId> _restoreCheckpoint;
    int _nextCheckpointId{1};
    int64_t _currentMemoryBytes = 0;
    std::map<CheckpointId, std::pair<mongo::ReplaySourceState, size_t>>
        _replayCheckpointSourceStates;
    boost::optional<CheckpointId> _lastCreatedCheckpointId;
    boost::optional<mongo::ReplaySourceState> _lastCheckpointSourceState;
    boost::optional<int64_t> _minWindowStartTime;
};

}  // namespace streams
