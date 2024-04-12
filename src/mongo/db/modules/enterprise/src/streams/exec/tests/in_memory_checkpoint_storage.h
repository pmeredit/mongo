#pragma once

#include "mongo/stdx/unordered_map.h"
#include "mongo/stdx/unordered_set.h"
#include "streams/exec/checkpoint_data_gen.h"
#include "streams/exec/checkpoint_storage.h"
#include "streams/exec/exec_internal_gen.h"

namespace streams {

// An in-memory implementation of CheckpointStorage, used in unit tests.
class InMemoryCheckpointStorage : public CheckpointStorage {
public:
    boost::optional<CheckpointId> getLatestCommittedCheckpointId() {
        return _mostRecentCommitted;
    }

    explicit InMemoryCheckpointStorage(Context* ctxt) : CheckpointStorage{ctxt} {}

private:
    struct Checkpoint {
        mongo::CheckpointInfo checkpointInfo;
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

    mongo::CheckpointDescription doStartCheckpointRestore(CheckpointId id) override {
        _restoreCheckpoint = id;
        return mongo::CheckpointDescription{
            *_mostRecentCommitted,
            "inmemory",
            _lastCheckpointSizeBytes,
            mongo::Date_t::now(), /* we do not track the actual commit ts, so
                                                            just return now() instead */
            mongo::Milliseconds{1} /* writeDurationMs */};
    }

    void doMarkCheckpointRestored(CheckpointId id) override {
        _restoreCheckpoint = boost::none;
    }

    std::unique_ptr<WriterHandle> doCreateStateWriter(CheckpointId id, OperatorId opId) override;

    std::unique_ptr<ReaderHandle> doCreateStateReader(CheckpointId id, OperatorId opId) override;

    void doCloseStateReader(ReaderHandle* reader) override;

    void doCloseStateWriter(WriterHandle* writer) override;

    void doAppendRecord(WriterHandle* writer, mongo::Document record) override;

    boost::optional<mongo::Document> doGetNextRecord(ReaderHandle* reader) override;

    void doAddStats(CheckpointId checkpointId,
                    OperatorId operatorId,
                    const OperatorStats& stats) override;

    std::vector<mongo::CheckpointOperatorInfo> doGetRestoreCheckpointOperatorInfo() override;

    mongo::stdx::unordered_map<CheckpointId, Checkpoint> _checkpoints;

    boost::optional<WriterInfo> _writer;
    boost::optional<ReaderInfo> _reader;
    boost::optional<CheckpointId> _mostRecentCommitted;
    boost::optional<CheckpointId> _restoreCheckpoint;
    int _nextCheckpointId{1};
    int64_t _currentMemoryBytes = 0;
};

}  // namespace streams
