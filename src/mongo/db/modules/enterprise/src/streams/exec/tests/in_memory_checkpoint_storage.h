#pragma once

#include "mongo/stdx/unordered_map.h"
#include "mongo/stdx/unordered_set.h"
#include "streams/exec/checkpoint_storage.h"

namespace streams {

// An in-memory implementation of CheckpointStorage, used in unit tests.
class InMemoryCheckpointStorage : public CheckpointStorage {
public:
    boost::optional<CheckpointId> getLatestCommittedCheckpointId() {
        return _mostRecentCommitted;
    }

private:
    struct Checkpoint {
        mongo::CheckpointInfo checkpointInfo;
        bool committed{false};
        mongo::stdx::unordered_map<OperatorId, std::vector<mongo::BSONObj>> operatorState;
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

    CheckpointId doStartCheckpoint() override;

    void doCommitCheckpoint(CheckpointId id) override;

    std::unique_ptr<WriterHandle> doCreateStateWriter(CheckpointId id, OperatorId opId) override;

    std::unique_ptr<ReaderHandle> doCreateStateReader(CheckpointId id, OperatorId opId) override;

    void doCloseStateReader(ReaderHandle* reader) override;

    void doCloseStateWriter(WriterHandle* writer) override;

    void doAppendRecord(WriterHandle* writer, mongo::BSONObj record) override;

    boost::optional<mongo::BSONObj> doGetNextRecord(ReaderHandle* reader) override;

    mongo::stdx::unordered_map<CheckpointId, Checkpoint> _checkpoints;

    boost::optional<WriterInfo> _writer;
    boost::optional<ReaderInfo> _reader;
    boost::optional<CheckpointId> _mostRecentCommitted;
    int _nextCheckpointId{1};
};

}  // namespace streams
