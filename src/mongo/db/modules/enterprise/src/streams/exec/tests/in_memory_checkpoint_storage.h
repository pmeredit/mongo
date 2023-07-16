#pragma once

#include "mongo/bson/bsonobj.h"
#include "streams/exec/checkpoint_storage.h"
#include "streams/exec/message.h"

namespace streams {

/**
 * A test only implementation of CheckpointStorage.
 */
class InMemoryCheckpointStorage : public CheckpointStorage {
protected:
    CheckpointId doCreateCheckpointId() override;
    void doCommit(CheckpointId id) override;
    void doAddState(CheckpointId checkpointId,
                    OperatorId operatorId,
                    mongo::BSONObj operatorState,
                    int32_t chunkNumber) override;
    boost::optional<mongo::BSONObj> doReadState(CheckpointId checkpointId,
                                                OperatorId operatorId,
                                                int32_t chunkNumber) override;
    boost::optional<CheckpointId> doReadLatestCheckpointId() override;

private:
    friend class CheckpointTest;

    struct Checkpoint {
        bool committed{false};
        mongo::stdx::unordered_map<OperatorId, std::vector<mongo::BSONObj>> operatorState;
    };

    mongo::stdx::unordered_map<CheckpointId, Checkpoint> _checkpoints;
    boost::optional<CheckpointId> _mostRecentCommitted;
    int _nextCheckpointId{0};
};

}  // namespace streams
