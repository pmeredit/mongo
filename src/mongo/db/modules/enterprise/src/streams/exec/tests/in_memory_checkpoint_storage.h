#pragma once

#include "mongo/bson/bsonobj.h"
#include "streams/exec/checkpoint_data_gen.h"
#include "streams/exec/checkpoint_storage.h"
#include "streams/exec/context.h"
#include "streams/exec/log_util.h"
#include "streams/exec/message.h"

namespace streams {

/**
 * A test only implementation of CheckpointStorage.
 */
class InMemoryCheckpointStorage : public CheckpointStorage {
public:
    InMemoryCheckpointStorage(Context* context) : CheckpointStorage(context) {}

protected:
    CheckpointId doCreateCheckpointId() override;
    void doCommit(CheckpointId checkpointId, mongo::CheckpointInfo checkpointInfo) override;
    void doAddState(CheckpointId checkpointId,
                    OperatorId operatorId,
                    mongo::BSONObj operatorState,
                    int32_t chunkNumber) override;
    boost::optional<mongo::BSONObj> doReadState(CheckpointId checkpointId,
                                                OperatorId operatorId,
                                                int32_t chunkNumber) override;
    boost::optional<CheckpointId> doReadLatestCheckpointId() override;
    boost::optional<mongo::CheckpointInfo> doReadCheckpointInfo(CheckpointId checkpointId) override;

private:
    friend class CheckpointTest;
    friend class WindowOperatorTest;

    struct Checkpoint {
        mongo::CheckpointInfo checkpointInfo;
        bool committed{false};
        mongo::stdx::unordered_map<OperatorId, std::vector<mongo::BSONObj>> operatorState;
    };

    mongo::stdx::unordered_map<CheckpointId, Checkpoint> _checkpoints;
    boost::optional<CheckpointId> _mostRecentCommitted;
    int _nextCheckpointId{1};
};

}  // namespace streams
